import os
from flask import Flask, render_template, request, send_file, jsonify
from markupsafe import Markup
import requests
from openpyxl import Workbook
from openpyxl.worksheet.table import Table, TableStyleInfo
from openpyxl.styles import Font, PatternFill
from datetime import datetime, timedelta
import json

app = Flask(__name__)

print(os.getcwd())

# Загрузка токена из файла конфигурации
with open('config.py', 'r') as config_file:
    exec(config_file.read())

BASE_URL = 'https://api.moysklad.ru/api/remap/1.2'

def render_group_options(groups, level=0):
    result = []
    for group in groups:
        indent = '—' * level
        result.append(f'<option value="{group["id"]}" {"class=\"nested-select\"" if level > 0 else ""} style="margin-left: {level * 20}px;">{indent} {group["name"]}</option>')
        if group.get('children'):
            result.extend(render_group_options(group['children'], level + 1))
    return '\n'.join(result)

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        start_date = request.form['start_date']
        end_date = request.form['end_date']
        store_id = request.form['store_id']
        product_group = request.form.get('product_group', '')
        
        print(f"Received form data: start_date={start_date}, end_date={end_date}, store_id={store_id}, product_group={product_group}")
        
        try:
            report_data = get_report_data(start_date, end_date, store_id, product_group)
            
            # print(f"Полученные данные: {json.dumps(report_data, indent=2, ensure_ascii=False)}")
            
            if not report_data or 'rows' not in report_data or not report_data['rows']:
                return "Нет данных для формирования отчета для выбранных параметров", 404
            
            excel_file = create_excel_report(report_data, store_id, end_date)
            
            return send_file(excel_file, as_attachment=True, download_name='profitability_report.xlsx')
        except Exception as e:
            print(f"Error in index(): {str(e)}")
            return f"Произошла ошибка при формировании отчета: {str(e)}", 500
    
    stores = get_stores()
    product_groups = get_product_groups()
    return render_template('index.html', stores=stores, product_groups=product_groups, render_group_options=render_group_options)

@app.route('/get_subgroups/<group_id>')
def get_subgroups(group_id):
    subgroups = get_subgroups_for_group(group_id)
    return jsonify(subgroups)

def get_report_data(start_date, end_date, store_id, product_group):
    try:
        url = f"{BASE_URL}/report/profit/byvariant"
        headers = {
            'Authorization': f'Bearer {MOYSKLAD_TOKEN}',
            'Accept': 'application/json;charset=utf-8',
            'Content-Type': 'application/json'
        }
        
        start_datetime = datetime.strptime(start_date, '%Y-%m-%d')
        end_datetime = datetime.strptime(end_date, '%Y-%m-%d')
        
        formatted_start = start_datetime.strftime('%Y-%m-%d %H:%M:%S')
        formatted_end = end_datetime.replace(hour=23, minute=59, second=59).strftime('%Y-%m-%d %H:%M:%S')
        
        params = {
            'momentFrom': formatted_start,
            'momentTo': formatted_end,
            'limit': 1000,
            'offset': 0
        }
        
        filter_parts = []
        
        if store_id:
            store_url = f"{BASE_URL}/entity/store/{store_id}"
            filter_parts.append(f'store={store_url}')
        
        if product_group:
            product_folder_url = f"{BASE_URL}/entity/productfolder/{product_group}"
            filter_parts.append(f'productFolder={product_folder_url}')
        
        if filter_parts:
            params['filter'] = ';'.join(filter_parts)
        
        all_rows = []
        total_count = None
        
        while True:
            # Формируем строку запроса
            query_params = [f"{k}={v}" for k, v in params.items() if k != 'filter']
            if 'filter' in params:
                query_params.append(f"filter={params['filter']}")
            query_string = '&'.join(query_params)
            
            full_url = f"{url}?{query_string}"
            print(f"Отправляем запрос: URL={full_url}, Headers={headers}")
            
            response = requests.get(full_url, headers=headers)
            
            if response.status_code != 200:
                error_message = f"Ошибка при получении данных: {response.status_code}. Ответ сервера: {response.text}"
                print(error_message)
                raise Exception(error_message)
            
            data = response.json()
            
            if total_count is None:
                total_count = data.get('meta', {}).get('size', 0)
                print(f"Всего записей: {total_count}")
            
            all_rows.extend(data.get('rows', []))
            
            if len(all_rows) >= total_count:
                break
            
            params['offset'] += params['limit']
        
        result = {
            'meta': data.get('meta', {}),
            'rows': all_rows
        }
        
        print(f"Полчено записей: {len(all_rows)}")
        return result
    except requests.exceptions.RequestException as e:
        error_message = f"Ошибка при отправке запроса: {str(e)}"
        print(error_message)
        raise Exception(error_message)
    except json.JSONDecodeError as e:
        error_message = f"Ошибка при разборе JSON ответа: {str(e)}"
        print(error_message)
        raise Exception(error_message)
    except Exception as e:
        error_message = f"Неожиданная ошибка: {str(e)}"
        print(error_message)
        raise Exception(error_message)

def get_stores():
    url = f"{BASE_URL}/entity/store"
    headers = {
        'Authorization': f'Bearer {MOYSKLAD_TOKEN}',
        'Accept': 'application/json;charset=utf-8'
    }
    
    print(f"Отправляем запрос для получения списка складов: URL={url}, Headers={headers}")  # Для отладки
    
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        stores = response.json()['rows']
        return [{'id': store['id'], 'name': store['name']} for store in stores]
    else:
        error_message = f"Ошибка при получении списка складов: {response.status_code}. Ответ сервера: {response.text}"
        print(error_message)  # Выводим ошибку в консоль для отладки
        raise Exception(error_message)
    
def get_subgroups_for_group(group_id):
    url = f"{BASE_URL}/entity/productfolder"
    headers = {
        'Authorization': f'Bearer {MOYSKLAD_TOKEN}',
        'Accept': 'application/json;charset=utf-8'
    }
    params = {
        'filter': f'productFolder={group_id}'
    }
    
    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        subgroups = response.json()['rows']
        return [{'id': group['id'], 'name': group['name'], 'children': []} for group in subgroups]
    else:
        error_message = f"Ошибка при получении подгрупп: {response.status_code}. Ответ сервера: {response.text}"
        print(error_message)
        return []

def get_product_groups():
    url = f"{BASE_URL}/entity/productfolder"
    headers = {
        'Authorization': f'Bearer {MOYSKLAD_TOKEN}',
        'Accept': 'application/json;charset=utf-8'
    }
    
    all_groups = []
    offset = 0
    limit = 1000

    while True:
        params = {
            'offset': offset,
            'limit': limit
        }
        
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            data = response.json()
            all_groups.extend(data['rows'])
            if len(data['rows']) < limit:
                break
            offset += limit
        else:
            error_message = f"Ошибка при получении писка групп товаров: {response.status_code}. Ответ сервера: {response.text}"
            print(error_message)
            raise Exception(error_message)

    return build_group_hierarchy(all_groups)

def build_group_hierarchy(groups):
    group_dict = {}
    root_groups = []

    # Первый проход: создаем словарь всех групп
    for group in groups:
        group_id = group['id']
        group_dict[group_id] = {
            'id': group_id,
            'name': group['name'],
            'children': [],
            'parent': None
        }

    # Второй проход: устанавливаем связи родитель-потомок
    for group in groups:
        group_id = group['id']
        parent_href = group.get('productFolder', {}).get('meta', {}).get('href')
        if parent_href:
            parent_id = parent_href.split('/')[-1]
            if parent_id in group_dict:
                group_dict[group_id]['parent'] = parent_id
                group_dict[parent_id]['children'].append(group_dict[group_id])
        else:
            root_groups.append(group_dict[group_id])

    # Сртируем группы по имени
    root_groups.sort(key=lambda x: x['name'])
    for group in group_dict.values():
        group['children'].sort(key=lambda x: x['name'])

    return root_groups

# Добавьте эту функцию для отладки
def print_group_hierarchy(groups, level=0):
    for group in groups:
        print("  " * level + f"{group['name']} (ID: {group['id']})")
        print_group_hierarchy(group['children'], level + 1)

def get_sales_speed(variant_id, store_id, end_date, is_variant):
    url = f"{BASE_URL}/report/turnover/byoperations"
    headers = {
        'Authorization': f'Bearer {MOYSKLAD_TOKEN}',
        'Accept': 'application/json;charset=utf-8'
    }
    
    start_date = "2024-01-01 00:00:00"
    end_date_formatted = datetime.strptime(end_date, '%Y-%m-%d').strftime('%Y-%m-%d 23:59:59')
    
    assortment_type = 'variant' if is_variant else 'product'
    
    filter_params = [
        f"filter=store={BASE_URL}/entity/store/{store_id}",
        f"filter={assortment_type}={BASE_URL}/entity/{assortment_type}/{variant_id}"
    ]
    
    params = {
        'momentFrom': start_date,
        'momentTo': end_date_formatted,
    }
    
    query_string = '&'.join([f"{k}={v}" for k, v in params.items()] + filter_params)
    full_url = f"{url}?{query_string}"
    
    print(f"Запрос для получения данных о продажах: URL={full_url}")
    
    response = requests.get(full_url, headers=headers)
    if response.status_code != 200:
        print(f"Ошибка при получении данных о продажах: {response.status_code}. Ответ сервера: {response.text}")
        return 0, 0, 0

    data = response.json()
    rows = data.get('rows', [])

    # Фильтрация по UUID модификации
    filtered_rows = [
        row for row in rows
        if row.get('assortment', {}).get('meta', {}).get('href', '').split('/')[-1] == variant_id
    ]

    # Сортировка операций по дате
    filtered_rows.sort(key=lambda x: datetime.fromisoformat(x['operation']['moment'].replace('Z', '+00:00')))

    retail_demand_counter = 0
    current_stock = 0
    last_operation_time = None
    on_stock_time = timedelta()

    end_datetime = datetime.strptime(end_date_formatted, '%Y-%m-%d %H:%M:%S')

    for row in filtered_rows:
        quantity = row['quantity']
        operation_time = datetime.fromisoformat(row['operation']['moment'].replace('Z', '+00:00'))
        operation_type = row['operation']['meta']['type']

        if last_operation_time and current_stock > 0:
            on_stock_time += operation_time - last_operation_time

        if quantity > 0:  # Приход товара
            current_stock += quantity
        else:  # Уход товара
            quantity = abs(quantity)
            current_stock = max(0, current_stock - quantity)
            if operation_type == 'retaildemand':
                retail_demand_counter += quantity

        last_operation_time = operation_time

    # Учитываем время от последней операции до конца периода
    if last_operation_time and current_stock > 0:
        on_stock_time += end_datetime - last_operation_time

    days_on_stock = on_stock_time.total_seconds() / (24 * 60 * 60)

    if days_on_stock > 0:
        sales_speed = round(retail_demand_counter / days_on_stock, 2)
    else:
        sales_speed = 0

    print(f"sales_speed: {sales_speed}")

    return sales_speed

def create_excel_report(data, store_id, end_date):
    print("Начало создания Excel отчета")

    wb = Workbook()
    ws = wb.active
    ws.title = "Отчет прибыльности"

    if not data or 'rows' not in data or not data['rows']:
        print("Ошибка: Нет данных для создания отчета")
        ws.cell(row=1, column=1, value="Нет данных для отчета")
        filename = f"empty_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        wb.save(filename)
        return filename

    headers = ['Наименование', 'Количество', 'Прибыльность', 'Скорость продаж']
    for col, header in enumerate(headers, start=1):
        cell = ws.cell(row=1, column=col, value=header)
        cell.font = Font(bold=True, color="FFFFFF")  # Белый цвет текста
        cell.fill = PatternFill(start_color="000000", end_color="000000", fill_type="solid")  # Черный фон

    for row, item in enumerate(data['rows'], start=2):
        assortment = item.get('assortment', {})
        assortment_meta = assortment.get('meta', {})
        assortment_href = assortment_meta.get('href', '')
        
        is_variant = '/variant/' in assortment_href
        
        if is_variant:
            variant_id = assortment_href.split('/variant/')[-1]
        else:
            variant_id = assortment_href.split('/product/')[-1]
        
        print(f"\nОбработка товара: {assortment.get('name', '')}")
        print(f"ID товара/модификации: {variant_id}")
        print(f"Тип: {'Модификация' if is_variant else 'Товар'}")
        print(f"Количество продаж: {item.get('sellQuantity', 0)}")
        print(f"Прибыль: {round(item.get('profit', 0) / 100, 2)}")
        
        ws.cell(row=row, column=1, value=assortment.get('name', ''))
        ws.cell(row=row, column=2, value=item.get('sellQuantity', 0))
        ws.cell(row=row, column=3, value=round(item.get('profit', 0) / 100, 2))
        
        if variant_id:
            print(f"Получение данных о продажах для товара: {assortment.get('name', '')}, ID: {variant_id}")
            sales_speed = get_sales_speed(variant_id, store_id, end_date, is_variant)
            ws.cell(row=row, column=4, value=sales_speed if sales_speed != 0 else "Нет данных")
        else:
            print(f"Не удалось получить ID для товара: {assortment.get('name', '')}")
            ws.cell(row=row, column=4, value="Ошибка")

    tab = Table(displayName="Table1", ref=f"A1:D{len(data['rows']) + 1}")
    style = TableStyleInfo(name="TableStyleMedium9", showFirstColumn=False,
                           showLastColumn=False, showRowStripes=True, showColumnStripes=False)
    tab.tableStyleInfo = style
    ws.add_table(tab)

    ws.freeze_panes = 'A2'

    for column in ws.columns:
        max_length = 0
        column_letter = column[0].column_letter
        for cell in column:
            try:
                if len(str(cell.value)) > max_length:
                    max_length = len(str(cell.value))
            except:
                pass
        adjusted_width = (max_length + 2)
        ws.column_dimensions[column_letter].width = adjusted_width

    filename = f"report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    wb.save(filename)
    print(f"Excel отчет создан: {filename}")

    return filename


if __name__ == '__main__':
    app.run(debug=True)
