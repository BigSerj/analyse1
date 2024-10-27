import os
from flask import Flask, render_template, request, send_file, jsonify, abort
from markupsafe import Markup
import requests
from openpyxl import Workbook
from openpyxl.worksheet.table import Table, TableStyleInfo
from openpyxl.styles import Font, PatternFill
from datetime import datetime, timedelta
import json
import threading

app = Flask(__name__)

print(os.getcwd())

# Загрузка токена из файла конфигурации
with open('config.py', 'r') as config_file:
    exec(config_file.read())

BASE_URL = 'https://api.moysklad.ru/api/remap/1.2'

# Добавим глобальную переменную для отслеживания состояния
processing_cancelled = False
processing_lock = threading.Lock()

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
        planning_days = int(request.form['planning_days'])  # Получаем количество дней
        product_groups = request.form['product_group'].split(',') if request.form['product_group'] else []
        
        print(f"Received form data: start_date={start_date}, end_date={end_date}, store_id={store_id}, product_groups={product_groups}, planning_days={planning_days}")
        
        try:
            report_data = get_report_data(start_date, end_date, store_id, product_groups)
            
            if not report_data or 'rows' not in report_data or not report_data['rows']:
                return "Нет данных для формирования отчета для выбранных параметров", 404
            
            excel_file = create_excel_report(report_data, store_id, end_date, planning_days)  # Передаем planning_days
            
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

@app.route('/stop_processing', methods=['POST'])
def stop_processing():
    global processing_cancelled
    with processing_lock:
        processing_cancelled = True
    return '', 204

def check_if_cancelled():
    global processing_cancelled
    with processing_lock:
        if processing_cancelled:
            raise Exception("Processing cancelled by user")

def get_report_data(start_date, end_date, store_id, product_groups):
    global processing_cancelled
    with processing_lock:
        processing_cancelled = False
    
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
        
        # Modified to handle multiple product groups
        if product_groups:
            for group_id in product_groups:
                if group_id:  # Check if group_id is not empty
                    product_folder_url = f"{BASE_URL}/entity/productfolder/{group_id}"
                    filter_parts.append(f'productFolder={product_folder_url}')
        
        if filter_parts:
            # Join all filter parts with semicolon
            params['filter'] = ';'.join(filter_parts)
        
        all_rows = []
        total_count = None
        
        while True:
            check_if_cancelled()  # Проверяем отмену
            
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
        
        return {'meta': data.get('meta', {}), 'rows': all_rows}
        
    except Exception as e:
        if str(e) == "Processing cancelled by user":
            abort(499, description="Processing cancelled by user")
        raise e

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
        return 0, '', ''  # Возвращаем 0 для скорости и пустую строку для UUID

    data = response.json()
    rows = data.get('rows', [])

    # Получаем UUID группы и название группы из первой строки данных
    group_uuid = ''
    group_name = ''
    if rows:
        assortment = rows[0].get('assortment', {})
        product_folder = assortment.get('productFolder', {})
        group_href = product_folder.get('meta', {}).get('href', '')
        group_uuid = group_href.split('/')[-1] if group_href else ''
        group_name = product_folder.get('name', '')  # Получаем название группы
        print(f"Found group UUID: {group_uuid}, name: {group_name}")

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

    print(f"sales_speed: {sales_speed}, group_uuid: {group_uuid}, group_name: {group_name}")

    return sales_speed, group_uuid, group_name  # Возвращаем также название группы

def get_group_path(group_uuid, product_groups):
    def find_group_path(groups, target_uuid, current_path=[]):
        for group in groups:
            if group['id'] == target_uuid:
                return current_path + [group['name']]
            if group.get('children'):
                path = find_group_path(group['children'], target_uuid, current_path + [group['name']])
                if path:
                    return path
        return None

    path = find_group_path(product_groups, group_uuid)
    return '/'.join(path) if path else ''

def create_excel_report(data, store_id, end_date, planning_days):
    try:
        print("Начало создания Excel отчета")
        wb = Workbook()
        ws = wb.active
        ws.title = "Отчет прибыльности"
        
        # Получаем структуру групп один раз
        product_groups = get_product_groups()

        # Добавляем заголовок для пути группы
        headers = ['Наименование', 'Количество', 'Прибыльность', 'Скорость продаж', 
                  f'Прогноз на {planning_days} дней', 'UUID группы', 'Наименование группы', 'Путь группы']
        for col, header in enumerate(headers, start=1):
            cell = ws.cell(row=1, column=col, value=header)
            cell.font = Font(bold=True, color="FFFFFF")
            cell.fill = PatternFill(start_color="000000", end_color="000000", fill_type="solid")
        
        # Создаем список для хранения всех данных
        rows_data = []
        
        # Собираем все данные
        for item in data['rows']:
            check_if_cancelled()
            assortment = item.get('assortment', {})
            assortment_meta = assortment.get('meta', {})
            assortment_href = assortment_meta.get('href', '')
            
            is_variant = '/variant/' in assortment_href
            variant_id = assortment_href.split('/variant/')[-1] if is_variant else assortment_href.split('/product/')[-1]
            
            row_data = {
                'name': assortment.get('name', ''),
                'quantity': item.get('sellQuantity', 0),
                'profit': round(item.get('profit', 0) / 100, 2),
                'sales_speed': 0,
                'forecast': '',
                'group_uuid': '',
                'group_name': '',
                'group_path': ''
            }
            
            if variant_id:
                sales_speed, group_uuid, group_name = get_sales_speed(variant_id, store_id, end_date, is_variant)
                if sales_speed != 0:
                    row_data['sales_speed'] = sales_speed
                    row_data['forecast'] = sales_speed * planning_days
                else:
                    row_data['sales_speed'] = "Нет данных"
                    row_data['forecast'] = ""
                
                row_data['group_uuid'] = group_uuid
                row_data['group_name'] = group_name
                row_data['group_path'] = get_group_path(group_uuid, product_groups)
            else:
                row_data['sales_speed'] = "Ошибка"
            
            rows_data.append(row_data)
        
        # Сортируем данные по UUID группы по убыванию
        rows_data.sort(key=lambda x: x['group_uuid'] if x['group_uuid'] else '', reverse=True)
        
        # Записываем отсортированные данные в Excel
        for row, row_data in enumerate(rows_data, start=2):
            ws.cell(row=row, column=1, value=row_data['name'])
            ws.cell(row=row, column=2, value=row_data['quantity'])
            ws.cell(row=row, column=3, value=row_data['profit'])
            ws.cell(row=row, column=4, value=row_data['sales_speed'])
            ws.cell(row=row, column=5, value=row_data['forecast'])
            ws.cell(row=row, column=6, value=row_data['group_uuid'])
            ws.cell(row=row, column=7, value=row_data['group_name'])
            ws.cell(row=row, column=8, value=row_data['group_path'])

        # Обновляем диапазон таблицы
        table_ref = f"A1:H{len(rows_data) + 1}"
        tab = Table(displayName="Table1", ref=table_ref)
        style = TableStyleInfo(
            name="TableStyleMedium9",
            showFirstColumn=False,
            showLastColumn=False,
            showRowStripes=True,
            showColumnStripes=False
        )
        tab.tableStyleInfo = style
        ws.add_table(tab)

        # Форматирование
        ws.freeze_panes = 'A2'
        
        # Автоподбор ширины столбцов
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
        wb.close()
        return filename
        
    except Exception as e:
        if str(e) == "Processing cancelled by user":
            abort(499, description="Processing cancelled by user")
        raise e
    finally:
        try:
            wb.close()
        except:
            pass


if __name__ == '__main__':
    app.run(debug=True)
