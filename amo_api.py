import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import time

# Параметры авторизации amoCRM
long_term_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImp0aSI6ImRjZDE5NzlkOGE2ZjVmZmQyNDBlYzNkOTJmN2I1YzYyYWM1YWNiZjE0MjZiZjYwM2ViYjllZmY4ZDk5MTQxNzE2NmM3YWIxZjkyZTFmOTFlIn0.eyJhdWQiOiI0NWY0ZDJhMi05YTBkLTQ5YzItYjMxNC1hNjUwNGM2YWI2MjIiLCJqdGkiOiJkY2QxOTc5ZDhhNmY1ZmZkMjQwZWMzZDkyZjdiNWM2MmFjNWFjYmYxNDI2YmY2MDNlYmI5ZWZmOGQ5OTE0MTcxNjZjN2FiMWY5MmUxZjkxZSIsImlhdCI6MTcyODU2Mzk2MCwibmJmIjoxNzI4NTYzOTYwLCJleHAiOjE4NDYxMDg4MDAsInN1YiI6IjcyOTkxNDUiLCJncmFudF90eXBlIjoiIiwiYWNjb3VudF9pZCI6Mjk2MjU0MDYsImJhc2VfZG9tYWluIjoiYW1vY3JtLnJ1IiwidmVyc2lvbiI6Miwic2NvcGVzIjpbImNybSIsImZpbGVzIiwiZmlsZXNfZGVsZXRlIiwibm90aWZpY2F0aW9ucyIsInB1c2hfbm90aWZpY2F0aW9ucyJdLCJoYXNoX3V1aWQiOiI5NDQ2M2QwZS1hZWNiLTQ4ZmUtOTcyMy05ZWUzZDE3ODE3NjEiLCJhcGlfZG9tYWluIjoiYXBpLWIuYW1vY3JtLnJ1In0.F18xsC11LNOjPqyzznfMYisY_z7cTD0njCS7aYDuwWmkSSPe3pqIacasVaBkT1oiHc14TO-ef30uy1Y6GJQI5MmTBAR2ckAwm63AEcYg8AUqAsjsxmSpFKFLlu-fZEVYV4hBEXUHpWY3XNvW67ij6V231uvNn9x_xXosxhyoZTBG_GnA1kFAvgNZRV2j2VgTIjffIUw-DL6eatmQe2UlgCyZzVXOOwbI4YGsZnegX4XwnVO5BRMErObm2R4_FnbmAfRS1A2M9jA0WEikpbVVMRRYKwK8X_wPcH2Td9fbI5TfhKO5hG7JOBNaBXkCIMaVfOIpg7rq2TAzSPnMh0ER_A"

url = 'https://murmanskandromeda.amocrm.ru/api/v4/leads'
headers = {
    'Authorization': f'Bearer {long_term_token}',
    'Content-Type': 'application/json'
}

tags_filter = [
    "Лид Feedot", "Лид robot@9111", "Gainnet.ru", "Лид_Закон_Партнер", "Заявки.рф", "Из файла", "импорт_15042022_1820", 
    "Файл", "UIS", "Чат в WhatsApp", "Группы ВКонтакте", "ЦентрЛидоген", "Telegram личный", "Яндекс.Директ", 
    "Яндекс.Директ консультация-юриста-мурамнск.рф", "Яндекс.Поиск", "Google Search", "Авито", "ЦентрЛидоген047", 
    "ЦентрЛидоген637", "Сайт"
]

# Настройка доступа к Google Sheets
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/spreadsheets", 
         "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name('autoamocrm-d37c97cc9b48.json', scope)
client = gspread.authorize(creds)
SPREADSHEET_ID = '108raO7JPW1JLLItA9pdu3pfe6vgQWiHWGLtOq2ksYNg'
worksheet = client.open_by_key(SPREADSHEET_ID).sheet1

# Очистка таблицы и добавление заголовков
worksheet.clear()
worksheet.append_row(['ID', 'Название', 'Цена', 'Дата', 'Тег', 'Источник', 'Комментарий'])

rows_to_add = []

def fetch_leads_page(url):
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = response.json()
        for lead in data['_embedded']['leads']:
            tags = [tag['name'] for tag in lead.get('_embedded', {}).get('tags', [])]
            if any(tag in tags_filter for tag in tags):
                price = lead.get('price', 'Не указано')
                created_at = datetime.fromtimestamp(lead['created_at']).strftime('%Y-%m-%d')
                
                source = ''
                comment = ''
                custom_fields = lead.get('custom_fields_values', [])
                if custom_fields:
                    for field in custom_fields:
                        if field['field_name'] == 'Источник обращений':
                            source = field['values'][0]['value']
                        elif field['field_name'] == 'Комментарий':
                            comment = field['values'][0]['value']

                print(f"ID: {lead['id']}, Название: {lead['name']}, Цена: {price}, Дата: {created_at}, Тег: {', '.join(tags)}, Источник: {source}, Комментарий: {comment}")
                
                rows_to_add.append([lead['id'], lead['name'], price, created_at, ', '.join(tags), source, comment])

        next_page = data['_links'].get('next', {}).get('href')
        if next_page:
            time.sleep(1)  # Задержка для предотвращения превышения лимита запросов
            fetch_leads_page(next_page)
    else:
        print(f"Ошибка: {response.status_code}")
        print(response.text)

fetch_leads_page(f"{url}?filter[created_at][from]=2023-01-01&filter[created_at][to]={datetime.today().strftime('%Y-%m-%d')}&with=tags,custom_fields_values&limit=50")

# Запись всех данных в Google Sheets одним запросом
worksheet.append_rows(rows_to_add)
print("Данные успешно обновлены в Google Sheets!")