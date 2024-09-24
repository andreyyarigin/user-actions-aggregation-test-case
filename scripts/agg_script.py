import pandas as pd
import os
import sys
from datetime import datetime, timedelta

def process_data(date_str):
    # Преобразуем строку в дату
    date = datetime.strptime(date_str, '%Y-%m-%d')
    
    # Определяем даты для диапазона
    start_date = date - timedelta(days=7)
    end_date = date - timedelta(days=1)
    
    # Путь к папке с входными файлами
    input_dir = 'input'
    
    # Путь к папке для сохранения результатов
    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)
    
    # Словарь для хранения данных
    data = {
        'email': [],
        'action': [],
        'dt': []
    }
    
    # Чтение данных из файлов за указанный диапазон
    for single_date in (start_date + timedelta(n) for n in range((end_date - start_date).days + 1)):
        file_path = os.path.join(input_dir, f"{single_date.strftime('%Y-%m-%d')}.csv")
        if os.path.exists(file_path):
            print(f"Processing file: {file_path}")
            df = pd.read_csv(file_path, names=['email', 'action', 'dt'])
            data['email'].extend(df['email'])
            data['action'].extend(df['action'])
            data['dt'].extend(df['dt'])
        else:
            print(f"File not found: {file_path}")
    
    # Создание DataFrame из данных
    df = pd.DataFrame(data)
    
    # Преобразуем столбец 'dt' в тип datetime
    df['dt'] = pd.to_datetime(df['dt'])
    
    # Группировка и агрегация данных
    aggregated_df = df.groupby('email').agg(
        create_count=pd.NamedAgg(column='action', aggfunc=lambda x: (x == 'CREATE').sum()),
        read_count=pd.NamedAgg(column='action', aggfunc=lambda x: (x == 'READ').sum()),
        update_count=pd.NamedAgg(column='action', aggfunc=lambda x: (x == 'UPDATE').sum()),
        delete_count=pd.NamedAgg(column='action', aggfunc=lambda x: (x == 'DELETE').sum())
    ).reset_index()
    
    # Запись результата в файл
    output_file = os.path.join(output_dir, f"{date.strftime('%Y-%m-%d')}.csv")
    print(f"Saving results to: {output_file}")
    aggregated_df.to_csv(output_file, index=False)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python script.py <YYYY-mm-dd>")
        sys.exit(1)

    process_data(sys.argv[1])
