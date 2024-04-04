import pandas as pd
import os

def process_data(file_name):
    input_csv_file = f"data_test/{file_name}.csv"
    output_result_file = f"{file_name}result.txt"

    try:
        # Чтение CSV файла с указанием разделителя (табуляции)
        df = pd.read_csv(input_csv_file, sep='\t')

        # Преобразование дат в нужный формат
        df['timestamp'] = pd.to_datetime(df['timestamp'])

        # Извлекаем данные о приобретенных товарах
        filtered_data = df[df['action'] == 'confirmation']

        # Группировка данных по месяцам и годам, нахождение максимального значения для каждой группы
        max_values = filtered_data.groupby([
            filtered_data['timestamp'].dt.year,
            filtered_data['timestamp'].dt.month
        ]).apply(lambda x: x.sort_values('timestamp').nlargest(1, columns='value', keep='first'))

        # Преобразование индекса (год, месяц) обратно в формат даты
        max_values.index = pd.to_datetime(max_values.index.map(lambda x: f"{x[0]}-{x[1]}"))

        # Убираем данные, соответствующие первой и последней дате в таблице
        first_month = df['timestamp'].iloc[0].month
        first_year = df['timestamp'].iloc[0].year
        last_month = df['timestamp'].iloc[-1].month
        last_year = df['timestamp'].iloc[-1].year

        max_values = max_values.drop(pd.to_datetime(f"{first_year}-{first_month}"), errors='ignore')
        max_values = max_values.drop(pd.to_datetime(f"{last_year}-{last_month}"), errors='ignore')

        # Преобразование временной метки в формат без времени
        max_values['timestamp'] = max_values['timestamp'].dt.date

        # Запись результатов в файл
        with open(output_result_file, 'w') as file:
            # Запись результатов без верхних и нижних пробелов и выравнивание в столбик
            file.write(max_values[['timestamp', 'value']].to_string(index=False).strip())

        print(f"Результат записан в файл: {output_result_file}")

    except FileNotFoundError:
        print(f"Файл {input_csv_file} не найден.")

# Пример использования функции:
file_name = input("Введите название файла без расширения: ")
process_data(file_name)

