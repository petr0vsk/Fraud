import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv
import logging
import shutil
import io

# Загрузка переменных окружения из файла .env
load_dotenv()

# Настройки логирования
logging.basicConfig(
    filename='upload_log.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Получение данных для подключения к базе данных из файла .env
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')

# Укажите рабочую директорию
working_directory = '/home/petr0vsk/input'
target_directory = os.path.join(working_directory, 'tar')

# Создаем папку tar, если она не существует
if not os.path.exists(target_directory):
    os.makedirs(target_directory)

# Ожидаемое количество столбцов
expected_columns_count = 6


def validate_row(row):
    """
    Проверяет строку на количество столбцов и корректность типов данных.

    Аргументы:
        row (pd.Series): строка данных.

    Возвращает:
        bool: True, если строка корректна, иначе False.
    """
    if len(row) != expected_columns_count:
        return False
    try:
        int(row[0])  # Проверка ИНН (должен быть числом)
        str(row[1])  # Проверка названия организации (должно быть строкой)

        # Проверка суммы (должна быть числом с плавающей запятой)
        amount = row[2].replace(',', '.')
        float(amount)

        str(row[3])  # Проверка типа операции (должно быть строкой)
        int(row[4])  # Проверка кода валюты (должен быть целым числом)

        # Проверка даты и времени
        pd.to_datetime(row[5], format='%Y-%m-%d %H:%M:%S.%f', errors='raise')

    except (ValueError, TypeError):
        return False
    return True


def load_csv_to_db(csv_file, conn):
    """
    Загружает данные из CSV-файла во временную таблицу в базе данных.

    Аргументы:
        csv_file (str): путь к CSV-файлу.
        conn (psycopg2.extensions.connection): подключение к базе данных.
    """
    try:
        # Загрузка файла в DataFrame без заголовков
        df = pd.read_csv(csv_file, delimiter=';', header=None)

        # Проверка каждой строки
        if not all(df.apply(validate_row, axis=1)):
            raise ValueError(f"Некорректные данные в файле: {csv_file}")

        # Преобразование столбца с суммой (amount) в правильный формат с точкой
        df[2] = df[2].apply(lambda x: str(x).replace(',', '.'))

        cursor = conn.cursor()

        # Создание временной таблицы tmp_import
        cursor.execute("""
            CREATE TEMPORARY TABLE tmp_import (
                inn VARCHAR(20),
                name VARCHAR(255),
                amount DECIMAL(20, 2),
                transaction_type VARCHAR(50),
                currency_code VARCHAR(10),
                transaction_date TIMESTAMP
            );
        """)

        # Преобразование DataFrame в строку формата CSV
        output = io.StringIO()
        df.to_csv(output, sep=';', header=False, index=False)
        output.seek(0)

        # Загрузка данных из строки CSV в временную таблицу
        cursor.copy_expert(f"COPY tmp_import FROM STDIN WITH DELIMITER ';' CSV", output)

        conn.commit()
        cursor.execute("SELECT * FROM tmp_import LIMIT 5;")
        rows = cursor.fetchall()
        logging.info("Первые 5 строк из временной таблицы tmp_import:")
        for row in rows:
            logging.info(row)

        # Перемещение обработанного файла в папку tar
        base_name = os.path.basename(csv_file)
        new_name = f"{os.path.splitext(base_name)[0]}_tar.csv"
        shutil.move(csv_file, os.path.join(target_directory, new_name))
        logging.info(f"Файл {csv_file} перемещен в {target_directory} под именем {new_name}")

    except Exception as e:
        logging.error(f"Ошибка при обработке файла {csv_file}: {e}")
        conn.rollback()


def process_csv_files():
    """
    Обрабатывает все CSV-файлы в рабочей директории, загружая их данные в базу данных.
    """
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )

        # Поиск и обработка всех CSV-файлов в рабочей директории
        for filename in os.listdir(working_directory):
            if filename.endswith('.csv'):
                csv_file = os.path.join(working_directory, filename)
                load_csv_to_db(csv_file, conn)
                transfer_data_to_main_tables(conn)

        conn.close()

    except Exception as e:
        logging.error(f"Ошибка подключения к базе данных: {e}")


def transfer_data_to_main_tables(conn):
    """
    Переносит данные из временной таблицы tmp_import в основные таблицы organizations и transactions.

    Аргументы:
        conn (psycopg2.extensions.connection): подключение к базе данных.
    """
    try:
        cursor = conn.cursor()

        # Вставка данных в таблицу organizations, пропуская дубликаты
        cursor.execute("""
            INSERT INTO organizations (inn, name)
            SELECT DISTINCT inn, name
            FROM tmp_import
            ON CONFLICT (inn) DO NOTHING;
        """)

        # Вставка данных в таблицу transactions
        cursor.execute("""
            INSERT INTO transactions (org_id, amount, transaction_type, currency_code, transaction_date)
            SELECT o.org_id, t.amount, t.transaction_type, t.currency_code, t.transaction_date
            FROM tmp_import t
            JOIN organizations o ON t.inn = o.inn
            ON CONFLICT DO NOTHING;
        """)

        conn.commit()
        logging.info("Данные успешно перенесены из tmp_import в основные таблицы.")

    except Exception as e:
        logging.error(f"Ошибка при переносе данных: {e}")
        conn.rollback()


if __name__ == "__main__":
    process_csv_files()
