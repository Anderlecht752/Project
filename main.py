import psycopg2
from  dotenv import load_dotenv
import os
import pandas as pd
from py_scripts.db_utils import process_by_day
#from sql_scripts.sql_scripts import sql_magic
"""
Устанавливаем конекшн с базой, формируем паттерны для поиска файлов 
и указываем пути к выгрузке и архиву
в конце запускаем обработку
"""
def main():

    load_dotenv()

    conn = psycopg2.connect(
        host = 'localhost',
        database = 'postgres',
        user = os.getenv("DATABASE_USER"),
        password = os.getenv("DATABASE_PASSWORD"),
        port = 5432
    )
    dsn = "postgresql://{user}:{password}@localhost:5432/postgres".format(
        user=os.getenv("DATABASE_USER"),
        password=os.getenv("DATABASE_PASSWORD")
        )

    files_LIST = ['transactions', 'passport_blacklist', 'terminals']
    folder = 'data'
    archive_folder = 'data/archive'

    process_by_day(files_LIST, folder, archive_folder, dsn, conn)


if __name__ == "__main__":
    # запускаем скрипт
    main() 
