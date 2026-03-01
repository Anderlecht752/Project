import re
import os
import pandas as pd
from datetime import datetime
from sql_scripts.sql_scripts import sql_magic


def get_unique_dates(data_folder:str):
    """
    Docstring для get_unique_dates

    Сканирует папку и извлекает уникальные даты из имен файлов.

    Используется для формирования очереди загрузки в DWH (STG в SCD1/SCD2).
    Ожидает файлы формата, соответствующего регулярному выражению 
        (например, 'transactions_01032025.xlsx').

    Args:
        data_folder (str): Путь к директории с выгрузками (STG слой).
        pattern (re.Pattern): Скомпилированное регулярное выражение с группой для захвата даты.

    Returns:
        set: Множество строк с датами (напр., {'01032024', '02032024'}).
             Возвращает пустой сет, если файлы не найдены или не соответствуют паттерну.

    Функция нужна для обработки - извлечения дат из имен файлов, возвращает отсортированное 
    множество уникальных дат. на самом деле это только для случая, если файлы лежат в папке 
    'data_folder' сразу за несколько дней. по условию не было сказано, что будем кормить по 1 дню, 
    а написано, что "файлы будут лежать сразу за три дня", не знаю, как будет проверяться, поэтому 
    пришлось поупражняться, чтобы корректно обрабатывалось в любом случае. Если файлы даются как по 
    условию -  по 3 шт за каждый день каждый день, то эта обработка не нужна

    :param data_folder: укажите папку, "где файлы лежат" 
   
    """
    # Регулярка для поиска 8 цифр перед расширением txt или xsls
    pattern = re.compile(r'(\d{8})\.(?:txt|xlsx)$')
    dates_str = set()
    
    #собираем все подходящте паттерны в папке (если лежат файлы сразу за несколько дат)
    for f in os.listdir(data_folder):
        match = pattern.search(f)
        if match:
            dates_str.add(match.group(1))
    
    # Превращаем "строки" в объекты datetime для ПРАВИЛЬНОЙ сортировки
    dates_obj = [datetime.strptime(d, '%d%m%Y') for d in dates_str]
    dates_obj.sort() # Теперь 31.01.yyyy будет ПЕРЕД 01.02.yyyy потому как в "стр" и "инт" было бы наоборот
    
    # Возвращаем обратно в строковом формате для поиска файлов
    return [d.strftime('%d%m%Y') for d in dates_obj]


def process_by_day(lst: list, 
                   data_folder: str, 
                   archive_folder: str, 
                   dsn: str, 
                   conn, #:extensions.connection чтобы прописать этот тип его нужно импортировать, а нам он не нужен
                   schema: str ='bank', 
                   load_type: str = 'replace') -> None:
    """
    Docstring для process_by_day
    осуществляет инкрементную загрузку данных в БД

      
    :param lst: передаем паттерны имен файлов для загрузки
    :param data_folder: каталог с этими файлами
    :param archive_folder: каталог для обработанных файлов
    :param dsn: коннекшн к базе PSQL для алхимии
    :param conn: просто коннекшн для команд
    :param schema: схема PSQL куда грузим файлы и ведем дальнейшую обработку 
    :param load_type: Тип загрузки, т.к. грузим в стейдж - везде replace
    """
    date_obj = get_unique_dates(data_folder)
    if not date_obj: 
        print(f'Nothing to procces in folder {data_folder}')
        return
    #начинаем обработку файлов по старшинству дат
    for dt in date_obj:
        print(f"--- Начинаем обработку за день: {dt} ---")
        cnt=0 #для корректной работы нужны 3 файла, начинаем считать
        
        for name in lst:
            # Ищем файл конкретного типа транс/терм/блек_лист за конкретную дату
            files = [f for f in os.listdir(data_folder) if f.startswith(name) and dt in f]
            if not files:
                print(f" Пропуск: файл {name} за {dt} не найден")
                continue
            
            f = files[0] 
            #имена файлов помещаются в список, берем первый, 
            #хотя маловероятно, что там будет несколько 
            full_path = os.path.join(data_folder, f)

            try:
                #считываем файл в pandas
                df = pd.read_excel(full_path) if f.endswith('x') else pd.read_csv(full_path, sep=';')
                #грузим в STG таблицу
                df.to_sql(name=f'STG_{name}', con=dsn, schema=schema, if_exists=load_type, index=False)
                # логгируем результат
                log_data = pd.DataFrame([{
                    'file_name': f,
                    'table_name': f'STG_{name}',
                    'row_count': len(df),
                    'create_dt': datetime.now(),
                    'update_dt': datetime.now()
                }])
                #сохраняем лог в базе
                log_data.to_sql(name='meta_load_stats', con=dsn, schema='bank', if_exists='append', index=False)
                print(f'файл {f} обработан')
                cnt+=1

            except Exception as e:
                print(str(e))
                return
            
        if cnt != 3:
            print(f"На дату {dt} не хватает {3-cnt} файлов, загрузка инкремента невозможна.") 
            #тут немного не по заданию, но не знаю, куда эти файлы девать, если "некомплект"
            print(f"Проверьте содержимое, найденные на эту дату файлы перемещены в каталог {dt}")
            for name in lst:
                files = [f for f in os.listdir(data_folder) if f.startswith(name) and dt in f]
                if files:
                    mv_to_archive(os.path.join(data_folder, files[0]), dt)
        else:
            #в случае, если файло достаточно, запускаем скрипт обработки и построения витрины
            print(f'Здесь начинается магия SQL')
            sql_magic(conn)

            #отправляем обработанные файлы в архив 
            for name in lst:
                files = [f for f in os.listdir(data_folder) if f.startswith(name) and dt in f]
                if files:
                    mv_to_archive(os.path.join(data_folder, files[0]), archive_folder)

def mv_to_archive(path:str, archive_folder:str) -> None:
	"""
    Docstring для mv_to_archive
    переносит обработанные файлы в архив, добавляя '.backup' к имени
    :param path: имя файла
    :param archive_folder: каталог для архива
    """
	f_name = os.path.basename(path)
	new_path = os.path.join(archive_folder, f_name + '.backup')
	os.renames(path, new_path)


