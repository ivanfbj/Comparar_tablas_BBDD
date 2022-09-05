import time
import pandas as pd
import connection_data_base as db
from bcolors import bcolors
import os


def print_time_start_finished(message, time_start, time_finish):
    print(f'Tiempo en: {message}, INICIO-> {time_start} -  {time_finish} <-FIN')


def create_csv(server_name, database_name, sql_string, type_data):
    connect_database_sqlalchemy = db.windows_authentication_sqlalchemy(server_name, database_name)

    start_sql_query = time.strftime("%H:%M:%S")
    sql_query = pd.read_sql_query(sql_string, connect_database_sqlalchemy)
    finish_sql_query = time.strftime("%H:%M:%S")
    print_time_start_finished('read_sql_query', start_sql_query, finish_sql_query)

    print('***************************')

    start_data_frame = time.strftime("%H:%M:%S")
    df = pd.DataFrame(sql_query)
    finish_data_frame = time.strftime("%H:%M:%S")
    print_time_start_finished('DataFrame', start_data_frame, finish_data_frame)

    print('***************************', end='')

    start_to_csv = time.strftime("%H:%M:%S")
    getdate = time.strftime("%Hh%Mm%Ss")
    # name_file = f'{database_name}_{getdate}.csv'
    name_file = f'{type_data}_{database_name}.csv'
    file_abspath = os.path.abspath(name_file)
    df.to_csv(file_abspath, sep='|', index=True)
    finish_to_csv = time.strftime("%H:%M:%S")
    print(bcolors.OK)
    print_time_start_finished('DataFrame_to_CSV', start_to_csv, finish_to_csv)
    print(f'''Nombre del archivo: {name_file}
    Ruta del archivo: {file_abspath}''')
    print(bcolors.RESET)

    return name_file, file_abspath
