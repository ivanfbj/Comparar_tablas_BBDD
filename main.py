from bcolors import bcolors
import connection_data_base as db
import pandas as pd
import time
import os
import querys_sql as sql
import warnings
# Con esta linea se puede omitir el mensaje de advertencia en el cual debo utilizar sqlalchemy con Pandas
# esa linea de advertencia se genera cuando se utiliza pyodbc en vez de sqlalchemy
# warnings.filterwarnings("ignore")

# print(f'Color por DEFECTO.')
# print(f'{bcolors.OK}Color VERDE.{bcolors.RESET}')
# print(f'{bcolors.WARNING}Color AMARILLO.{bcolors.RESET}')
# print(f'{bcolors.FAIL}Color ROJO.{bcolors.RESET}')
# print(f'{bcolors.HIGHLIGHTER}Letra en color BLANCO fondo en Color VERDE.{bcolors.RESET}')
# print(f'{bcolors.BOLD_RED}Negrita en Color ROJO.{bcolors.RESET}')

def print_message_hours(message):
    print(message, time.strftime("%H:%M:%S"))

def print_time_start_finished(message,time_start,time_finish):
    print(f'Tiempo en: {message}, INICIO-> {time_start} -  {time_start} <-FIN')

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    database_name = 'AdventureWorks2019'
    server_name = 'AGN5\SQLEXPRESS'
    connect_database_sqlalchemy = db.windows_authentication_sqlalchemy(server_name, database_name)

    start_sql_query = time.strftime("%H:%M:%S")
    sql_query_tables = pd.read_sql_query(sql.list_all_tables(), connect_database_sqlalchemy)
    finish_sql_query = time.strftime("%H:%M:%S")
    print_time_start_finished('read_sql_query', start_sql_query, finish_sql_query)

    print('***************************')

    start_data_frame = time.strftime("%H:%M:%S")
    df = pd.DataFrame(sql_query_tables)
    finish_data_frame = time.strftime("%H:%M:%S")
    print_time_start_finished('DataFrame', start_data_frame, finish_data_frame)

    print('***************************', end='')

    start_to_csv = time.strftime("%H:%M:%S")
    getdate = time.strftime("%Hh%Mm%Ss")
    # name_file = f'{database_name}_{getdate}.csv'
    name_file = f'{database_name}.csv'
    file_abspath = os.path.abspath(name_file)
    df.to_csv(file_abspath, sep='|', index=False)
    finish_to_csv = time.strftime("%H:%M:%S")
    print(bcolors.OK)
    print_time_start_finished('DataFrame_to_CSV', start_to_csv, finish_to_csv)
    print(bcolors.RESET)

    # cursor.close()
    connect_database_sqlalchemy.dispose()

