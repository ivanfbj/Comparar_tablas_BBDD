from bcolors import bcolors
import connection_data_base as db
import pandas as pd
import time
import os
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
# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    database_name = 'AdventureWorks2019'
    # cursor = db.windows_authentication('SQL Server', 'AGN5\SQLEXPRESS','AdventureWorks2019')
    connect_database_sqlalchemy = db.windows_authentication_sqlalchemy('SQL Server', 'AGN5\SQLEXPRESS', database_name)
    # print(db.prueba())
    # database_name = 'AdventureWorks2019'
    # cursor.execute(f'''SELECT name FROM sysdatabases WHERE (name = '{database_name}')''')
    # row_database = cursor.fetchone()
    # print(row_database)

    print_message_hours('Inicio del sql_query con la conexión:')
    sql_query = pd.read_sql_query('''
        SELECT [DepartmentID]
          ,[Name]
          ,[GroupName]
          ,[ModifiedDate]
        FROM [AdventureWorks2019].[HumanResources].[Department]
    ''', connect_database_sqlalchemy)
    print_message_hours('Fin del sql_query con la conexión:')

    print('***************************')

    print_message_hours('INICIO crear DataFrame:')
    df = pd.DataFrame(sql_query)
    print_message_hours('FIN crear DataFrame:')

    print('***************************')

    print_message_hours('Inicio Generar csv:')
    getdate = time.strftime("%Hh%Mm%Ss")
    # name_file = f'{database_name}_{getdate}.csv'
    name_file = f'{database_name}.csv'
    file_abspath = os.path.abspath(name_file)
    df.to_csv(file_abspath, sep='|', index=False)
    print_message_hours(f'{bcolors.OK}Fin Generar csv:{bcolors.RESET}')
    print(f'Ruta completa donde queda el archivo csv: {bcolors.OK}{file_abspath}{bcolors.RESET}')

    # cursor.close()
    connect_database_sqlalchemy.dispose()

