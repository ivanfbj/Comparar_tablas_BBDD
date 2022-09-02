import pyodbc as pyodbc
from sqlalchemy.engine import URL
from sqlalchemy import create_engine

def prueba():
    return 'Prueba de que va funcionando lo que hago'

def windows_authentication(driver_name, server_name, database_name):
    try:
        connection_windows_authentication = pyodbc.connect(
            f'DRIVER={driver_name};SERVER={server_name};DATABASE={database_name};trusted_connection=true', autocommit=True)
        print('Conexión exitosa a la base de datos con pyodbc.')

        return connection_windows_authentication
        # return connection_windows_authentication.cursor()
    except Exception as ex:
        print('Error en la conexión a la base de datos con pyodbc.')

def windows_authentication_sqlalchemy(driver_name, server_name, database_name):
    try:
        connection_string: str = f'DRIVER={driver_name};SERVER={server_name};DATABASE={database_name};trusted_connection=true'
        connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
        print('Conexión exitosa a la base de datos con Sqlalchemy.')
        return create_engine(connection_url)
    except Exception as ex:
        print('Error en la conexión a la base de datos con Sqlalchemy')
