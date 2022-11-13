import connection_data_base as db
import time

def bulk_insert_csv(server_name, database_name, file_and_table_name, file_abspath):
    connect_database_pyodbc = db.windows_authentication_pyodbc(server_name, database_name)

    cursor = connect_database_pyodbc.cursor()
    try:
        print(f'Hora Inicio Bulk: {time.strftime("%H:%M:%S")} File: {file_and_table_name}')
        cursor.execute(
            f"BULK INSERT {file_and_table_name} FROM '{file_abspath}' WITH(FIELDTERMINATOR='|', ROWTERMINATOR='\n', FIRSTROW=2)")
        connect_database_pyodbc.commit()
        print(f'Hora Fin Bulk: {time.strftime("%H:%M:%S")}')
    except Exception as ex:
        print (f'Error al realizar bulk insert en {file_and_table_name}: {ex}')
    cursor.close()
    connect_database_pyodbc.close()


