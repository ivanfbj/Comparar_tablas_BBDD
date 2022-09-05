import connection_data_base as db

def bulk_insert_csv(server_name, database_name, file_and_table_name, file_abspath):
    connect_database_pyodbc = db.windows_authentication_pyodbc(server_name, database_name)

    cursor = connect_database_pyodbc.cursor()

    cursor.execute(
        f"BULK INSERT {file_and_table_name} FROM '{file_abspath}' WITH(FIELDTERMINATOR='|', ROWTERMINATOR='\n', FIRSTROW=2)")
    connect_database_pyodbc.commit()

    cursor.close()
    connect_database_pyodbc.close()


