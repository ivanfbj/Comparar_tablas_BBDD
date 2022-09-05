
import querys_sql as sql
from create_csv import create_csv
import bulk_insert_database as bki

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    database_name = 'AdventureWorks2019'
    server_name = 'AGN5\SQLEXPRESS'
    name_file, file_abspath = create_csv(server_name, database_name, sql.list_all_tables(), 'tables')
    # print(name_file)
    # print(file_abspath)

    bki.bulk_insert_csv(server_name, 'appTest', name_file.replace('.csv', ''), file_abspath)

    # name_file, file_abspath = create_csv(server_name, database_name, sql.list_all_columns_tables(), 'columns_tables')
    # print(name_file)
    # print(file_abspath)
    # cursor.close()

