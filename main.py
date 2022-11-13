import querys_sql as sql
from create_csv import create_csv
import bulk_insert_database as bki
import connection_data_base as db
from bcolors import bcolors
import copy


class InformationObject:
    def __init__(self) -> object:
        self.name_file = ''
        self.file_abspath = ''
        self.table_name = ''
        self.type_data = ''

    def print_information(self):
        print('***************************')
        print(f'The name file is: {self.name_file}')
        print(f'The file abspath is: {self.file_abspath}')
        print(f'The table name is: {self.table_name}')
        print('***************************')

    def return_dictionary(self):
        return dict({
            'name_file': self.name_file,
            'file_abspath': self.file_abspath,
            'table_name': self.table_name,
            'type_data': self.type_data
        })


if __name__ == '__main__':
    # TODO: cambiar la lista por un archivo JSON
    list_object_to_load = []
    infoFile = InformationObject()

    # database_name = 'dbStreaming'
    # server_name = 'AGN5\SQLEXPRESS'

    # database_name = 'CoderHouse'
    # # server_name = 'AGN5\SQLEXPRESS'

    # database_name = 'replica_dbSistemaInventarioTiendaSentimientos'
    # server_name = 'AGN5\SQLEXPRESS'

    database_name = 'AdventureWorks2019'
    server_name = 'AGN5\SQLEXPRESS'
    # IF add element included in function list_selected of querys_sql.py
    type_data_select = ['tables', 'columns_tables', 'constraint_column_usage', 'constraint_table', 'stpr_parameters',
                        'view_column_usage', 'routines']

    for type_data in type_data_select:
        infoFile.name_file, infoFile.file_abspath, infoFile.table_name = create_csv(server_name, database_name,
                                                                                    sql.list_selected(type_data),
                                                                                    type_data)
        infoFile.type_data = type_data
        list_object_to_load.append(copy.deepcopy(infoFile))

    engine = db.windows_authentication_sqlalchemy(server_name, 'appTest')
    for info_file in list_object_to_load:
        is_create = sql.create_table_selected(engine, info_file.table_name, info_file.type_data)
        if not is_create:
            sql.truncate_table(engine, info_file.table_name)

    for info_file in list_object_to_load:
        bki.bulk_insert_csv(server_name, 'appTest', info_file.table_name, info_file.file_abspath)

    print(f'{bcolors.BOLD_RED}ELEMENTOS DE LA LISTA{bcolors.RESET}')
    for obj in list_object_to_load:
        # print(obj.name_file)
        # obj.print_information()
        print(obj.return_dictionary())
    engine.dispose()
