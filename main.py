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


def for_create_csv(in_server_name, in_database_name, list_type_data_select, in_list_object_to_load):
    info_file = InformationObject()
    for type_data in list_type_data_select:
        info_file.name_file, info_file.file_abspath, info_file.table_name = create_csv(in_server_name,
                                                                                       in_database_name,
                                                                                       sql.list_selected(type_data),
                                                                                       type_data)
        info_file.type_data = type_data
        in_list_object_to_load.append(copy.deepcopy(info_file))


def for_create_or_truncate_tables(local_server_name, in_local_database_name, in_list_object_to_load):
    engine = db.windows_authentication_sqlalchemy(local_server_name, in_local_database_name)
    for obj_info_file in in_list_object_to_load:
        is_create = sql.create_table_selected(engine, obj_info_file.table_name, obj_info_file.type_data)
        if not is_create:
            sql.truncate_table(engine, obj_info_file.table_name)
    engine.dispose()


def for_bulk_insert_csv(local_server_name, in_local_database_name, in_list_object_to_load):
    for obj_info_file in in_list_object_to_load:
        bki.bulk_insert_csv(local_server_name, in_local_database_name,
                            obj_info_file.table_name, obj_info_file.file_abspath)


if __name__ == '__main__':
    # TODO: cambiar la lista por un archivo JSON
    list_object_to_load = []
    # infoFile = InformationObject()
    # IF add element included in function list_selected of querys_sql.py
    type_data_select = ['tables', 'columns_tables', 'constraint_column_usage', 'constraint_table', 'stpr_parameters',
                        'view_column_usage', 'routines']

    local_database_name = 'LocalDB'
    local_server_name = 'localhost\SQLEXPRESS'

    database_name = 'AdventureWorks2019'
    server_name = 'localhost\SQLEXPRESS'
    for_create_csv(server_name, database_name, type_data_select, list_object_to_load)
    for_create_or_truncate_tables(local_server_name, local_database_name, list_object_to_load)
    for_bulk_insert_csv(local_server_name, local_database_name, list_object_to_load)

    database_name = 'AdventureWorks2012'
    server_name = 'localhost\SQLEXPRESS'
    for_create_csv(server_name, database_name, type_data_select, list_object_to_load)
    for_create_or_truncate_tables(local_server_name, local_database_name, list_object_to_load)
    for_bulk_insert_csv(local_server_name, local_database_name, list_object_to_load)

    database_name = 'AdventureWorks2016'
    server_name = 'localhost\SQLEXPRESS'
    for_create_csv(server_name, database_name, type_data_select, list_object_to_load)
    for_create_or_truncate_tables(local_server_name, local_database_name, list_object_to_load)
    for_bulk_insert_csv(local_server_name, local_database_name, list_object_to_load)

    print(f'{bcolors.BOLD_RED}ELEMENTOS DE LA LISTA{bcolors.RESET}')
    for obj in list_object_to_load:
        print(obj.return_dictionary())
