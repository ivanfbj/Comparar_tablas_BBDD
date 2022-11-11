import querys_sql as sql
from create_csv import create_csv
import bulk_insert_database as bki
import connection_data_base as db
from sqlalchemy import MetaData, Table, Column, Integer, String
import sqlalchemy
from sqlalchemy.orm import sessionmaker
from bcolors import bcolors

class InformationObject:
    def __init__(self):
        self.name_file = ''
        self.file_abspath = ''
        self.table_name = ''

    def print_information(self):
        print('***************************')
        print(f'The name file is: {self.name_file}')
        print(f'The file abspath is: {self.file_abspath}')
        print(f'The table name is: {self.table_name}')
        print('***************************')


if __name__ == '__main__':
    list_object_to_load = []
    infoFile1 = InformationObject()
    infoFile2 = InformationObject()
    infoFile3 = InformationObject()
    infoFile4 = InformationObject()

    database_name = 'dbStreaming'
    server_name = 'AGN5\SQLEXPRESS'
    infoFile1.name_file, infoFile1.file_abspath, infoFile1.table_name = create_csv(server_name, database_name, sql.list_all_tables(), 'tables')
    list_object_to_load.append(infoFile1)

    database_name = 'AdventureWorks2019'
    # server_name = 'AGN5\SQLEXPRESS'
    infoFile2.name_file, infoFile2.file_abspath, infoFile2.table_name = create_csv(server_name, database_name, sql.list_all_tables(), 'tables')
    list_object_to_load.append(infoFile2)

    database_name = 'CoderHouse'
    # server_name = 'AGN5\SQLEXPRESS'
    infoFile3.name_file, infoFile3.file_abspath, infoFile3.table_name = create_csv(server_name, database_name, sql.list_all_tables(), 'tables')
    list_object_to_load.append(infoFile3)

    database_name = 'replica_dbSistemaInventarioTiendaSentimientos'
    # server_name = 'AGN5\SQLEXPRESS'
    infoFile4.name_file, infoFile4.file_abspath, infoFile4.table_name = create_csv(server_name, database_name, sql.list_all_tables(), 'tables')
    list_object_to_load.append(infoFile4)

    # TODO: Pendiente por mejorar para convertir en funciones.
    engine = db.windows_authentication_sqlalchemy(server_name, 'appTest')
    for info_file in list_object_to_load:
        if not sqlalchemy.inspect(engine).has_table(info_file.table_name):
            metadata = MetaData(engine)
            Table(info_file.table_name, metadata,
                  Column('ID', Integer, primary_key=True, nullable=False, autoincrement=False),
                  Column('TABLE_SCHEMA', String),
                  Column('TABLE_NAME', String)
                  )
            metadata.create_all()

        meta_data = MetaData(engine)
        sqlalchemy.MetaData.reflect(meta_data)
        name_table_for_count = meta_data.tables[info_file.table_name]

        result = sqlalchemy.select([sqlalchemy.func.count()]).select_from(name_table_for_count).scalar()
        if result > 0:
            Session = sessionmaker(bind=engine)
            session = Session()
            session.execute(f'''TRUNCATE TABLE {info_file.table_name}''')
            session.commit()
            session.close()
            print(f'Tabla {info_file.table_name} borrada correctamente')

        print("Count:", result)
        bki.bulk_insert_csv(server_name, 'appTest', info_file.table_name, info_file.file_abspath)

    # name_file, file_abspath = create_csv(server_name, database_name, sql.list_all_columns_tables(), 'columns_tables')
    # print(name_file)
    # print(file_abspath)
    # cursor.close()

    print(f'{bcolors.BOLD_RED}ELEMENTOS DE LA LISTA{bcolors.RESET}')
    for obj in list_object_to_load:
        # print(obj.name_file)
        obj.print_information()