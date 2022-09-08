import querys_sql as sql
from create_csv import create_csv
import bulk_insert_database as bki
import connection_data_base as db
from sqlalchemy import MetaData, Table, Column, Integer, String
import sqlalchemy
from sqlalchemy.orm import sessionmaker

# Press the green button in the gutter to run the script.

if __name__ == '__main__':
    database_name = 'AdventureWorks2019'
    server_name = 'AGN5\SQLEXPRESS'
    name_file, file_abspath = create_csv(server_name, database_name, sql.list_all_tables(), 'tables')
    table_name = name_file.replace('.csv', '')
    # print(name_file)
    # print(file_abspath)

    engine = db.windows_authentication_sqlalchemy(server_name, 'appTest')
    if not sqlalchemy.inspect(engine).has_table(name_file.replace('.csv', '')):
        metadata = MetaData(engine)
        Table(table_name, metadata,
              Column('ID', Integer, primary_key=True, nullable=False, autoincrement=False),
              Column('TABLE_SCHEMA', String),
              Column('TABLE_NAME', String)
              )
        metadata.create_all()

    meta_data = MetaData(engine)
    sqlalchemy.MetaData.reflect(meta_data)
    list_tables_table = meta_data.tables[table_name]

    result = sqlalchemy.select([sqlalchemy.func.count()]).select_from(list_tables_table).scalar()
    if result > 0:
        Session = sessionmaker(bind=engine)
        session = Session()
        session.execute(f'''TRUNCATE TABLE {table_name}''')
        session.commit()
        session.close()
        print(f'Tabla {table_name} borrada correctamente')

    print("Count:", result)
    bki.bulk_insert_csv(server_name, 'appTest', name_file.replace('.csv', ''), file_abspath)

    # name_file, file_abspath = create_csv(server_name, database_name, sql.list_all_columns_tables(), 'columns_tables')
    # print(name_file)
    # print(file_abspath)
    # cursor.close()
