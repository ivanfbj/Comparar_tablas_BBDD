from sqlalchemy import MetaData, Table, Column, Integer, String
import sqlalchemy
from sqlalchemy.orm import sessionmaker


def create_table_info_tables(conect_engine, table_name):
    if not sqlalchemy.inspect(conect_engine).has_table(table_name):
        metadata = MetaData(conect_engine)
        Table(table_name, metadata,
              Column('ID', Integer, primary_key=True, nullable=False, autoincrement=False),
              Column('TABLE_SCHEMA', String(150)),
              Column('TABLE_NAME', String(150))
              )
        metadata.create_all()
        return True
    else:
        return False


def list_all_tables():
    return '''
        SELECT
            TABLE_SCHEMA,
            TABLE_NAME
        FROM
            INFORMATION_SCHEMA.TABLES
        WHERE
            TABLE_TYPE = 'BASE TABLE'
    '''


def create_table_info_columns_tables(connect_engine, table_name):
    if not sqlalchemy.inspect(connect_engine).has_table(table_name):
        metadata = MetaData(connect_engine)
        Table(table_name, metadata,
              Column('ID', Integer, primary_key=True, nullable=False, autoincrement=False),
              Column('TABLE_SCHEMA', String(150)),
              Column('TABLE_NAME', String(150)),
              Column('COLUMN_NAME', String(150)),
              Column('ORDINAL_POSITION', Integer),
              Column('DATA_TYPE', String(150))
              )
        metadata.create_all()
        return True
    else:
        return False


def list_all_columns_tables():
    return '''
        SELECT
            TABLE_SCHEMA,
		    TABLE_NAME,
		    COLUMN_NAME,
		    ORDINAL_POSITION,
		    DATA_TYPE
        FROM
            INFORMATION_SCHEMA.COLUMNS
    '''


def create_table_info_constraint_column_usage(connect_engine, table_name):
    if not sqlalchemy.inspect(connect_engine).has_table(table_name):
        metadata = MetaData(connect_engine)
        Table(table_name, metadata,
              Column('ID', Integer, primary_key=True, nullable=False, autoincrement=False),
              Column('TABLE_SCHEMA', String(150)),
              Column('TABLE_NAME', String(150)),
              Column('COLUMN_NAME', String(150)),
              Column('CONSTRAINT_SCHEMA', String(150)),
              Column('CONSTRAINT_NAME', String(150))
              )
        metadata.create_all()
        return True
    else:
        return False


def list_all_constraint_column_usage():
    return '''
        SELECT 
            TABLE_SCHEMA
        ,	TABLE_NAME
        ,	COLUMN_NAME
        ,	CONSTRAINT_SCHEMA
        ,	CONSTRAINT_NAME	
        FROM INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE
    '''


def create_table_info_constraint_table(connect_engine, table_name):
    if not sqlalchemy.inspect(connect_engine).has_table(table_name):
        metadata = MetaData(connect_engine)
        Table(table_name, metadata,
              Column('ID', Integer, primary_key=True, nullable=False, autoincrement=False),
              Column('CONSTRAINT_SCHEMA', String(150)),
              Column('CONSTRAINT_NAME', String(150)),
              Column('TABLE_SCHEMA', String(150)),
              Column('TABLE_NAME', String(150)),
              Column('CONSTRAINT_TYPE', String(150))
              )
        metadata.create_all()
        return True
    else:
        return False


def list_all_constraint_table():
    return '''
        SELECT
            CONSTRAINT_SCHEMA
        ,	CONSTRAINT_NAME
        ,	TABLE_SCHEMA
        ,	TABLE_NAME
        ,	CONSTRAINT_TYPE
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
    '''


def create_table_info_stpr_parameters(connect_engine, table_name):
    if not sqlalchemy.inspect(connect_engine).has_table(table_name):
        metadata = MetaData(connect_engine)
        Table(table_name, metadata,
              Column('ID', Integer, primary_key=True, nullable=False, autoincrement=False),
              Column('SPECIFIC_SCHEMA', String(150)),
              Column('SPECIFIC_NAME', String(150)),
              Column('ORDINAL_POSITION', Integer),
              Column('PARAMETER_MODE', String(150)),
              Column('PARAMETER_NAME', String(150)),
              Column('DATA_TYPE', String(150))
              )
        metadata.create_all()
        return True
    else:
        return False


def list_all_stpr_parameters():
    return '''
        SELECT 
            SPECIFIC_SCHEMA
        ,	SPECIFIC_NAME
        ,	ORDINAL_POSITION
        ,	PARAMETER_MODE
        ,	PARAMETER_NAME
        ,	DATA_TYPE
        FROM INFORMATION_SCHEMA.PARAMETERS 
    '''


def create_table_info_view_column_usage(connect_engine, table_name):
    if not sqlalchemy.inspect(connect_engine).has_table(table_name):
        metadata = MetaData(connect_engine)
        Table(table_name, metadata,
              Column('ID', Integer, primary_key=True, nullable=False, autoincrement=False),
              Column('VIEW_SCHEMA', String(150)),
              Column('VIEW_NAME', String(150)),
              Column('TABLE_SCHEMA', Integer),
              Column('TABLE_NAME', String(150)),
              Column('COLUMN_NAME', String(150))
              )
        metadata.create_all()
        return True
    else:
        return False


def list_all_view_column_usage():
    return '''
        SELECT 
            VIEW_SCHEMA
        ,	VIEW_NAME
        ,	TABLE_SCHEMA
        ,	TABLE_NAME
        ,	COLUMN_NAME
        FROM INFORMATION_SCHEMA.VIEW_COLUMN_USAGE 
    '''


def create_table_info_routines(connect_engine, table_name):
    if not sqlalchemy.inspect(connect_engine).has_table(table_name):
        metadata = MetaData(connect_engine)
        Table(table_name, metadata,
              Column('ID', Integer, primary_key=True, nullable=False, autoincrement=False),
              Column('ROUTINE_TYPE', String(150)),
              Column('ROUTINE_NAME', String(150)),
              Column('LENGTH', Integer)
              )
        metadata.create_all()
        return True
    else:
        return False


def list_all_routines():
    return '''
        WITH ROUTINES AS ( 
            -- CANNOT use INFORMATION_SCHEMA.ROUTINES because of 4000 character limit 
            SELECT 
                'ROUTINE_TYPE' = o.type_desc 
            ,	'ROUTINE_NAME' = o.name
            ,	'ROUTINE_DEFINITION' = m.definition
            FROM sys.sql_modules AS m 
            INNER JOIN sys.objects AS o ON m.object_id = o.object_id 
        
        ) 
        
        SELECT
            ROUTINE_TYPE
        ,	ROUTINE_NAME
        ,	'LENGTH' = SUM(LEN(ROUTINE_DEFINITION))
        FROM ROUTINES 
        GROUP BY ROUTINE_TYPE,ROUTINE_NAME 
    '''


def truncate_table(connect_engine, table_name):
    meta_data = MetaData(connect_engine)
    sqlalchemy.MetaData.reflect(meta_data)
    name_table_for_count = meta_data.tables[table_name]

    result = sqlalchemy.select([sqlalchemy.func.count()]).select_from(name_table_for_count).scalar()
    if result > 0:
        Session = sessionmaker(bind=connect_engine)
        session = Session()
        session.execute(f'''TRUNCATE TABLE {table_name}''')
        session.commit()
        session.close()
        print(f'Tabla: {table_name} borrada correctamente. Cantidad eliminada: {result}')


'''
/*****************************/ 


;WITH ROUTINES AS ( 

    -- CANNOT use INFORMATION_SCHEMA.ROUTINES because of 4000 character limit 

    SELECT o.type_desc AS ROUTINE_TYPE 

            ,o.[name] AS ROUTINE_NAME 

            ,m.definition AS ROUTINE_DEFINITION 

    FROM sys.sql_modules AS m 

    INNER JOIN sys.objects AS o 

        ON m.object_id = o.object_id 

) 

SELECT ROUTINE_NAME,SUM(LEN(ROUTINE_DEFINITION)) 

FROM ROUTINES 

group by ROUTINE_NAME 

order by 2 desc 

 

 

/*****************************/ 
'''
