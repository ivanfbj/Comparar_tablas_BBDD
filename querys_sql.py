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