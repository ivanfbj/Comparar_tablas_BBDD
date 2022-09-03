from bcolors import bcolors
import connection_data_base as db
import pandas as pd
import time
import os
import querys_sql as sql
from create_csv import create_csv


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    database_name = 'AdventureWorks2019'
    server_name = 'AGN5\SQLEXPRESS'
    create_csv(server_name, database_name, sql.list_all_columns_tables(), 'tables')

    create_csv(server_name, database_name, sql.list_all_columns_tables(), 'columns_tables')
    # cursor.close()


