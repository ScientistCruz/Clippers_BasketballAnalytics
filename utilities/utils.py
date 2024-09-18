import pyodbc
import sys
import datetime as dt
import sqlalchemy
import pandas as pd

class sql_server:

    def __init__(self, server = '', port = 1443, username = '', password = '') -> None:
        server = str(server + ',' + str(port))
        connect = 'DRIVER={ODBC Driver 18 for SQL Server}' + ';SERVER=' + 'sql_server,1433' + ';UID=' + str(username) + ';PWD=' + str(password) + ';TrustServerCertificate=yes;'
        self.connection = pyodbc.connect(connect)

    def execute_query_return_df(self, query_string, database_name):
        engine = sqlalchemy.create_engine('mssql+pyodbc://sa:tEST1234@sql_server/'+database_name+'?driver=ODBC+Driver+18+for+SQL+Server&Encrypt=no',fast_executemany=True, echo=False)
        df = pd.read_sql(query_string, engine)
        return df

    def return_engine_fast(self, database_name):
      engine = sqlalchemy.create_engine('mssql+pyodbc://sa:tEST1234@sql_server/'+database_name+'?driver=ODBC+Driver+18+for+SQL+Server&Encrypt=no',fast_executemany=True, echo=False)
      return engine

    def create_database(self, database=''):
        query_string = 'CREATE DATABASE ' + database
        try:

            self.exceute_query(query_string)
        except:
            error_value = str(sys.exc_info()[1])

            if 'already exists' in error_value:
                print(database + ' already exists!')
            else:
                pass

    def exceute_query(self, query_string):
        with self.connection as connection:
            # try:
            connection.autocommit = True
            connection.execute(query_string)
            connection.commit()   


    def try_create_table(self, query_string):

        try:
            self.exceute_query(query_string)

            rt = "Success"


        except:
            error_value = str(sys.exc_info()[1])


            if "There is already an object named" in error_value:
                rt = "Table Exists"

            else:
                print("Errors occurred:")
                print("Error value: " + error_value)
                rt = "Error"

        return rt

    # def big_table_sql(df,db_table_name, test_prod, merge_column, max_rows):
        
    #     # merge_column = [merge_column]

    #     if df.shape[0] > max_rows:
    #         total_uploads = math.ceil(df.shape[0]/max_rows)
    #         print("Total uploads to occur:", total_uploads)

    #         if merge_column == '':
    #             if test_prod.upper() == "TEST":
    #                 database_name = stg_database_name ='BA_SANDBOX_SC'
    #             elif test_prod.upper() == "PROD":
    #                 database_name = "SALESFORCE"
    #                 stg_database_name ='ANALYTICS_DW_STAGING'
    #             else:
    #                 sys.exit("Please define TEST or PROD.")
            
    #             stg_db_table_name = db_table_name + "_STAGING"

    #             print(stg_db_table_name)
    #             # Will remove
    #             db.execute_query_commit_result('TRUNCATE TABLE     ' + stg_database_name +'.DBO.' + stg_db_table_name)
    #             # print('After truncation.')
    #             df.to_sql(stg_db_table_name, db.return_engine_fast(stg_database_name), index = False, if_exists= 'append')
    #             print("Production Table Table updated.")


    #         else:
    #             for upload in range(total_uploads):
    #                 print(upload)
    #                 print('-- wait --')
    #                 time.sleep(10)
    #                 print('-- continue --')

    #                 if upload == 0:
    #                     df_temp = df[:max_rows * 1]
    #                     print('0 - ', max_rows * 1)
                        
    #                 elif upload == (total_uploads - 1):
    #                     df_temp = df[max_rows * upload:]
    #                     print(max_rows * upload,' - ', df.shape[0])

    #                 else:
    #                     df_temp = df[max_rows * upload : max_rows * (upload + 1)]
    #                     print(max_rows * (upload),' - ', max_rows * (upload + 1))

    #                 sql_query_bt(df_temp,db_table_name, test_prod, merge_column, upload)
    #                 print(df_temp)

    #     else:
    #         sql_query_bt(df,db_table_name, test_prod, merge_column, 0)

    #         pass

        
    #     return 

    def sql_query_bt(self, df,db_table_name, merge_column, upload):
    # Added logic for bigger tables.

        df['LOAD_DATETIME'] = dt.datetime.now()
        
        if len(merge_column) > 1:
            # merge_column = """TGT.""" + merge_column + """ = SRC. """ + merge_column 
            merge_text_list = []
            for column in merge_column:
                # print(column)
                merge_column_text = """TGT.""" + column + """ = SRC.""" + column
                merge_text_list.append(merge_column_text)
            merge_text_list = " AND ".join(merge_text_list)
            # print(merge_text_list)
        else:
            merge_text_list = """TGT.""" + merge_column[0] + """ = SRC. """ + merge_column[0]


        database_name = "lac_fullstack_dev"
        stg_database_name ='lac_fullstack_dev_staging'
        
        stg_db_table_name = db_table_name + "_STAGING"


        sql_query_column_list = []
        sql_mergequery_column_list = []
        sql_mergequery_column_list_null = []

        # print(df.columns)
        for column in df.columns:
            

            sql_mergecolumn = "TGT." + column + " = " + "SRC." + column

            if df[column].dtype == "object":
                sql_column = "[" + column + "] [nvarchar] (max) NULL"
            elif df[column].dtype == "bool" or df[column].dtype == "boolean":
                sql_column = "[" + column + "] [int] NULL"
            elif df[column].dtype == "int64" or df[column].dtype == "Int64" or df[column].dtype == "int32":
                sql_column = "[" + column + "] [int] NULL"

            elif df[column].dtype == "float32" or df[column].dtype == "float64" :
                sql_column = "[" + column + "] [float] NULL"
            elif df[column].dtype == "datetime64[ns]" or df[column].dtype == "dbdate" or  df[column].dtype == "datetime64[us]":
                sql_column = "[" + column + "] [datetime] NULL"

            else:
                print(df[column].dtype)
                print(df[column])
                print(column)
                print(str(column) + " type " + str(df[column].dtype) + " not mapped")


            sql_mergecolumn_null = str("(" + sql_mergecolumn + " OR (TGT." + column + " IS NULL AND SRC." + column + " IS NULL))")

            sql_mergequery_column_list.append(sql_mergecolumn)
            sql_mergequery_column_list_null.append(sql_mergecolumn_null)

            sql_query_column_list.append(sql_column)

        sql_query_column_list = ",".join(sql_query_column_list)
        sql_mergequery_column_list_and = " AND ".join(sql_mergequery_column_list_null[:-1])
        sql_mergequery_column_list_comma = ", ".join(sql_mergequery_column_list[:])
        sql_mergequery_column_list = ", ".join(df.columns.to_list())

        if '[nvarchar]' not in sql_query_column_list:
            create_table_query = ["""CREATE TABLE [""" + database_name + """].[dbo].[""" + db_table_name + """](""",sql_query_column_list, """) ON [PRIMARY]  """]
            create_stg_table_query = ["""CREATE TABLE [""" + stg_database_name + """].[dbo].[""" + stg_db_table_name + """](""",sql_query_column_list, """) ON [PRIMARY]  """]

        else:
            create_table_query = ["""CREATE TABLE [""" + database_name + """].[dbo].[""" + db_table_name + """](""",sql_query_column_list, """) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]  """]
            create_stg_table_query = ["""CREATE TABLE [""" + stg_database_name + """].[dbo].[""" + stg_db_table_name + """](""",sql_query_column_list, """) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]  """]

        create_table_query = " ".join(create_table_query)
        create_stg_table_query = " ".join(create_stg_table_query)
        
        # print(create_table_query)

        # print(create_stg_table_query)
        merge_tables_query = [""" MERGE INTO """ + database_name + """.DBO.""" + db_table_name + """ TGT 
                    USING """ + stg_database_name + """.DBO.""" + stg_db_table_name + """ SRC 
                        ON """ + merge_text_list  +"""
                    WHEN MATCHED THEN UPDATE SET""",
                        
                        sql_mergequery_column_list_comma,

                    """WHEN NOT MATCHED THEN INSERT 
                    (""",
                        sql_mergequery_column_list

                    ,"""    
                    )
                    VALUES 
                    (""",
                        sql_mergequery_column_list  
                    ,"""    
                    ); """]
        
        merge_tables_query = " ".join(merge_tables_query)
        # print(merge_tables_query)
        table = self.try_create_table(create_table_query)

        if upload == 0:
            if "Table Exists" in table:
                print(db_table_name + " in " + database_name + " already exists.")
            else:
                print(db_table_name + " created in " + database_name + ".")
                

            stg_table = self.try_create_table(create_stg_table_query)
            if "Table Exists" in stg_table:
                print(stg_db_table_name + " in " + stg_database_name + " already exists.")
            else:
                print(stg_db_table_name + " created in " + stg_database_name + ".")
        else:
            pass
            
    
        self.exceute_query('TRUNCATE TABLE     ' + stg_database_name +'.DBO.' + stg_db_table_name)
        # print('After truncation.')
        # df.to_sql(stg_db_table_name, self.connection, index = False, if_exists= 'replace')
        # df.to_sql(stg_db_table_name, self.return_engine_fast(stg_database_name), index = False, if_exists= 'replace', dtype = sql_dt)
        df.to_sql(stg_db_table_name, self.return_engine_fast(stg_database_name), index = False, if_exists= 'replace')




        # df.to_sql(stg_db_table_name, engine, index = False, if_exists= 'append') 
        print("Staging Table updated.")

        self.exceute_query(merge_tables_query)
        print("Production Table Table updated.")

        return database_name, stg_database_name




