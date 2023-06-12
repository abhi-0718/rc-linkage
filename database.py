import pandas as pd
import numpy as np
from recordlinkage.preprocessing import clean 
from cryptography.fernet import Fernet
import mysql.connector
from mysql.connector import Error   
import geopy
from geopy.geocoders import Nominatim
import vl_convert as vlc
from splink.duckdb.duckdb_linker import DuckDBLinker

correct_columns_path = 'Dataset/Columns.xlsx'
columns_febrl = [ 'given_name', 'surname', 'address_1', 'address_2', 'suburb', 'state']
columns_usa = ['given_name','surname','birthdate','address','county','city','state','zip','latitude','longitude']

class Database():

    def create_connection(self, host_name, username, password, my_db):
        connection = None
        try:
            connection = mysql.connector.connect(
                host = host_name,
                user = username,
                user_password = password,
                database = my_db 

            )
            print("MySQL Database connection successful")
        except Error as err:
            print(f"Error :{err}")
        
        return connection
    
    
    def read_query(self, connection, tables, operation, check_which_encryption):

        tables = tables.split(',')
        cursor = connection.cursor()

        if operation=='link_only':

            if len(tables)==2:
                query_1 = f"select * from {tables[0]}"
                query_2 = f"select * from {tables[1]}" 
                
                cursor.execute(query_1)
                result_query_1 = cursor.fetchall()

                cursor.execute(query_2)
                result_query_2 = cursor.fetchall()

                dfA = []
                dfB = []

                for res in result_query_1:
                    dfA.append(list(res))
                
                for res in result_query_2:
                    dfB.append(list(res))
                
                if check_which_encryption == 'febrl':
                    dfA = pd.DataFrame(dfA,columns = columns_febrl)
                    dfB = pd.DataFrame(dfB,columns = columns_febrl)

                elif check_which_encryption=='usa':
                    dfA = pd.DataFrame(dfA,columns_usa)
                    dfB = pd.DataFrame(dfB,columns_usa)
                
                return dfA,dfB

                
        elif operation=='dedupe_only':

            if len(tables)==1:
                query = f"select * from {tables[0]}"
                
                cursor.execute(query)
                result_query = cursor.fetchall()
                df = []
                for res in result_query:
                    df.append(list(res))
                
                if check_which_encryption == 'febrl':
                    df = pd.DataFrame(df,columns = columns_febrl)

                elif check_which_encryption=='usa':
                    df = pd.DataFrame(df,columns = columns_usa)
                
                return df

            


        pass



