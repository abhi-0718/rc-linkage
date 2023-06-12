import pandas as pd
import numpy as np
import sqlite3
from recordlinkage.preprocessing import clean 
from cryptography.fernet import Fernet
import geopy
import json
import tempfile
from pathlib import Path
from uuid import uuid4
from pymysql import connect
from geopy.geocoders import Nominatim
import vl_convert as vlc
from splink.duckdb.duckdb_linker import DuckDBLinker

columns_to_encrypt = [ 'given_name', 'surname', 'address_1', 'address_2', 'suburb', 'state']
columns_to_encrypt_usa = ['given_name','surname','birthdate','address','county','city','state','zip','latitude','longitude']

class Preprocess():

    def __init__(self):

        try:
            with open('path.json','r') as config:
                self.params = json.load(config)

        except Exception as err:
            print(err)

    def generate_key(self):
        try:

            key = Fernet.generate_key()
            with open("secret.key",'wb') as key_file:
                key_file.write(key)
                print('key is generated')

        except Exception as err:
            print(err)
    
    def load_key(self):
        try:

            return open('secret.key','rb').read()
        
        except Exception as err:
            print(err)
    
    def Encrypter(self,input_dataframe,check_which_encryption):
        try:

            key = self.load_key()
            f = Fernet(key)
            if check_which_encryption == 'febrl':
                for col in self.params['febrl_columns']:
                    if col in list(input_dataframe.columns):

                        input_dataframe[col] = input_dataframe[col].apply(lambda x: f.encrypt(str(x).encode()))
            else:
                for col in self.params['custom_columns']:
                    if col in list(input_dataframe.columns):

                        input_dataframe[col] = input_dataframe[col].apply(lambda x: f.encrypt(str(x).encode()))

            
            return input_dataframe
        except Exception as err:
            print(err)

        

    def Decrpyter( self, dataframe, check_which_encryption):
        try:

            key = self.load_key()
            f = Fernet(key)
            if check_which_encryption == 'febrl':
                for col in self.params['febrl_columns']:
                    if col in dataframe.columns:
                    
                        dataframe[col] = dataframe[col].apply(lambda x : f.decrypt(x).decode())
            else:
                for col in self.params['custom_columns']:
                    if col in dataframe.columns:
                    
                        dataframe[col] = dataframe[col].apply(lambda x : f.decrypt(x).decode())

            return dataframe
        
        except Exception as err:
            print(err)
    
    def Data_Masking(self, dataframe):

        try:
            columns_of_dataframe = dataframe.columns
            for col in columns_of_dataframe:
                if 'cluster_id'==col:
                    continue
                dataframe[col] = dataframe[col].apply(lambda x: x if str(x) == 'nan' else str(x).replace(str(x)[-3::],"XXX"))

            return dataframe
        
        except Exception as err:
            print(err)

    # def Check_Columns_Present(input_dataframe):
        
    #     main_columns = ['address','city','state','zip','county','birthplace','first','last']
    #     consider_columns = []

    #     columns_ = list(input_dataframe.columns)
    #     columns_dataframe = pd.read_excel(correct_columns_path)

    #     actual_columns = list(columns_dataframe['Actual_Columns'])
    #     correct_columns = list(columns_dataframe['Correct_Columns'])
        
    #     for col in columns_:
            
    #         if col in main_columns:
    #             consider_columns.append(col)

    #         elif col in actual_columns:

    #             ind = actual_columns.index(col)
    #             new_column = correct_columns[ind]
    #             input_dataframe[new_column] = input_dataframe[col]
    #             input_dataframe.drop(columns = col)
    #             consider_columns.append(new_column)
            
    #     return input_dataframe[consider_columns]
        

    def record_linkage_preprocessing( self, dataframe, check_which_encryption):

        try:

            dataframe = self.Decrpyter(dataframe, check_which_encryption)
            columns_of_dataframe = dataframe.columns
            for col in columns_of_dataframe:
                if col=='latitude' or col=='longitude':
                    continue 
                result = dataframe[col].dtypes
                if str(result) == 'object':
                    dataframe[col] = clean(dataframe[col])
            
            dataframe = self.Encrypter(dataframe,check_which_encryption)
            return dataframe
        
        except Exception as err:
            print(err)

    def explore_dataframe(self, main_dataframe, check_which_encryption, operation):

        try:
            dataframe = main_dataframe.copy()
            dataframe = self.Decrpyter(dataframe, check_which_encryption)
            masked_dataframe = self.Data_Masking( dataframe)
            masked_dataframe['unique_id'] = [x for x in range(1,len(dataframe)+1)]
            basic_settings = {
                "link_type": operation
            }
            
            linker = DuckDBLinker(masked_dataframe,basic_settings)
            
            if check_which_encryption=='febrl':
                
                dataframe_columns = self.params['febrl_columns']
                spec_missing = linker.missingness_chart().spec
                spec_columns = linker.profile_columns(dataframe_columns, top_n=10, bottom_n=5).spec
                png_data = vlc.vegalite_to_png(vl_spec=spec_missing, scale=2)
                with open(self.params['base_febrl_path']+self.params['missingness'], "wb") as f:
                    f.write(png_data)
                
                png_data = vlc.vegalite_to_png(vl_spec=spec_columns, scale=2)
                with open(self.params['base_febrl_path']+self.params['columns_profile'], "wb") as f:
                    f.write(png_data)

            else:

                dataframe_columns = self.params['custom_columns']
                spec_missing = linker.missingness_chart().spec
                spec_columns = linker.profile_columns(dataframe_columns, top_n=10, bottom_n=5).spec
                png_data = vlc.vegalite_to_png(vl_spec=spec_missing, scale=2)
                with open(self.params['base_custom_path']+self.params['missingness'], "wb") as f:
                    f.write(png_data)
                
                png_data = vlc.vegalite_to_png(vl_spec=spec_columns, scale=2)
                with open(self.params['base_custom_path']+self.params['columns_profile'], "wb") as f:
                    f.write(png_data)

            return main_dataframe
        
        except Exception as err:
            print(err)


    def geocoding(self, dataframe, check_which_encryption):
        
        def geocoding_usa(df, geolocator, lat_field, lon_field):
            location = geolocator.reverse((df[lat_field], df[lon_field]))
            
            if 'state' in location.raw['address'] and df['state']!=location.raw['address']['state']:
                    df['state'] = location.raw['address']['state'] 

            if 'postcode' in location.raw['address'] and df['zip']!=location.raw['address']['postcode']:
                df['zip'] = location.raw['address']['postcode']
                
            if 'county' in location.raw['address'] and df['county']!=location.raw['address']['county']:
                df['county'] = location.raw['address']['county']
                
            if 'city' in location.raw['address'] and df['city']!=location.raw['address']['city']:
                df['city'] = location.raw['address']['city']

            return df
        
        def geocoding_febrl(df,geolocator,lat_field,lon_field):

            location = geolocator.reverse((df[lat_field], df[lon_field]))
            
            if 'state' in location.raw['address'] and df['state']!=location.raw['address']['state']:
                df['state'] = location.raw['address']['state'] 

            if 'postcode' in location.raw['address'] and df['postcode']!=location.raw['address']['postcode']:
                df['zip'] = location.raw['address']['postcode']
            
            if 'suburb' in location.raw['address'] and df['suburb']!=location.raw['address']['suburb']:
                df['suburb'] = location.raw['address']['suburb']
            
            return df

        if 'latitude' not in dataframe.columns and 'longitude' not in dataframe.columns:
            return dataframe
        
        dataframe = self.Decrpyter( dataframe, check_which_encryption)
        geolocator = geopy.Nominatim(user_agent = 'myGeocode')
        new_dataframe = dataframe[0:15].copy()
        if check_which_encryption == 'usa':
            new_dataframe = new_dataframe.apply(geocoding_usa, axis=1, geolocator=geolocator, lat_field='latitude', lon_field='longitude')
        
        elif check_which_encryption =='febrl':

            new_dataframe = new_dataframe.apply(geocoding_febrl,axis=1,geolocator=geolocator,lat_field = 'latitude',lon_field='longitude')
        
        dataframe[0:15] = new_dataframe
        dataframe = self.Encrypter(dataframe,check_which_encryption)
        
        return dataframe

    def create_sql_dataframe (self, hostname, username, password, database, tables, operation):
        try:
            sql_connection = connect(host = hostname, user = username, passwd=password, database=database)
            if operation == 'link_only':
                

                tables = tables.split(', ')
                query_1 = f'select * from {tables[0]}'
                query_2 = f'select * from {tables[1]}'

                dataframeA =  pd.read_sql(query_1, sql_connection)
                dataframeB =  pd.read_sql(query_2, sql_connection)

                return dataframeA, dataframeB

            elif operation == 'dedupe_only':
                print(tables)
                query = f'select * from {tables}'
                dataframe =  pd.read_sql(query, sql_connection)
                print(dataframe)

                return dataframe
            
        except Exception as err:
            print(err)



    
    def create_dataframe( self, database_file, tables, operation):

        try:

            with tempfile.NamedTemporaryFile() as fp:
                fp = Path(str(uuid4()))
                fp.write_bytes(database_file.getvalue())
                conn = sqlite3.connect(str(fp))
            
            if operation=='link_only':
                tables = tables.split(', ')
                query_1 = f'select * from {tables[0]}'
                query_2 = f'select * from {tables[1]}'

                dataframeA =  pd.read_sql(query_1,conn)
                dataframeB =  pd.read_sql(query_2,conn)

                return dataframeA, dataframeB

            elif operation=='dedupe_only':

                query = f'select * from {tables}'
                dataframe =  pd.read_sql(query,conn)

                return dataframe
            
        except Exception as err:
            print(err)

    def state_correction( self, dataframe, check_which_encryption):

        dataframe = self.Decrypter(dataframe,check_which_encryption)
        pass
        
    def county_correction( self, dataframe, check_which_encryption):

        dataframe = self.Decrypter(dataframe,check_which_encryption)
        pass

    def abbreviation_correction( self, dataframe, check_which_encryption):

        dataframe = self.Decrypter(dataframe,check_which_encryption)
        pass
    


