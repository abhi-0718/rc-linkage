from pymysql import connect
import pandas as pd
from pandas.io import sql
from sqlalchemy import create_engine
import pymysql
pymysql.install_as_MySQLdb()


conn = connect(
    host = "localhost",
    user='root',
)

my_cursor = conn.cursor()
query = "create database data_linkage"
my_cursor.execute(query)
query = "create database deduplication"
my_cursor.execute(query)

df1 = pd.read_excel('Dataset/Input Datasets/EHR_Deduplication.xlsx',engine='openpyxl')
df2 = pd.read_excel('Dataset/Input Datasets/Febrl1_Data.xlsx',engine='openpyxl')
df3 = pd.read_excel('Dataset/Input Datasets/Febrl3_Data.xlsx',engine='openpyxl')

my_conn = create_engine("mysql+mysqldb://root:root@localhost/deduplication")

df1.to_sql(con = my_conn,name='EHR_Deduplication', if_exists='replace',index=False)
df2.to_sql(con = my_conn,name='Febrl1_Data', if_exists='replace',index=False)
df3.to_sql(con = my_conn,name='Febrl3_Data', if_exists='replace',index=False)

df1 = pd.read_excel('Dataset/Input Datasets/EHR_Linkage(1).xlsx')
df2 = pd.read_excel('Dataset/Input Datasets/EHR_Linkage(2).xlsx')
df3 = pd.read_excel('Dataset/Input Datasets/Febrl_Data4A.xlsx')
df4 = pd.read_excel('Dataset/Input Datasets/Febrl_Data4B.xlsx')

my_conn = create_engine("mysql+mysqldb://root:root@localhost/data_linkage")

df1.to_sql(con = my_conn,name='EHR_Linkage_1', if_exists='replace',index=False)
df2.to_sql(con = my_conn,name='EHR_Linkage_2', if_exists='replace',index=False)
df3.to_sql(con = my_conn,name='Febrl_Data4A', if_exists='replace',index=False)
df4.to_sql(con = my_conn,name='Febrl_Data4B', if_exists='replace',index=False)