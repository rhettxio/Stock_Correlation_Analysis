from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.sql.functions import col
import pyspark.sql.functions as F
from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler
from datetime import datetime
import pandas as pd
import numpy as np
import settings as sts
from sqlalchemy import create_engine
import pymysql
import ast
import time
import data_etl as dtetl

# get user defined parameters
tickers = sts.LIST_STOCKS
start_date = sts.START_DATE
end_date = sts.END_DATE

# read table_ticker_dict.txt and get table ticker dictionary
file = open("table_tickers_dict.txt","r")
contents = file.read()
tabtk_dict = ast.literal_eval(contents)

sc = SparkContext()
spark = SparkSession.builder.appName("ReadfromDB").getOrCreate()

def read_from_database(spark, url, properties, select_sql):
    #Takes a spark dataframe df with standard parameters
    # read tables from postgresqlDB
    # return type: dataframe
    '''
    url = "jdbc:postgresql://ec2-##-###-###-###.compute-1.amazonaws.com:5432/DATABASE_NAME"
    properties = {"user": USER_NAME, "password": USER_PASSWORD, "driver": "org.postgresql.Driver"}
    table = TABLE_NAME
    and read the dataframe with select columns as a table from postgres.
    '''
    df = spark.read.jdbc(url=url, table=select_sql, properties=properties)
    return df

def get_dfwindow(df,start_date,end_date):
    # get return of select stocks and time period and drop the entries with null
    # input: df as return dataframe, start_date and end_date are strings to define time window
    # return type: dataframe

    dfwindow = df.filter(df.Datetime > start_date).filter(df.Datetime < end_date).na.drop()
    dfwindow = dfwindow.orderBy('Datetime',ascending=True)
    return dfwindow

def get_corr(dfrtselect):
    # get correltion matrix of selected stocks within time window
    # input: dataframe with selected tickers
    # return type: dataframe od correlation matrix

    cols = dfrtselect.columns
    cols.remove('Datetime')
    dfrtonly = dfrtselect.select(cols)
    # convert to vector column first
    vector_col = "corrcoef"
    assembler = VectorAssembler(inputCols=dfrtonly.columns, outputCol=vector_col)
    df_vector = assembler.transform(dfrtonly).select(vector_col)

    # get correlation matrix
    corr_matrix = Correlation.corr(df_vector, vector_col)
    return corr_matrix

def conn_db():
    #Returns database connection object
    try:
        host = "POSTGRESQLDB_PUBLIC_DNS"
        database = 'DATABASE_NAME'
        user = "USER_NAME"
        password = "USER_PASSWORD"

        conn_string = 'postgresql://' + user + ':' + password + '@' + host + ':5432/' + database

        engine = create_engine(conn_string)
    except (Exception, psycopg2.Error) as error :
        print ("Error while creating DB engine", error)

    return engine

def get_table_ticker(tickers,tabdict):
    # get the tickers (as values) and correspinding tables (as keys)
    # input: tickers as user defined ticker list
    # tabtk_dict as the dictionary with tablenames as keys, ticker symbols as values
    # return type: dictionary with tablenames as keys, ticker symbols as values

    # define a function returns intersection of two lists
    def intersection(lst1, lst2):
        return list(set(lst1) & set(lst2))

    tabtk_dic = {}
    for tbnm,tkl in tabdict.items():
        tabtk_dict.update({tbnm:intersection(tickers, tkl)})
    return tabtk_dict

if __name__ == "__main__":
    start_time = time.time()
    tab_tk_dict = get_table_ticker(tickers,tabtk_dict)

    url = "jdbc:postgresql://POSTGRESQLDB_PUBLIC_DNS:5432/DATABASE_NAME"
    properties = {"user":"USER_NAME", "password":"USER_PASSWORD", "driver":"org.postgresql.Driver"}
    tablecorr = "TABLE_NAME_CORRELATION"
    mode = "overwrite"
    tablert = "TABLE_NAME_RETURN"

    ct = 0
    for tbnm,tkl in tab_tk_dict.items():
        if tkl:
            ct += 1
            tklcap = []
            for tk in tkl:
                tklcap.append("\"" + tk + "\"")
            cols = ["\"Datetime\""] + tklcap
            select_tablert = "(SELECT " + ", ".join(cols) + " FROM " + tbnm + ") select_alias"
            #Read selected return from Database
            dftemp = read_from_database(spark, url, properties, select_tablert)
            if ct > 1:
                dfrt = dfrt.join(dftemp, on=["Datetime"], how='full')
            else:
                dfrt = dftemp

    # get return data within a time window
    dfrtwindow = get_dfwindow(dfrt,start_date,end_date)
    dfrtwindow.show(n=5)
    # get correlation matrix
    corr_matrix = get_corr(dfrtwindow)
    corr_matrix_array = corr_matrix.collect()[0][0].values
    matrix0 = np.reshape(corr_matrix_array, (-1, len(tickers)))

    coef_sum = 0
    n = len(tickers)
    for idx,coefrow in enumerate(matrix0):
        print(coefrow[idx+1:])
        coef_sum += sum(coefrow[idx+1:])
    coef_avg = coef_sum / (n*(n-1)/2)

    engine = conn_db()
    corrpddf = pd.DataFrame(matrix0)
    corrpddf.round(3)
    corrpddf.columns = tickers
    corrpddf.insert(0,"Symbol",tickers)
    corrpddf.to_sql('TABLE_NAME_CORRELATION', con=engine, index=False, if_exists='replace')
    dtetl.write_to_database(dfrtwindow, url, properties, mode, tablert)

    print(time.time() - start_time)
