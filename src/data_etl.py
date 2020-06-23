from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import col
import pyspark.sql.functions as F
from datetime import datetime
from pyspark.sql.functions import to_date
import time

def get_openclose(df):
    # get open and close price
    # input df: (dataframe with Datetime, Adj Close, Close, Open, High, Low, Volume for each ticker
    # original column name as _c0(as Datetime), Adj Close1, ..., Adj Close3095,
    # Close 1, ..., Close 3095, Open 1,..., Open 3095, High1,... High 3095, Low1,... Low 3095,Volume 1,...
    # df header as ticker symbols
    # total # of columns = number of tickers * 6)
    # return type: dataframe with datetime as the first column
    # dataframe schema:
    '''
    root
     |-- Datetime: timestamp (nullable = true)
     |-- Close ticker1: double (nullable = true)
     |-- Close ticker2: double (nullable = true)
     |-- Close ticker3: double (nullable = true)
     |-- ''
     |-- ''
     |-- Open ticker1: double (nullable = true)
     |-- Open ticker2: double (nullable = true)
     |-- Open ticker3: double (nullable = true)
     |-- ''
     |-- ''
    '''
    # create list of column names as "Open symbol" or "Close symbol"
    colnm = []
    colnm0 = []
    tickers = []
    for x in df.columns:
        if x.startswith("Close"):
            if '.' not in df.select(x).head()[0]:
                tickers.append(df.select(x).head()[0])
                s = "Close " + df.select(x).head()[0]
            else:
                # replace . with _ in column names
                s = "Close " + df.select(x).head()[0].replace('.','_')
                tickers.append(df.select(x).head()[0].replace('.','_'))
            colnm.append(s)
            colnm0.append(x)
        if x.startswith("Open"):
            if '.' not in df.select(x).head()[0]:
                s = "Open " + df.select(x).head()[0]
            else:
                s = "Open " + df.select(x).head()[0].replace('.','_')
            colnm.append(s)
            colnm0.append(x)

    cols = ["Datetime"] + colnm
    cols0 = ["_c0"] + colnm0
    # convert original columnnames (Close 1, Close 2,...) to (Close AAPL, Close AMZN,...)
    df = df.select(cols0).toDF(*cols)
    df = df.filter(df.Datetime != "null").filter(df.Datetime != "Datetime")
    # convert DataType of columns to double, and Daetime column DataType as timestamp
    df = df.select(["Datetime"] + [*(col(c).cast("double").alias(c) for c in df.columns if c != "Datetime")])
    df = df.withColumn("Datetime", df["Datetime"].cast('timestamp'))
    # drop row if datetime has na value
    df = df.filter(df.Datetime.isNotNull())
    # for minute data only
#    df = df.withColumn("Datetime", df.Datetime + F.expr('INTERVAL -4 HOURS'))
    return [tickers,df]

def get_close(tickers,df):
    # get the close price
    # df: dataframe with Datetime Close and Open price for each ticker
    # tickers: a list of string for all tickers
    # return type: dataframe
    # dataframe schema:
    '''
    root
     |-- Datetime: timestamp (nullable = true)
     |-- ticker1: double (nullable = true)
     |-- ticker2: double (nullable = true)
     |-- ticker3: double (nullable = true)
     |-- ticker4: double (nullable = true)
     |-- ticker5: double (nullable = true)
    '''

    for i in tickers:
        df = df.withColumn(i, df['Close '+i])

    df = df.select([df.columns[0]] + tickers)
    return df

def get_return(tickers,dfwithrt):

    # get the return data by (Close ticker - Open ticker) / Open ticker
    # dfwithrt: dataframe with Datetime, Close and Open price for each ticker
    # tickers: a list of string for all stock tickers
    # return type: dataframe
    # dataframe schema:
    '''
    root
     |-- Datetime: timestamp (nullable = true)
     |-- ticker1: double (nullable = true)
     |-- ticker2: double (nullable = true)
     |-- ticker3: double (nullable = true)
     |-- ticker4: double (nullable = true)
     |-- ticker5: double (nullable = true)
    '''

    for i in tickers:
        try:
            dfwithrt = dfwithrt.withColumn(i, F.round(100*(dfwithrt['Close '+i] - dfwithrt['Open '+i])\
             / dfwithrt['Open '+i],3))
        except Exception as e:
            # handle division by 0
            print("Return Calculation Error:",e)

    dfrt = dfwithrt.select([dfwithrt.columns[0]] + tickers)
    return dfrt

def write_to_database(df, url, properties, mode, table):

    #Takes a spark dataframe df with standard parameters
    '''
    url = "jdbc:postgresql://ec2-##-###-###-###.compute-1.amazonaws.com:5432/DATABASE_NAME"
    properties = {"user": USER_NAME, "password": USER_PASSWORD, "driver": "org.postgresql.Driver"}
    mode = "overwrite"
    table = TABLE_NAME
    and writes the dataframe as a table in postgres.
    '''
    df.write.jdbc(url=url, table=table, mode=mode, properties=properties)

def table_ticker_map(tickers,n):
    # split ticker list into sublist with length n and map the sublists with tablenames
    # input: tickers as a list of tickers, n as number of columns of table
    # return type: dictionary {'table0':['ticker0','ticker2',...'tickern-1'],
    #                          'table0':['tickern','tickern+2',...'ticker2n-1'],
    #                           ..........}
    # list of tablenames for return data
    rttablenames = []
    tabdict = {}
    
    for i,j in enumerate(range(0,len(tickers),n)):
        rttablenames.append('table'+str(i))
        tabdict[rttablenames[i]] = tickers[j:j + n]
    return tabdict

if __name__ == "__main__":

    t0 = time.time()
    spark = SparkSession.builder.appName("Readrawdata").getOrCreate()

    #bucketname = 'nysestockdaily'
    bucketname = 'sp500daily'

    print("read data from s3------------------------------------------------------------")
    # read raw data from s3
    spdf = spark.read.option('maxColumns',150000).csv("s3a://" + bucketname,header= True, inferSchema = True)
    print("read data from s3 done-------------------------------------------------------")

    print("get open close price-------------------------------------------------------")
    # get close and open price
    [alltickers,dfopenclose] = get_openclose(spdf)
    print("get open close done-------------------------------------------------------")

    # define database parameters
    url = "jdbc:postgresql://mypostgresql.coui47h5pc42.us-east-1.rds.amazonaws.com:5432/stocks"
    mode = "overwrite"
    properties = {"user":"anqi", "password":"xaq123456", "driver":"org.postgresql.Driver"}

    print("get tabledict------------------------------------------------------------")
    # set max 500 columns of a table
    n = 50
    tabledict = table_ticker_map(alltickers,n)

    print("write table tickers dict------------------------------------------------")
    # save table_ticker_dict to a .txt file
    f = open("table_tickers_dict.txt","w")
    f.write(str(tabledict))
    f.close()
    print("write table tickers dict done------------------------------------------------")

    for tab,tklst in tabledict.items():
        print("get return------------------------------------------------------------")
        #get return data
        dfrt = get_return(tklst,dfopenclose)
        dfrt.show()
        print('Write return to Database.................................................')
        # write return data to database
        write_to_database(dfrt, url, properties, mode, tab)
        print('Write to database done!')

    print(time.time() - t0)
    print("Program run successfully!")
