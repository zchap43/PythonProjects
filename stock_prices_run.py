import mysql.connector
from yahoo_fin.stock_info import tickers_sp500,  get_data
from mysql.connector import errorcode
from yaml_reader import read_yaml_file

yams = read_yaml_file()

STARTDATE = '1/1/2021'
ENDDATE = '10/3/2022'
tickers = tickers_sp500()
try:
    cnx = mysql.connector.connect(user=yams['user'], password=yams['password'], database=yams['database'], host=yams['host'])
    cur = cnx.cursor()
    for ticker in tickers:
        try:
            data = get_data(ticker = ticker, start_date = STARTDATE, end_date = ENDDATE)
        except:
            print('could not insert: {}'.format(ticker))
            continue
        sql = 'INSERT INTO prices(day, ticker, open, high, low, close, adj_close, volume) VALUES'
        count = 0
        for index, row in data.iterrows():
            sql = sql + '("{}", "{}", {}, {}, {}, {}, {}, {})'.format(index.date(), ticker, row['open'], row['high'], row['low'], row['close'], row['adjclose'], row['volume'])
            count = count + 1
            if count > 1:
                print(sql)
                cur.execute(sql + ';')
                sql = 'INSERT INTO  prices(day, ticker, open, high, low, close, adj_close, volume) VALUES'
                count = 0
            else:
                sql = sql + ','
    cnx.commit()
except mysql.connector.Error as err:
  if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
    print("Something is wrong with your user name or password")
  elif err.errno == errorcode.ER_BAD_DB_ERROR:
    print("Database does not exist")
  else:
    print(err)
else:
    cnx.commit()
    cur.close()
    cnx.close()