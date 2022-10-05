import mysql.connector
from yahoo_fin.stock_info import get_income_statement, tickers_sp500
from mysql.connector import errorcode
from yaml_reader import read_yaml_file

yams = read_yaml_file()
tickers = tickers_sp500()
try:
    cnx = mysql.connector.connect(user=yams['user'], password=yams['password'], database=yams['database'], host=yams['host'])
    cur = cnx.cursor()
    for ticker in tickers:
        try:
            data = get_income_statement(ticker = ticker)
            data = data.transpose().fillna(0)
            print(ticker)
        except:
            print('could not insert: {}'.format(ticker))
            continue
        sql = ("INSERT INTO incomeStatements(day, ticker, researchDevelopment,effectOfAccountingCharges,incomeBeforeTax,minorityInterest,netIncome, "
        "sellingGeneralAdministrative,grossProfit,ebit, operatingIncome, otherOperatingExpenses, interestExpense, extraordinaryItems, nonRecurring, otherItems,"
        "incomeTaxExpense, totalRevenue, totalOperatingExpenses, costOfRevenue, totalOtherIncomeExpenseNet, discontinuedOperations, netIncomeFromContinuingOps,"
        "netIncomeApplicableToCommonShares) VALUES")
        count = 0
        for index, row in data.iterrows():
            sql = sql + '("{}", "{}", {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})'.format(
                index.date(), 
                ticker,
                row['researchDevelopment'],
                row['effectOfAccountingCharges'],
                row['incomeBeforeTax'],
                row['minorityInterest'],
                row['netIncome'],
                row['sellingGeneralAdministrative'],
                row['grossProfit'],
                row['ebit'],
                row['operatingIncome'],
                row['otherOperatingExpenses'],
                row['interestExpense'],
                row['extraordinaryItems'],
                row['nonRecurring'],
                row['otherItems'],
                row['incomeTaxExpense'],
                row['totalRevenue'],
                row['totalOperatingExpenses'],
                row['costOfRevenue'],
                row['totalOtherIncomeExpenseNet'],
                row['discontinuedOperations'],
                row['netIncomeFromContinuingOps'],
                row['netIncomeApplicableToCommonShares']
                )
            count = count + 1
            if count > 1:
                print(sql)
                cur.execute(sql + ';')
                sql = ("INSERT INTO incomeStatements(day, ticker, researchDevelopment,effectOfAccountingCharges,incomeBeforeTax,minorityInterest,netIncome, "
                        "sellingGeneralAdministrative,grossProfit,ebit, operatingIncome, otherOperatingExpenses, interestExpense, extraordinaryItems, nonRecurring, otherItems,"
                        "incomeTaxExpense, totalRevenue, totalOperatingExpenses, costOfRevenue, totalOtherIncomeExpenseNet, discontinuedOperations, netIncomeFromContinuingOps,"
                        "netIncomeApplicableToCommonShares) VALUES")
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