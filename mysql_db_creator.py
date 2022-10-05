from __future__ import print_function
import mysql.connector
from mysql.connector import errorcode
from yaml_reader import read_yaml_file

yams = read_yaml_file()


cnx = mysql.connector.connect(user=yams['user'], password=yams['password'], host=yams['host'])
cursor = cnx.cursor()
TABLES = {}

TABLES['prices'] = (
    "CREATE TABLE prices(day date, ticker varchar(10), "
    " open DECIMAL(12,2), high DECIMAL(12,2), "
    " low DECIMAL(12,2), close DECIMAL(12,2), "
    " adj_close DECIMAL(12,2), volume int, PRIMARY KEY (day, ticker))"
    )
TABLES['incomeStatement'] = (
"CREATE TABLE incomeStatements("
"ticker varchar(10), "
"day date,"                            
"researchDevelopment DECIMAL(12,2),"
"effectOfAccountingCharges DECIMAL(12,2),"
"incomeBeforeTax DECIMAL(12,2),"
"minorityInterest DECIMAL(12,2),"
"netIncome DECIMAL(12,2),"
"sellingGeneralAdministrative DECIMAL(12,2),"
"grossProfit DECIMAL(12,2),"
"ebit DECIMAL(12,2),"
"operatingIncome DECIMAL(12,2),"
"otherOperatingExpenses DECIMAL(12,2),"
"interestExpense DECIMAL(12,2),"
"extraordinaryItems DECIMAL(12,2),"
"nonRecurring DECIMAL(12,2),"
"otherItems DECIMAL(12,2),"
"incomeTaxExpense DECIMAL(12,2),"
"totalRevenue DECIMAL(12,2),"
"totalOperatingExpenses DECIMAL(12,2),"
"costOfRevenue DECIMAL(12,2),"
"totalOtherIncomeExpenseNet DECIMAL(12,2),"
"discontinuedOperations DECIMAL(12,2),"
"netIncomeFromContinuingOps DECIMAL(12,2),"
"netIncomeApplicableToCommonShares DECIMAL(12,2), PRIMARY KEY(ticker, day))"
)
def create_database(cursor):
    try:
        cursor.execute(
            "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(yams['database']))
    except mysql.connector.Error as err:
        print("Failed creating database: {}".format(err))
        exit(1)

try:
    cursor.execute("USE {}".format(yams['database']))
except mysql.connector.Error as err:
    print("Database {} does not exists.".format(yams['database']))
    if err.errno == errorcode.ER_BAD_DB_ERROR:
        create_database(cursor)
        print("Database {} created successfully.".format(yams['database']))
        cnx.database = yams['database']
    else:
        print(err)
        exit(1)
for table_name in TABLES:
    table_description = TABLES[table_name]
    try:
        print("Creating table {}: ".format(table_name), end='')
        cursor.execute(table_description)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("already exists.")
        else:
            print(err.msg)
    else:
        print("OK")

cursor.close()
cnx.close()
