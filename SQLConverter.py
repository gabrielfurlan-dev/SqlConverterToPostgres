import psycopg2
import pyodbc
import os
from termcolor import colored
from getpass  import getpass

def printData(text, exemple):
    print(colored(f'\n{text}', "white"))
    print(colored(f'ex: {exemple}', 'light_grey'))

def obterString(row):
    return row.replace('\'', '\'\'')

#Obtendo dados dos bancos
os.system('cls')

print("[====================================================]")

print(colored('[POSTGRES DATA]', 'green'))
printData('Enter your Postgres Server: ', 'locahost')
POSTGRES_HOST = input(colored('', 'green'))

printData('Enter your Postgres User: ', 'postgres')
POSTGRES_USER = input()

print(colored('\nEnter your Postgres Password: ', 'white'))
POSTGRES_PASSWORD = getpass()

printData('Enter your Postgres Database name: ', 'database_postgres_test')
POSTGRES_DATABASE_NAME = input()

print(colored("\n[SQL SERVER DATA]", "light_blue"))

printData('Enter your SqlServer Database name: ', 'database_sqlserver_test')
SQL_SERVER_DATABASE_NAME = input()

printData('Enter your SqlServer user: ', 'sa')
SQL_SERVER_USER = input()

print(colored('\nEnter your SqlServer Password: ', 'white'))
SQL_SERVER_PASSWORD = getpass()

print('\n[====================================================]')
print('\t\t   Processando . . .')
print('[====================================================]\n')

#conexão com o banco de dados postgres
try:
    conn_pg = psycopg2.connect(
        dbname=POSTGRES_DATABASE_NAME,
        password=POSTGRES_PASSWORD,
        user=POSTGRES_USER,
        host=POSTGRES_HOST
    )
except Exception as e:
    print("Erro ao conectar com o banco de dados PostgreSQL: " + str(e))
    exit()

#conexão com o banco de dados sql server
try:
    conn_sql = pyodbc.connect(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER=localhost;DATABASE={SQL_SERVER_DATABASE_NAME};UID={SQL_SERVER_USER};PWD={SQL_SERVER_PASSWORD}"
    )
except Exception as e:
    print("Erro ao conectar com o banco de dados SQL Server: " + str(e))
    exit()

#função para clonar todas as tabelas de um banco de dados sql server para postgres
def clone_tables():
    #obtendo todas as tabelas do banco de dados sql server
    cur_sql = conn_sql.cursor()
    cur_sql.execute("SELECT name FROM sysobjects WHERE xtype='U'")
    tables = cur_sql.fetchall()

    #criando as tabelas no banco de dados postgres
    for table in tables:
        #obtendo os campos e tipos das tabelas
        cur_sql = conn_sql.cursor()
        cur_sql.execute(f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table[0]}'")
        columns = cur_sql.fetchall()

        #montando o sql para criar a tabela
        sql = f"CREATE TABLE IF NOT EXISTS {table[0]} ("
        for i in range(len(columns)):
            if columns[i][1] == "uniqueidentifier":
                sql += f"{columns[i][0]} uuid"
            elif columns[i][1] == "smalldatetime":
                sql += f"{columns[i][0]} timestamp without time zone"
            elif columns[i][1] == "datetime":
                sql += f"{columns[i][0]} timestamp with time zone"
            elif columns[i][1] == "tinyint":
                sql += f"{columns[i][0]} smallint"
            elif columns[i][1] == "nvarchar":
                sql += f"{columns[i][0]} varchar"
            elif columns[i][1] == "nchar":  
                sql += f"{columns[i][0]} char"
            elif columns[i][1] == "ntext":
                sql += f"{columns[i][0]} text"
            elif columns[i][1] == "numeric":
                sql += f"{columns[i][0]} decimal"
            elif columns[i][1] == "smallmoney":
                sql += f"{columns[i][0]} numeric"
            elif columns[i][1] == "varbinary" :
                sql += f"{columns[i][0]} bytea"
            elif columns[i][1] == "image" :
                sql += f"{columns[i][0]} bytea"
            elif columns[i][1] == "boolean" :
                sql += f"{columns[i][0]} bit"
            else:
                sql += f"{columns[i][0]} {columns[i][1]}"
            if i < len(columns)-1:
                sql += ", "
        sql += ");"

        #executando o sql para criar a tabela
        cur_pg = conn_pg.cursor()
        cur_pg.execute(sql)

    #clonando os dados das tabelas no banco de dados postgres
    for table in tables:
        cur_sql = conn_sql.cursor()
        cur_sql.execute(f"SELECT * FROM {table[0]};")
        rows = cur_sql.fetchall()

        cur_pg = conn_pg.cursor()
        for row in rows:
            sql = f"INSERT INTO {table[0]} VALUES ("
            for i in range(len(row)):
                if type(row[i]) == str:
                    sql += f"'{obterString(row[i])}'"
                elif type(row[i]) == "datetime" or type(row[i]) == "smalldatetime": #Não funciona
                    sql += f"'{row[i].strftime('%Y-%m-%d %H:%M:%S')}'"
                elif type(row[i]) == bool:
                    sql += "'0'" if row[i] else "'1'"
                elif row[i] is None:
                    sql += "null"
                else:
                    sql += f"'{row[i]}'"
                if i < len(row)-1:
                    sql += ", "
            sql += ");"

            print(sql)

            cur_pg.execute(sql)
        conn_pg.commit()

#chamando a função para clonar as tabelas
clone_tables()

#fechando as conexões com o banco de dados
conn_sql.close()
conn_pg.close()

#Finalizando o código
print('\n[====================================================]\n')
print(colored('Conversion performed successfully', 'green\n'))
input('Press "Enter" to exit')
exit()
