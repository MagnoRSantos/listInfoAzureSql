# -*- coding: utf-8 -*-

## imports
import os, io
import sqlite3
import dotenv
import pyodbc as po
#import json
import pandas as pd
from tabulate import tabulate
from datetime import datetime


## Local raiz da aplicacao
dirapp = os.path.dirname(os.path.realpath(__file__))

## Carrega os valores do .env
dotenvProd = os.path.join(dirapp, '.env.prod')
dotenv.load_dotenv(dotenvProd)

## funcao que retorna data e hora Y-M-D H:M:S
def obterDataHora():
    datahora = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    return datahora


## funcao de gravacao de log
def GravaLog(strValue, strAcao):

    ## Path LogFile
    datahoraLog = datetime.now().strftime('%Y-%m-%d')
    pathLog = os.path.join(dirapp, 'log')
    pathLogFile = os.path.join(pathLog, 'loginfoDatabaseAzureSqlApi.txt')

    if not os.path.exists(pathLog):
        os.makedirs(pathLog)
    else:
        pass

    msg = strValue
    with io.open(pathLogFile, strAcao, encoding='utf-8') as fileLog:
        fileLog.write('{0}\n'.format(strValue))

    return msg


## funcao de formacao da connString Database de origem
def strConnectionDatabaseOrigem(p_server):

    #variaveis de conexao azuresql
    if p_server == 'azuredb040':
        server   = os.getenv("SERVER_SOURCE_AZURESQL")

    if p_server == 'sonarqube':
        server   = os.getenv("SERVER_SOURCE_SONARQUBE_AZURESQL")

    port     = os.getenv("PORT_SOURCE_AZURESQL")
    database = os.getenv("DATABASE_SOURCE_AZURESQL") ##"##DATABASE.NAME##"
    username = os.getenv("USERNAME_SOURCE_AZURESQL")
    password = os.getenv("PASSWORD_SOURCE_AZURESQL")

    strConnection = 'DRIVER={{ODBC Driver 17 for SQL Server}};\
        SERVER={v_server};\
        PORT={v_port};\
        DATABASE={v_database};\
        UID={v_username};\
        PWD={v_password}'.format(v_server = server, v_port = port, v_database = database, v_username = username, v_password = password)

    return strConnection


## funcao de formacao da connString Database de destino
def strConnectionDatabaseDestino():

    #variaveis de conexao azuresql
    server   = os.getenv("SERVER_TARGET_AZURESQL")
    port     = os.getenv("PORT_TARGET_AZURESQL")
    database = os.getenv("DATABASE_TARGET_AZURESQL")
    username = os.getenv("USERNAME_TARGET_AZURESQL")
    password = os.getenv("PASSWORD_TARGET_AZURESQL")

    strConnection = 'DRIVER={{ODBC Driver 17 for SQL Server}};\
        SERVER={v_server};\
        PORT={v_port};\
        DATABASE={v_database};\
        UID={v_username};\
        PWD={v_password}'.format(v_server = server, v_port = port, v_database = database, v_username = username, v_password = password)

    return strConnection


## funcao para obter os nomes dos databases
def getListNameDatabasesOrigem(p_server):

    try:

        connString = str(strConnectionDatabaseOrigem(p_server)).replace('##DATABASE.NAME##', str('master'))
        cnxn = po.connect(connString)
        cursor = cnxn.cursor()

        sqlcmd = """
        SELECT [name] FROM sys.databases WHERE [name] <> 'master'
        """

        listDbNames = list(cursor.execute(sqlcmd).fetchall())
        #listDbNames = []
        #cursor.execute(sqlcmd)
        #listDbNames = list(cursor.fetchall())

    except Exception as e:
        datahora = obterDataHora()
        msgException = "Error: {0}".format(e)
        msgLog = 'Erro ao obter os nomes dos databases: {0}\n{1}'.format(datahora, msgException)
        print(GravaLog(msgLog, 'a'))

    finally:
        cursor.close()
        del cursor
        cnxn.close()
        datahora = obterDataHora()
        msgLog = 'Concluido a coleta dos nomes dos databases - {0}'.format(datahora)
        print(GravaLog(msgLog, 'a'))

    return listDbNames


## funcao para obter informações dos databases
def getListInfoDatabasesOrigem(p_server, p_listDBNames):

    try:

        listInfoDbNames = []
        listDbsAux = []

        tamlist =  range(len(p_listDBNames))
        for i in tamlist:
            v_dbname = str(p_listDBNames[i][0])

            msgLog = 'Coletando informacoes do database: [{0}]'.format(v_dbname)
            print(GravaLog(msgLog, 'a'))

            connString = str(strConnectionDatabaseOrigem(p_server)).replace('##DATABASE.NAME##', v_dbname)
            cnxn = po.connect(connString)
            cursor = cnxn.cursor()

            sqlcmd = """
              ; WITH SpaceDBAux AS (
                SELECT
                    DB_NAME() as DatabaseName,
                    (CAST(DATABASEPROPERTYEX(DB_NAME(), 'MaxSizeInBytes') as bigint))/(1024*1024*1024) as [MaxStorageinGB],
                    SUM(size/128.0)/1024 AS [AllocatedSpaceinGB],
                    SUM(size/128.0 - CAST(FILEPROPERTY(name, 'SpaceUsed') AS int)/128.0) /1024 AS [AllocatedSpaceUnusedInGB]
                    FROM sys.database_files
                    GROUP BY type_desc
                    HAVING type_desc = 'ROWS'
            ), SpaceDBFinal AS (
                SELECT
                    ([AllocatedSpaceinGB] - [AllocatedSpaceUnusedInGB]) AS [UsedSpaceinGB],
                    [MaxStorageinGB],
                    [AllocatedSpaceinGB],
                    [MaxStorageinGB] - (([AllocatedSpaceinGB] - [AllocatedSpaceUnusedInGB])) AS [RemainingSpaceinGB]
                FROM SpaceDBAux
            ), InfoDbsFinal (
                DatabaseName, Edition, ServiceObjective, [MaxStorageinGB],
                [AllocatedSpaceinGB], [UsedSpaceinGB], [RemainingSpaceinGB])
            AS (
                SELECT
                    DB_NAME() AS DatabaseName,
                    CAST(DATABASEPROPERTYEX(DB_NAME(), 'Edition') AS VARCHAR(60)) as Edition,
                    CAST(DATABASEPROPERTYEX(DB_NAME(), 'ServiceObjective') AS VARCHAR(60)) as ServiceObjective,
                    [MaxStorageinGB],
                    [AllocatedSpaceinGB],
                    [UsedSpaceinGB],
                    [RemainingSpaceinGB]
                FROM SpaceDBFinal
            ), InfoCapacityDtu ([DatabaseName], [CapacityDtu]) AS (
                SELECT
                    DB_NAME() AS DatabaseName,
                    Capacity = CASE DATABASEPROPERTYEX(DB_NAME(), 'ServiceObjective')
                        --BASIC
                        WHEN 'Basic' THEN 5

                        --STANDARD
                        WHEN 'S0' THEN 10
                        WHEN 'S1' THEN 20
                        WHEN 'S2' THEN 50
                        WHEN 'S3' THEN 100
                        WHEN 'S4' THEN 200
                        WHEN 'S6' THEN 400
                        WHEN 'S7' THEN 800
                        WHEN 'S9' THEN 1600
                        WHEN 'S12' THEN 3000

                        --PREMIUM
                        WHEN 'P1' THEN 125
                        WHEN 'P2' THEN 250
                        WHEN 'P4' THEN 500
                        WHEN 'P6' THEN 1000
                        WHEN 'P11' THEN 1750
                        WHEN 'P15' THEN 4000

                        ELSE 'N/D'
                    END
            )
            SELECT
                @@SERVERNAME AS [SERVERNAME],
                I.DatabaseName,
                I.Edition,
                I.ServiceObjective,
                D.CapacityDtu,
                I.MaxStorageinGB,
                I.AllocatedSpaceinGB,
                I.UsedSpaceinGB,
                I.RemainingSpaceinGB
            FROM InfoDbsFinal I
            INNER JOIN InfoCapacityDtu D ON D.DatabaseName = I.DatabaseName
            """

            registros = cursor.execute(sqlcmd).fetchall()
            for registro in registros:
                ServerName         = registro[0]
                DatabaseName       = registro[1]
                Edition            = registro[2]
                ServiceObjective   = registro[3]
                CapacityDtu        = registro[4]
                MaxStorageinGB     = registro[5]
                AllocatedSpaceinGB = registro[6]
                UsedSpaceinGB      = registro[7]
                RemainingSpaceinGB = registro[8]

                strListValues = '{0},{1},{2},{3},{4},{5},{6},{7},{8}'\
                    .format(ServerName, DatabaseName, Edition, ServiceObjective, CapacityDtu,
                            MaxStorageinGB, AllocatedSpaceinGB, UsedSpaceinGB, RemainingSpaceinGB)

                listDbsAux = strListValues.split(',')
                listInfoDbNames.append(listDbsAux)


    except Exception as e:
        datahora = obterDataHora()
        msgException = "Error: {0}".format(e)
        msgLog = 'Erro ao obter informacoes dos databases: {0}\n{1}'.format(datahora, msgException)
        print(GravaLog(msgLog, 'a'))

    finally:
        cursor.close()
        del cursor
        cnxn.close()
        datahora = obterDataHora()
        msgLog = 'Concluido a coleta das informacoes dos databases - {0}'.format(datahora)
        print(GravaLog(msgLog, 'a'))

    return listInfoDbNames


## Funcao de criacao do database e tabela caso nao exista
def create_tables(dbname_sqlite3):
    """
    script sql de criacao da tabela
    pode ser adicionado a criacao de mais de uma tabela
    separando os scripts por virgulas
    """
    sql_statements = [
        """
        CREATE TABLE "infoDatabaseAzureSql" (
            "ServerName" TEXT NOT NULL,
            "Database" TEXT NOT NULL,
            "Edition" TEXT NOT NULL,
            "ServiceObject"     TEXT NOT NULL,
            "CapacityDtu" INTEGER,
            "MaxStorageinGB" NUMERIC NOT NULL,
            "AllocatedSpaceinGB" NUMERIC NOT NULL,
            "UsedSpaceinGB" NUMERIC NOT NULL,
            "RemainingSpaceinGB" NUMERIC NOT NULL,
            "UltimaVerificacao" TIMESTAMP NOT NULL
        )
        """
    ]

    # variaveis da conexão ao database
    path_dir_db = os.path.join(dirapp, 'db')
    path_full_dbname_sqlite3 = os.path.join(path_dir_db, dbname_sqlite3)

    # cria o diretorio caso nao exista
    if not os.path.exists(path_dir_db):
        os.makedirs(path_dir_db)
    else:
        pass


    try:
        with sqlite3.connect(path_full_dbname_sqlite3) as conn:
            cursor = conn.cursor()
            for statement in sql_statements:
                cursor.execute(statement)

            conn.commit()
    except sqlite3.Error as e:
        datahora = obterDataHora()
        msgException = "Error: {0}".format(e)
        msgLog = 'Criar tabela SQlite3 [infoDatabaseAzureSql] [Erro]: {0}\n{1}'.format(datahora, msgException)
        print(GravaLog(msgLog, 'a'))
    finally:
        msgLog = 'Criado tabela [infoDatabaseAzureSql] no database [{0}]'.format(dbname_sqlite3)
        print(GravaLog(msgLog, 'a'))


## gera comandos de inserts conforme valores da lista passada
def gravaDadosSqlite(v_ListInfoDbs):
    dbname_sqlite3 = "database_bi.db"
    path_dir_db = os.path.join(dirapp, 'db')
    path_full_dbname_sqlite3 = os.path.join(path_dir_db, dbname_sqlite3)
    RowCount = 0

    ## verifica se banco de dados existe
    # caso não exista realizada a chamada da funcao de criacao
    if not os.path.exists(path_dir_db):
        create_tables(dbname_sqlite3)
    else:
        pass


    try:
        with sqlite3.connect(path_full_dbname_sqlite3) as conn:

            cur = conn.cursor()

            ## sql statement DELETE
            for i in range(len(v_ListInfoDbs)):
                v_namedb = str(v_ListInfoDbs[i][1])

                ## sql statement DELETE
                sqlcmdDELETE = "DELETE FROM infoDatabaseAzureSql WHERE Database = '{}';".format(v_namedb)
                cur.execute(sqlcmdDELETE)

            conn.commit()
            RowCountDelete = conn.total_changes

            ## sql statement INSERT
            sqlcmdINSERT = '''
            INSERT INTO infoDatabaseAzureSql
                (
                    ServerName, Database, 
                    Edition, ServiceObject, 
                    CapacityDtu, MaxStorageinGB, 
                    AllocatedSpaceinGB, UsedSpaceinGB, 
                    RemainingSpaceinGB, UltimaVerificacao)
            VALUES
                (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now','localtime'));
            '''
            cur.executemany(sqlcmdINSERT, v_ListInfoDbs)
            RowCountInsert = conn.total_changes
            conn.commit()

    except sqlite3.Error as e:
        datahora = obterDataHora()
        msgException = "Error: {0}".format(e)
        msgLog = 'Fim Insert tabela SQlite3 [infoDatabaseAzureSqlApi] [Erro]: {0}\n{1}'.format(datahora, msgException)
        print(GravaLog(msgLog, 'a'))

    finally:
        RowCount = RowCountInsert - RowCountDelete
        msgLog = 'Quantidade de Registros Inseridos na tabela [infoDatabaseAzureSqlApi]: {0} registro(s)'.format(RowCount)
        print(GravaLog(msgLog, 'a'))


def exibeDadosSqlite():
    #dbname_sqlite3 = "database_bi.db"
    dbname_sqlite3 = os.getenv("DATABASE_TARGET_SQLITE")
    path_dir_db = os.path.join(dirapp, 'db')
    path_full_dbname_sqlite3 = os.path.join(path_dir_db, dbname_sqlite3)

    try:
        with sqlite3.connect(path_full_dbname_sqlite3) as conn:

            sqlcmd = """
            SELECT
                ServerName,
                Database,
                Edition,
                ServiceObject,
                CapacityDtu,
                MaxStorageinGB,
                AllocatedSpaceinGB,
                UsedSpaceinGB,
                RemainingSpaceinGB,
                UltimaVerificacao
            FROM infoDatabaseAzureSql
            --HERE Database != 'master';
            """

            df = pd.read_sql(sqlcmd, conn)
            v_out_table = tabulate(df, headers='keys', tablefmt='psql', showindex=False)
            print(GravaLog(v_out_table, 'a'))

    except sqlite3.Error as e:
        datahora = obterDataHora()
        msgException = "Error: {0}".format(e)
        msgLog = 'Fim Select tabela SQlite3 [infoDatabaseAzureSql] [Erro]: {0}\n{1}'.format(datahora, msgException)
        print(GravaLog(msgLog, 'a'))

    finally:
        msgLog = 'Fim Select tabela SQlite3 [infoDatabaseAzureSql]'
        print(GravaLog(msgLog, 'a'))


## Grava dados no destino
def gravaDadosDestinoAzureSQL(listSource):

    try:
        ## Connection string
        connString = str(strConnectionDatabaseDestino())
        cnxn = po.connect(connString)
        cnxn.autocommit = False

        ## query de busca
        cursor = cnxn.cursor()

        for i in range(len(listSource)):
            v_namedb = str(listSource[i][1])

            ## sql statement DELETE
            sqlcmdDELETE = "DELETE FROM InfoDatabaseAzureSql WHERE [Database] = '{}';".format(v_namedb)
            cursor.execute(sqlcmdDELETE)

        ## sql statement INSERT
        sqlcmd = '''
            INSERT INTO [dbo].[InfoDatabaseAzureSql]
            (
                [ServerName], [Database],
                [Edition], [ServiceObject],
                [CapacityDtu], [MaxStorageinGB],
                [AllocatedSpaceinGB], [UsedSpaceinGB],
                [RemainingSpaceinGB], [UltimaVerificacao]
            )
            VALUES
            (?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE());
        '''

        RowCount = 0
        for params in listSource:
            cursor.execute(sqlcmd, params)
            RowCount = RowCount + cursor.rowcount


    except Exception as e:
        datahora = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        msgException = "Error: {0}".format(e)
        msgLog = 'Fim insercao de dados no destino AzureSQL - [Erro]: {0}\n{1}'.format(datahora, msgException)
        print(GravaLog(msgLog, 'a'))
        cnxn.rollback()

    else:
        cnxn.commit()

    finally:
        ## Close the database connection
        cursor.close()
        del cursor
        cnxn.close()
        datahora = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        msgLog = 'Quantidade de Registros Inseridos no destino AzureSQL: {0}\n'.format(RowCount)
        msgLog = '{0}Fim insercao de dados no destino AzureSQL - {1}'.format(msgLog, datahora)
        print(GravaLog(msgLog, 'a'))


## FUNCAO INICIAL
def main():
    ## log do inicio da aplicacao
    GravaLog('', 'w')
    datahora = obterDataHora()
    msgLog = '\n***** Inicio da aplicacao: {0}'.format(datahora)
    print(GravaLog(msgLog, 'a'))

    #listServers = ['azuredb040', 'sonarqube']
    listServers = ['azuredb040']

    for server in listServers:
        lisdtdbNames = getListNameDatabasesOrigem(server)
        listInfoDatabases = getListInfoDatabasesOrigem(server, lisdtdbNames)
        
        # grava dados no SQLite como destino dos dados
        gravaDadosSqlite(listInfoDatabases)
        
        # grava dados no SQL Server/Azure SQL como destino dos dados
        # para isso tirar o comentario da chamada da funcao
        #gravaDadosDestinoAzureSQL(listInfoDatabases)

    exibeDadosSqlite()

    ## log do final da aplicacao
    datahora = obterDataHora()
    msgLog = '***** Final da aplicacao: {0}'.format(datahora)
    print(GravaLog(msgLog, 'a'))

#### inicio da aplicacao ####
if __name__ == "__main__":
    ## chamada da funcao inicial
    main()