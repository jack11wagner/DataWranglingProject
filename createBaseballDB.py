import os
from dotenv import load_dotenv
import mysql.connector
import webscrapeAwardsAndHonors, webscrapeCareerLeaders, webscrapeTopIndividualPerf


def connect_to_SQL():
    load_dotenv()
    conn = mysql.connector.connect(user=os.getenv("USERNAME"), password=os.getenv("PASSWORD"),
                                   host='127.0.0.1')
    cursor = conn.cursor()
    return cursor, conn


def createBaseballDB(cursor, db_name):
    cursor.execute('DROP DATABASE IF EXISTS ' + db_name)
    cursor.execute('CREATE DATABASE ' + db_name)
    cursor.execute('USE ' + db_name)


def createTable(cursor, fields, table_name):
    pass


def createDBFields():
    name_fields = {
        'PlayerID': 'VARCHAR(100)',
        'PlayerName': 'VARCHAR(100)'
    }
    return name_fields


def createID(playerName):
    pass


def loadBaseballData():
    webscrapeAwardsAndHonors.main()
    webscrapeTopIndividualPerf.main()
    webscrapeCareerLeaders.main()


def main():
    loadBaseballData()
    cursor, conn = connect_to_SQL()
    createBaseballDB(cursor, "baseballStats_db")
    name_fields = createDBFields()
    createTable(cursor, name_fields, 'Player Names')


if __name__ == '__main__':
    main()
