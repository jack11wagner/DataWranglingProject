import os
from dotenv import load_dotenv
import mysql.connector


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


def createDBFields():
    pass


def main():
    cursor, conn = connect_to_SQL()
    createBaseballDB(cursor, "baseballStats_db")


if __name__ == '__main__':
    main()
