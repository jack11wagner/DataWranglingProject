from dotenv import load_dotenv
import mysql.connector, os, csv
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
    cursor.execute('DROP TABLE IF EXISTS {}'.format(table_name))
    columns = ','.join(['{} {}'.format(key, fields[key]) for key in fields.keys()])
    cursor.execute('CREATE TABLE {}'.format(columns))


def createDBFields():
    name_fields = {
        'PlayerID': 'VARCHAR(100)',
        'PlayerName': 'VARCHAR(100)'
    }
    return name_fields


def getPlayerNamesDictionary(directories):
    playerNameDictionary = {}
    for file in directories:

        with open(file) as stat_file:
            stat_file.readline()
            stat_file = csv.reader(stat_file)
            for line in stat_file:
                playerName = line[0]
                playerID = createID(playerName, playerNameDictionary)
                playerNameDictionary[playerID] = playerName
    return playerNameDictionary


def createID(playerName, name_dict):
    id_num = 1
    playerName = playerName.lower().split()
    playerID ='' + playerName[0][0] + playerName[-1].title() + str(id_num)
    return playerID


def loadBaseballData():
    webscrapeAwardsAndHonors.main()
    webscrapeTopIndividualPerf.main()
    webscrapeCareerLeaders.main()


def getDataDirectories(folder_name, directories):
    for root, dirs, files in os.walk(folder_name):
        for file in files:
            if file.endswith('.csv'):
                directories.append(os.path.join(root, file))


def main():
    loadBaseballData()
    cursor, conn = connect_to_SQL()
    createBaseballDB(cursor, "baseballStats_db")
    name_fields = createDBFields()
    directories = []
    getDataDirectories('battingstats', directories)
    getDataDirectories('pitchingstats', directories)
    getDataDirectories('awards', directories)

    createTable(cursor, name_fields, 'Player Names')
    getPlayerNamesDictionary(directories)





if __name__ == '__main__':
    main()
