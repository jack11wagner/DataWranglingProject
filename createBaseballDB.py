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
    cursor.execute('CREATE TABLE {} ({})'.format(table_name, columns))


def load_Player_NamesTable(cursor, player_names_dict, table_name):
    for player in player_names_dict:
        cursor.execute('INSERT INTO {} VALUES ("{}","{}")'.format(table_name, player_names_dict[player], player))


def load_HOF_Table(cursor, hall_of_fame_dict, table_name):
    for player in hall_of_fame_dict:
        cursor.execute('INSERT INTO {} VALUES ("{}", "{}")'.format(table_name, player, hall_of_fame_dict[player]))


def createDBFields():
    name_fields = {
        'PlayerID': 'VARCHAR(100)',
        'PlayerName': 'VARCHAR(100)'
    }
    player_bio_fields = {
        'PlayerID': 'VARCHAR(100)',
        'PlayerName': 'VARCHAR(100)'
    }
    hall_of_fame_fields = {
        'PlayerID': 'VARCHAR(25)',
        'YearOfInduction': 'Year'
    }
    return name_fields, player_bio_fields, hall_of_fame_fields


def loadBaseballData():
    webscrapeAwardsAndHonors.main()
    webscrapeTopIndividualPerf.main()
    webscrapeCareerLeaders.main()


def getDataDirectories(folder_name, directories):
    for root, dirs, files in os.walk(folder_name):
        for file in files:
            if file.endswith('.csv'):
                directories.append(os.path.join(root, file))


def getPlayerNamesDictionary(filename):
    playerNameDictionary = {}
    with open(filename) as file:
        file.readline()
        player_info = csv.reader(file)
        for line in player_info:
            line = [element.strip('"') for element in line]
            playerID, playerName = line[0], line[3] + ' ' + line[1]
            playerNameDictionary[playerName] = playerID
    print(playerNameDictionary)

    return playerNameDictionary


def getHallOfFamePlayers(filename, playerNameDictionary):
    hall_of_fame_dictionary = {}
    with open(filename) as file:
        file.readline()
        hall_of_fame_info = csv.reader(file)
        for line in hall_of_fame_info:
            player_name, year_inducted = line[0], line[1]
            if player_name in playerNameDictionary:
                player_id = playerNameDictionary[player_name]
                hall_of_fame_dictionary[player_id] = year_inducted
    return hall_of_fame_dictionary


def main():
    # loadBaseballData()
    cursor, conn = connect_to_SQL()
    createBaseballDB(cursor, "baseballStats_db")
    name_fields, player_bio_fields, hall_of_fame_fields = createDBFields()

    createTable(cursor, name_fields, 'Player_Names')
    player_names_dict = getPlayerNamesDictionary('playerinformation/playerBios.csv')
    load_Player_NamesTable(cursor, player_names_dict, 'Player_Names')

    createTable(cursor, hall_of_fame_fields, 'Hall_Of_Fame')
    hall_of_fame_dict = getHallOfFamePlayers('awards/The Hall of Fame.csv', player_names_dict)
    load_HOF_Table(cursor, hall_of_fame_dict, 'Hall_Of_Fame')
    conn.commit()


if __name__ == '__main__':
    main()
