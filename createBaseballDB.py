from dotenv import load_dotenv
import mysql.connector, os, csv
import webscrapeHallOfFame, webscrapeCareerLeaders, webscrapeTopIndividualPerf
from datetime import datetime
from urllib.request import urlopen
from bs4 import BeautifulSoup
import pandas as pd


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


def loadPlayerBiosTable(cursor, bios_dict, table_name):
    for player in bios_dict:
        playerName, debut_date, final_game, bats, throws = bios_dict[player]
        cursor.execute(
            'INSERT INTO {} VALUES ("{}", "{}", "{}", "{}", "{}","{}")'.format(table_name, player, playerName,
                                                                               debut_date, final_game, bats, throws))


def loadAllTimeBattingLeaders(cursor, all_time_batting, table_name):
    for player in all_time_batting:
        cursor.execute(
            'INSERT INTO {} VALUES ("{}", {}, {}, {}, {},{}, {}, {}, {}, {}, {},{}, {}, {}, {}, {}, {},{}, {}, {}, {}, {}, {})'.format(
                table_name, player, *all_time_batting[player]))


def loadAllTimePitchingLeaders(cursor, all_time_pitching, table_name):
    for player in all_time_pitching:
        cursor.execute(
            'INSERT INTO {} VALUES ("{}", {}, {}, {}, {},{}, {}, {}, {}, {}, {},{}, {}, {}, {}, {}, {},{}, {}, {}, {}, {})'.format(
                table_name, player, *all_time_pitching[player]))


def createDBFields():
    name_fields = {
        'PlayerID': 'VARCHAR(100)',
        'PlayerName': 'VARCHAR(100)'
    }
    player_bio_fields = {
        'PlayerID': 'VARCHAR(100)',
        'PlayerName': 'VARCHAR(100)',
        'debutDate': 'DATE',
        'finalGameDate': 'DATE',
        'bats': 'CHAR(1)',
        'throws': 'CHAR(1)'

    }
    hall_of_fame_fields = {
        'PlayerID': 'VARCHAR(25)',
        'YearOfInduction': 'Year'
    }
    all_time_batting = {
        'PlayerID': 'VARCHAR(25)',
        'At_Bats': 'FLOAT',
        'BFW': 'FLOAT',
        'Batting_Avg': 'FLOAT',
        'CaughtStealing': 'FLOAT',
        'Doubles': 'FLOAT',
        'Games': 'FLOAT',
        'Grounded_Into_Double_Plays': 'FLOAT',
        'HitByPitch': 'INT',
        'Hits': 'Float',
        'Homeruns': 'INT',
        'Intentional_Walks': 'INT',
        'On_Base_Percentage': 'FLOAT',
        'Plate_Apps': 'INT',
        'RBIs': 'INT',
        'Runs': 'INT',
        'SacrificeFlies': 'INT',
        'SacrificeHits': 'INT',
        'SluggingPercent': 'FLOAT',
        'StolenBases': ' INT',
        'Strikeouts': 'INT',
        'Triples': 'INT',
        'Walks': 'INT'

    }
    all_time_pitching = {
        'PlayerID': 'VARCHAR(25)',
        'Balks': 'FLOAT',
        'Complete_Games': 'FLOAT',
        'ERA': 'FLOAT',
        'Earned_Runs': 'INT',
        'GamesFinished': 'INT',
        'GamesPitched': 'INT',
        'GamesStarted': 'INT',
        'HitByPitch': 'INT',
        'Hits': 'INT',
        'Homeruns': 'INT',
        'InningsPitched': 'INT',
        'IntentionalWalks': 'FLOAT',
        'Losses': 'INT',
        'PW': 'INT',
        'Runs': 'INT',
        'Saves': 'INT',
        'Shutouts': 'INT',
        'Strikeouts': 'INT',
        'Walks': ' INT',
        'Wild_Pitches': 'INT',
        'Wins': 'INT'
    }

    career_batting_stats = {
        'PlayerID': 'VARCHAR(100)',
        'Games': 'INT',
        'At_Bats': 'INT',
        'Runs': 'INT',
        'Hits':'INT',
        'Doubles': 'INT',
        'Triples': 'INT',
        'Homeruns': 'INT',
        'Walks': 'INT',
        'Intentional_Walks': 'INT',
        'Strikeouts': 'INT',
        'HitByPitch': 'INT',


    }

    return name_fields, player_bio_fields, hall_of_fame_fields, all_time_batting, all_time_pitching


def loadBaseballData():
    webscrapeHallOfFame.main()
    webscrapeTopIndividualPerf.main()
    webscrapeCareerLeaders.main()


def getDataDirectories(folder_name):
    directories = []
    for root, dirs, files in os.walk(folder_name):
        for file in files:
            if file.endswith('.csv'):
                directories.append(os.path.join(root, file))
    return directories


def getPlayerNamesDictionary(filename):
    playerNameDictionary = {}
    with open(filename) as file:
        file.readline()
        player_info = csv.reader(file)
        for line in player_info:
            line = [element.strip('"') for element in line]
            playerID, playerName = line[0], line[3] + ' ' + line[1]
            playerNameDictionary[playerName] = playerID

    return playerNameDictionary


def convertDate(date):
    format_string = "%m/%d/%Y"
    try:
        d = datetime.strptime(date, format_string)
    except (ValueError):
        # if there is an error in the date format we simply return 0000-01-01 as the date to denote an invalid date
        return '0000-01-01'
    output_date = "%Y-%m-%d"
    return d.strftime(output_date)


def getPlayerBioDictionary(filename):
    playerBioDictionary = {}
    with open(filename) as file:
        headers = [header.strip() for header in file.readline().split(',')]
        bats_index = headers.index('BATS')
        throws_index = headers.index('THROWS')
        debut_date_index = headers.index('PLAY DEBUT')
        final_game_index = headers.index('PLAY LASTGAME')
        player_info = csv.reader(file)
        for line in player_info:
            line = [element.strip('"') for element in line]
            playerID, playerName, debut_date, final_game, bats, throws = line[0], line[3] + ' ' + line[1], line[
                debut_date_index], \
                                                                         line[final_game_index], line[bats_index], line[
                                                                             throws_index],
            debut_date, final_game = convertDate(debut_date), convertDate(final_game)
            playerBioDictionary[playerID] = [playerName, debut_date, final_game, bats, throws]
    # print(playerBioDictionary)
    return playerBioDictionary


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


def getAllTimeLeadersDictionary(playerNameDictionary):
    batters_list = []
    pitchers_list = []
    batters_headers = ['Player Name', 'G', 'AB', 'R', 'H', '2B', '3B', 'HR', 'RBI', 'BB', 'IBB', 'SO', 'HBP', 'SH', 'SF', 'XI', 'ROE',
                       'GDP', 'SB', 'CS', 'AVG', 'OBP', 'SLG', 'BFW']

    pitchers_headers = ['Player Name', 'G', 'GS', 'CG', 'SHO', 'GF', 'SV', 'IP', 'H', 'BFP', 'HR', 'R', 'ER', 'BB', 'IB', 'SO', 'SH',
                        'SF', 'WP', 'HBP', 'BK', '2B', '3B', 'GDP', 'ROE', 'W', 'L', 'ERA', 'RS', 'PW']

    for name in playerNameDictionary:
        player_id = playerNameDictionary[name]
        player_last_name_first_character = player_id[0].upper()

        url = "https://www.retrosheet.org/boxesetc/" + player_last_name_first_character + "/P" + player_id + ".htm"
        html = urlopen(url)
        soup = BeautifulSoup(html, features='html.parser')
        text = [words.getText() for words in soup.find_all('pre')]
        text.remove(text[-1])
        for line in text:
            title = line.split("\n")[0].strip()
            if title != "Fielding Record":
                continue
            position_text = line.split("\n")[2]
            position = position_text[25:28].strip()

            if position != "P":
                batter_stats_list = []
                batter_stats_list.append(name)
                for line in text:
                    title = line.split("\n")[0].strip()
                    if title == "Batting Record":
                        stats = line.split("\n")
                        for stat in stats:
                            categories = stat.split(" ")
                            if categories[0] == "Total" and categories[1] == '':
                                categories.remove(categories[0])
                                categories.remove(categories[0])
                                categories.remove(categories[0])
                                categories.remove(categories[0])
                                categories.remove(categories[0])
                                categories.remove(categories[0])
                                categories.remove(categories[0])
                                for category in categories:
                                    if category != '' and category != "Total" and category != "Splits" and category != ")":
                                        batter_stats_list.append(category)
                                batters_list.append(batter_stats_list)
                                batter_stats_df = pd.DataFrame(batters_list, columns=batters_headers)
                                print(batter_stats_df)
                                break

            else:
                pitcher_stats_list = []
                pitcher_stats_list.append(name)
                for line in text:
                    title = line.split("\n")[0].strip()
                    if title == "Pitching Record":
                        stats = line.split("\n")
                        for stat in stats:
                            categories = stat.split(" ")
                            if categories[0] == "Total" and categories[1] == '':
                                categories.remove(categories[0])
                                categories.remove(categories[0])
                                categories.remove(categories[0])
                                categories.remove(categories[0])
                                categories.remove(categories[0])
                                categories.remove(categories[0])
                                for category in categories:
                                    if category == "-":
                                        pitcher_stats_list.append("None")
                                        continue
                                    if category != '' and category != "Total" and category != "Splits" and category != "Years)":
                                        pitcher_stats_list.append(category)
                                pitchers_list.append(pitcher_stats_list)
                                pitcher_stats_df = pd.DataFrame(pitchers_list, columns=pitchers_headers)
                                print(pitcher_stats_df)
                                break






    
    """
    for file in filedirectories:
        in_file = open(file)
        in_file.readline()
        all_time_stats = csv.reader(in_file)
        for line in all_time_stats:
            playerId = playerNameDictionary[line[0]]
            all_time_leaders[playerId] = []

    for file in filedirectories:
        list_of_players_in_file = []
        in_file = open(file)
        in_file.readline()
        all_time_stats = csv.reader(in_file)
        for line in all_time_stats:
            playerId = playerNameDictionary[line[0]]
            all_time_leaders[playerId].append(line[1])
            list_of_players_in_file.append(playerId)
        players_not_in_file = [player for player in all_time_leaders.keys() if player not in list_of_players_in_file]
        for player in players_not_in_file:
            all_time_leaders[player].append('NULL')
    return all_time_leaders
    """

def main():
    # loadBaseballData()
    """
    cursor, conn = connect_to_SQL()
    createBaseballDB(cursor, "baseballStats_db")
    name_fields, player_bio_fields, hall_of_fame_fields, all_time_batting, all_time_pitching = createDBFields()
    #
    createTable(cursor, name_fields, 'Player_Names')
    """
    player_names_dict = getPlayerNamesDictionary('playerinformation/playerBios.csv')
    """
    load_Player_NamesTable(cursor, player_names_dict, 'Player_Names')

    createTable(cursor, player_bio_fields, 'Player_Bios')
    player_bio_dict = getPlayerBioDictionary('playerinformation/playerBios.csv')
    loadPlayerBiosTable(cursor, player_bio_dict, 'Player_Bios')

    createTable(cursor, hall_of_fame_fields, 'Hall_Of_Fame')
    hall_of_fame_dict = getHallOfFamePlayers('awards/The Hall of Fame.csv', player_names_dict)
    load_HOF_Table(cursor, hall_of_fame_dict, 'Hall_Of_Fame')

    createTable(cursor, all_time_batting, 'All_Time_Batting')
    all_time_batting_dirs = sorted(getDataDirectories('battingstats/careerleaders/'))
    """
    all_time_batting = getAllTimeLeadersDictionary(player_names_dict)
    """
    loadAllTimeBattingLeaders(cursor, all_time_batting, 'All_Time_Batting')

    all_time_pitching_dirs = sorted(getDataDirectories('pitchingstats/careerleaders/'))
    createTable(cursor, all_time_pitching, 'All_Time_Pitching')
    all_time_pitching = getAllTimeLeadersDictionary(all_time_pitching_dirs, player_names_dict)
    loadAllTimePitchingLeaders(cursor, all_time_pitching, 'All_Time_Pitching')

    conn.commit()
    """

if __name__ == '__main__':
    main()
