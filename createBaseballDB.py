from dotenv import load_dotenv
import mysql.connector, os, csv
import webscrapeHallOfFame, webscrapeCareerLeaders, webscrapeTopIndividualPerf, webscrapePlayerCareerStats
from datetime import datetime
from dateutil import relativedelta


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


def loadPlayerNamesTable(cursor, player_names_dict, table_name):
    for player in player_names_dict:
        cursor.execute('INSERT INTO {} VALUES ("{}","{}")'.format(table_name, player_names_dict[player], player))


def loadHOFTable(cursor, hall_of_fame_dict, table_name):
    for player in hall_of_fame_dict:
        cursor.execute('INSERT INTO {} VALUES ("{}", "{}")'.format(table_name, player, hall_of_fame_dict[player]))


def loadPlayerBiosTable(cursor, bios_dict, table_name):
    for player in bios_dict:
        debut_date, final_game, bats, throws, career_length, excess_months, birth_state, birth_country = bios_dict[
            player]
        cursor.execute(
            'INSERT INTO {} VALUES ("{}", "{}", "{}", "{}","{}", {},{}, "{}", "{}")'.format(table_name, player,
                                                                                            debut_date, final_game,
                                                                                            bats, throws,
                                                                                            career_length,
                                                                                            excess_months, birth_state,
                                                                                            birth_country))


def loadCareerStatsTables(cursor, career_stats, table_name):
    for player in career_stats:
        stats_string = ','.join(career_stats[player])
        sql = f'INSERT INTO {table_name} VALUES ("{player}",' + stats_string + ')'  # string slicing to remove extra comma and append a parenthesis to the sql command
        cursor.execute(sql)


def createDBFields():
    name_fields = {
        'PlayerID': 'VARCHAR(100)',
        'PlayerName': 'VARCHAR(100)'
    }
    player_bio_fields = {
        'PlayerID': 'VARCHAR(100)',
        'debutDate': 'DATE',
        'finalGameDate': 'DATE',
        'bats': 'CHAR(1)',
        'throws': 'CHAR(1)',
        'CareerLength_Years': 'INT',
        'MonthsExtra': 'INT',
        'birthState': 'VARCHAR(100)',
        'birthCountry': 'VARCHAR(100)'

    }
    hall_of_fame_fields = {
        'PlayerID': 'VARCHAR(25)',
        'YearOfInduction': 'Year'
    }

    career_batting_stats = {
        # Career Batting Table Columns
        'PlayerID': 'VARCHAR(100)',
        'Games': 'INT',
        'AtBats': 'INT',
        'Runs': 'INT',
        'Hits': 'INT',
        'Doubles': 'INT',
        'Triples': 'INT',
        'Homeruns': 'INT',
        'RBI': 'INT',
        'Walks': 'INT',
        'IntentionalWalks': 'INT',
        'Strikeouts': 'INT',
        'HitByPitch': 'INT',
        'Sacrifice_Hits': 'INT',
        'Sacrifice_Flies': 'INT',
        'XI': 'INT',
        'ROE': 'INT',
        'GroundedIntoDoublePlays': 'INT',
        'StolenBases': 'INT',
        'CaughtStealing': 'INT',
        'BattingAVG': 'FLOAT',
        'On_BasePercent': 'FLOAT',
        'SluggingPercent': 'FLOAT',
        'BFW': 'FLOAT'
    }

    career_pitching_stats = {
        # Career Pitching Table Columns
        'PlayerID': 'VARCHAR(100)',
        'Games': 'INT',
        'GamesStarted': 'INT',
        'CompleteGames': 'INT',
        'Shutouts': 'INT',
        'GamesFinished': 'INT',
        'Saves': 'INT',
        'InningsPitched': 'INT',
        'Hits': 'INT',
        'BFP': 'INT',
        'Homeruns': 'INT',
        'Runs': 'INT',
        'EarnedRuns': 'INT',
        'Walks': 'INT',
        'IntentionalWalks': 'INT',
        'Strikeouts': 'INT',
        'SacrificeHits': 'INT',
        'SacrificeFlies': 'INT',
        'WildPitches': 'INT',
        'HitByPitch': 'INT',
        'Balks': 'INT',
        'Doubles': 'INT',
        'Triples': 'INT',
        'GroundedIntoDoublePlays': 'INT',
        'ROE': 'INT',
        'Wins': 'INT',
        'Losses': 'INT',
        'ERA': 'FLOAT',
        'RunSupport': 'FLOAT',
        'PW': 'FLOAT'
    }
    single_game_batting = {
        'playerID': 'VARCHAR(100)',
        'CaughtStealing': 'INT',
        'ConsecutiveGameHitStreaks': ''
    }

    return name_fields, player_bio_fields, hall_of_fame_fields, career_batting_stats, career_pitching_stats


def loadBaseballData():
    """
    This function calls all of our webscraping python files which loads various sources from retrosheet into CSVs for database loading
    """
    webscrapeHallOfFame.main()
    webscrapeTopIndividualPerf.main()
    webscrapeCareerLeaders.main()


def webscrapeCareerStatsForEachPlayer(playerNameDictionary):
    """
    Opens batting/pitching files for adding career statistics
    This function calls our separate webscrape file which goes to each individual players url and scrapes either their pitching record, fielding record or both
    Any errors in formatting such as players missing fields or players that did not have certain stats were skipped over in the webscraping process
    We made sure not to abuse the webscraping of retrosheet by making sure only making calls out to the server 10 times per minute.

    I ran this particular function on my raspberry pi and it took roughly 35 hours total to webscrape all of the necessary data

    """
    # TODO Better String Handling for Individual Player Career Stat Lines
    batting_file = open('playerinformation/batting_stats.csv', 'a')
    pitching_file = open('playerinformation/pitching_stats.csv', 'a')
    player_dict_len = len(playerNameDictionary.keys())
    current_index = 0
    player_list = list(playerNameDictionary.keys())
    for player in player_list:
        try:
            webscrapePlayerCareerStats.webscrapeCareerStats(player, playerNameDictionary, batting_file,
                                                            pitching_file)
            current_index += 1
            print('Current Completion Level: {}/{}'.format(current_index, player_dict_len))
        except ValueError:
            # Players with invalid career line formats would be skipped, such as not having enough columns for data
            continue


def getDataDirectories(folder_name):
    directories = []
    for root, dirs, files in os.walk(folder_name):
        for file in files:
            if file.endswith('.csv'):
                directories.append(os.path.join(root, file))
    return directories


def convertDate(date):
    format_string = "%m/%d/%Y"

    try:
        d = datetime.strptime(date, format_string)
    except ValueError:
        # if there is no date we return 0000-01-01 to denote no date
        return '0000-01-01', 0

    return d, d.year


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


def getPlayerBiosDictionary(filename):
    playerBioDictionary = {}
    with open(filename) as file:
        headers = [header.strip() for header in file.readline().split(',')]
        bats_index = headers.index('BATS')
        throws_index = headers.index('THROWS')
        birth_state_index = headers.index('BIRTH STATE')
        birth_country_index = headers.index('BIRTH COUNTRY')

        debut_date_index = headers.index('PLAY DEBUT')
        final_game_index = headers.index('PLAY LASTGAME')
        player_info = csv.reader(file)
        for line in player_info:
            line = [element.strip('"') for element in line]
            playerID, debut_date, final_game, bats, throws = line[0], line[debut_date_index], line[final_game_index], \
                                                             line[bats_index], line[throws_index]

            birth_state, birth_country = line[birth_state_index], line[birth_country_index]

            """New Column for Career Length in Years"""
            debut_date, debut_date_year = convertDate(debut_date)
            final_game, final_game_year = convertDate(final_game)

            career_length_in_years = final_game_year - debut_date_year
            if debut_date != '0000-01-01' and final_game != '0000-01-01':
                date_diff = relativedelta.relativedelta(final_game, debut_date)
                years, months, days = date_diff.years, date_diff.months, date_diff.days
                # if months is greater than or equal to one we want to include this as a year of playing since they at least started the season
                if months >= 1:
                    career_length_in_years += 1
                output_format_date = "%Y-%m-%d"
                debut_date_str, final_game_str = datetime.strftime(debut_date, output_format_date), datetime.strftime(
                    final_game, output_format_date)

            playerBioDictionary[playerID] = [debut_date_str, final_game_str, bats, throws, career_length_in_years,
                                             months, birth_state, birth_country]
    return playerBioDictionary


def getHallOfFamePlayersDictionary(filename, playerNameDictionary, ):
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


def loadAllTimeLeaders(filedirectories, playerDictionary, player_position, cursor):
    for filename in filedirectories:
        with open(filename) as file:
            headers = file.readline()
            headers = headers.replace("\n", "")
            categories = headers.split(",")
            categories[0] = categories[0].replace(" ", "")
            player_name_header = categories[0]
            category = categories[1]
            if category == "G":
                category_list = categories[1]
            else:
                category_list = categories[1:]

            fields = "playerID VARCHAR(255), " + player_name_header + " VARCHAR(255)"
            for value in category_list:
                value = value.replace("/", "Per")
                value = value.replace("%", "Percentage")
                fields = fields + ", " + value + " FLOAT"
            table_name = player_position + category
            cursor.execute('DROP TABLE IF EXISTS {}'.format(table_name))
            cursor.execute('CREATE TABLE {} ({})'.format(table_name, fields))
            all_time_stats = csv.reader(file)
            for line in all_time_stats:
                name = line[0]
                if name in playerDictionary:
                    value_command = 'INSERT INTO {} VALUES ("{}", "{}"'.format(table_name, playerDictionary[name], name)
                for values in line[1:]:
                    if category == "G":
                        value_command += ', "{}"'.format(values)
                        break
                    value_command += ', "{}"'.format(values)
                value_command += ')'
                cursor.execute(value_command)


def getCareerStatsForPlayersDictionary(filename):
    career_stats_dict = {}
    with open(filename) as file:
        file.readline()
        career_stats = csv.reader(file)
        for line in career_stats:
            career_stats_dict[line[0]] = line[1:]
    return career_stats_dict


def addColumns(cursor, column, datatype, tbl_name, player_bio_dict, career_stats_dict):
    cursor.execute(f"ALTER TABLE {tbl_name} ADD {column} {datatype}")
    cursor.execute(f"ALTER TABLE {tbl_name} ADD careerLength INT")

    for player in player_bio_dict:
        try:
            games = career_stats_dict[player][0]
            career_length = player_bio_dict[player][4]
            if float(career_length) == 0:
                cursor.execute(f"UPDATE {tbl_name} SET {column} = 0 WHERE playerID ='{player}'")
                cursor.execute(f"UPDATE {tbl_name} SET careerLength = 0 WHERE playerID ='{player}'")
                continue
            games_per_year = float(games) / float(career_length)
            cursor.execute(f"UPDATE {tbl_name} SET {column} = {round(games_per_year, 2)} WHERE PlayerID ='{player}'")
            cursor.execute(f"UPDATE {tbl_name} SET careerLength = {career_length} WHERE PlayerID ='{player}'")
            # UPDATE table_name SET column1 = value1, column2 = value2, ...WHERE condition;
        except KeyError:
            continue


def main():
    # loadBaseballData()  # function calls webscraping py files, will take a little while to complete

    cursor, conn = connect_to_SQL()
    createBaseballDB(cursor, "baseballStats_db")
    name_fields, player_bio_fields, hall_of_fame_fields, \
    career_batting_stats_fields, career_pitching_stats_fields = createDBFields()

    # Player Names Table
    createTable(cursor, name_fields, 'PlayerNames')
    player_names_dict = getPlayerNamesDictionary('playerinformation/playerBios.csv')
    loadPlayerNamesTable(cursor, player_names_dict, 'PlayerNames')
    print("PlayerNames Table Loaded...")

    # Player Bios Table
    createTable(cursor, player_bio_fields, 'PlayerBios')
    player_bio_dict = getPlayerBiosDictionary('playerinformation/playerBios.csv')
    loadPlayerBiosTable(cursor, player_bio_dict, 'PlayerBios')
    print("PlayerBios Table Loaded...")

    # Hall Of Fame Table
    createTable(cursor, hall_of_fame_fields, 'HallOfFame')
    hall_of_fame_dict = getHallOfFamePlayersDictionary('awards/The Hall of Fame.csv', player_names_dict)
    loadHOFTable(cursor, hall_of_fame_dict, 'HallOfFame')
    print("HallOfFame Table Loaded...")

    # All Time Batting Leaders Table
    all_time_batting_dirs = sorted(getDataDirectories('battingstats/careerleaders/'))
    batting_string = "Batting"
    loadAllTimeLeaders(all_time_batting_dirs, player_names_dict, batting_string, cursor)
    print("AllTimeBattingLeaders Tables Loaded...")

    # All Time Pitching Leaders Table
    all_time_pitching_dirs = sorted(getDataDirectories('pitchingstats/careerleaders/'))
    pitching_string = "Pitching"
    loadAllTimeLeaders(all_time_pitching_dirs, player_names_dict, pitching_string, cursor)
    print("AllTimePitchingLeaders Table Loaded...")

    # Career Batting Stats for All Players Table
    # webscrapeCareerStatsForEachPlayer(player_names_dict)
    createTable(cursor, career_batting_stats_fields, 'CareerBattingStats')
    career_batting_stats = getCareerStatsForPlayersDictionary('playerinformation/batting_stats.csv')
    loadCareerStatsTables(cursor, career_batting_stats, 'CareerBattingStats')
    print("CareerBattingStats Table Loaded...")

    # Career Pitching Stats for All Players Table
    createTable(cursor, career_pitching_stats_fields, 'CareerPitchingStats')
    career_pitching_stats = getCareerStatsForPlayersDictionary('playerinformation/pitching_stats.csv')
    loadCareerStatsTables(cursor, career_pitching_stats, 'CareerPitchingStats')
    print("CareerPitchingStats Table Loaded...")

    addColumns(cursor, 'AVGGamesPerYear', 'FLOAT', 'CareerBattingStats', player_bio_dict, career_batting_stats)
    addColumns(cursor, 'AVGGamesPerYear', 'FLOAT', 'CareerPitchingStats', player_bio_dict, career_pitching_stats)
    conn.commit()


if __name__ == '__main__':
    main()
