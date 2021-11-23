from urllib.request import urlopen
from bs4 import BeautifulSoup
import pandas as pd
from ratelimit import limits, sleep_and_retry

@sleep_and_retry
@limits(calls= 7, period=60)
def getAllTimeLeadersDictionary(name, playerNameDictionary, batting_file, pitching_file):

    batters_list = []
    pitchers_list = []
    batters_headers = ['Player Name', 'G', 'AB', 'R', 'H', '2B', '3B', 'HR', 'RBI', 'BB', 'IBB', 'SO', 'HBP', 'SH', 'SF', 'XI', 'ROE',
                       'GDP', 'SB', 'CS', 'AVG', 'OBP', 'SLG', 'BFW']

    pitchers_headers = ['Player Name', 'G', 'GS', 'CG', 'SHO', 'GF', 'SV', 'IP', 'H', 'BFP', 'HR', 'R', 'ER', 'BB', 'IB', 'SO', 'SH',
                        'SF', 'WP', 'HBP', 'BK', '2B', '3B', 'GDP', 'ROE', 'W', 'L', 'ERA', 'RS', 'PW']


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
            batter_stats_list.append(player_id)
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
                                    batter_stats_list.append(float(category))
                            batters_list.append(batter_stats_list)
                            batter_stats_df = pd.DataFrame(batters_list, columns=batters_headers)
                            print(batter_stats_df)
                            batter_stats_df.to_csv(batting_file, header=None, index= False)


        else:
            pitcher_stats_list = []
            pitcher_stats_list.append(player_id)
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
                                    pitcher_stats_list.append(0)
                                    continue
                                if category != '' and category != "Total" and category != "Splits" and category != "Years)":
                                    pitcher_stats_list.append(float(category))
                            pitchers_list.append(pitcher_stats_list)
                            pitcher_stats_df = pd.DataFrame(pitchers_list, columns=pitchers_headers)
                            print(pitcher_stats_df)
                            pitcher_stats_df.to_csv(pitching_file,header =None, index= False)

