# webscrape file
from urllib.request import urlopen
from bs4 import BeautifulSoup
import pandas as pd

major_league_career_leaders = 'https://www.retrosheet.org/boxesetc/M/XCL_ML.htm'


def getAllCategories(url):
    html = urlopen(url)
    soup = BeautifulSoup(html, features='html.parser')
    text = [words.getText() for words in soup.find_all('pre')]
    text = text[2:]
    category_list = []
    for category in text:
        category = category.split('\n')[0]
        category_list.append(category)
    return category_list


def getBattingStatsFromScrape(url, category_to_scrape):
    html = urlopen(url)
    soup = BeautifulSoup(html, features='html.parser')
    text = [words.getText() for words in soup.find_all('pre')]
    text = text[2:24]
    category_lookup = {}
    forbidden_per_game_stat_list = ["Games:", "Batting Average:", "On-Base Percentage:", "Slugging Percentage:", "BFW:"]
    cat_index = 0
    for category in text:
        category = category.split('\n')[0]
        category_lookup[category] = cat_index
        cat_index += 1
    raw_headers = text[category_lookup[category_to_scrape]].split('\n')[1]
    headers = [header.strip() for header in raw_headers[12:].split()]
    headers.insert(0, 'Player Name')

    if category_to_scrape not in forbidden_per_game_stat_list:
        headers.insert(3, headers[1] + "/G")
        headers = headers[:4]
    else:
        headers = headers[:3]

    stats = text[category_lookup[category_to_scrape]].split('\n')[2:-1]

    def getPlayerName(line):
        """
        Helper function which uses string slicing to extract out the player name and strip
        any trailing/leading whitespace
        """
        return line[:23].strip()

    # the 23 represents the number of characters of padding between player name and stats for each header
    stats_list = []
    for line in stats:
        player_stats = [getPlayerName(line)]
        cleaned_line = line[23:]
        column_stats = cleaned_line.split()[:2]

        for stat in column_stats:
            player_stats.append(stat)
        if category_to_scrape not in forbidden_per_game_stat_list:
            stat_per_game = float(column_stats[0]) / float(column_stats[1])
            player_stats.append("{:.2f}".format(stat_per_game))
        stats_list.append(player_stats)

    stats_df = pd.DataFrame(stats_list, columns=headers)

    category_to_scrape = category_to_scrape[:-1]
    stats_df.to_csv('battingstats/careerleaders/{}.csv'.format(category_to_scrape), index=False)


def getPitchingStatsFromScrape(url, category_to_scrape):
    html = urlopen(url)
    soup = BeautifulSoup(html, features='html.parser')
    text = [words.getText() for words in soup.find_all('pre')]
    # Only look at Text After Pitching Leaders Header
    text = text[25:]
    category_lookup = {}
    percentage_list = ["Wins:", "Losses:", "Games Started:", "Complete Games:", "Shutouts:", "Games Finished:", "Saves:"]
    forbidden_per_game_stat_list = ["Earned Run Average:", "PW:"]
    cat_index = 0
    for category in text:
        category = category.split('\n')[0]
        category_lookup[category] = cat_index
        cat_index += 1
    raw_headers = text[category_lookup[category_to_scrape]].split('\n')[1]
    headers = [header.strip() for header in raw_headers[12:].split()]
    # Inserting Player Name Header in front of stat headers
    headers.insert(0, 'Player Name')
    if category_to_scrape in percentage_list:
        headers.insert(3, headers[1] + "%")
        headers = headers[:4]

    elif category_to_scrape == "Games Pitched:":
        headers = headers[:2]

    elif category_to_scrape not in forbidden_per_game_stat_list:
        headers.insert(3, headers[1] + "/G")
        headers = headers[:4]

    else:
        headers = headers[:3]
    # [:6] denotes first 6 headers in the retro sheet html
    stats = text[category_lookup[category_to_scrape]].split('\n')[2:-1]

    def getPlayerName(line):
        """
        Helper function which uses string slicing to extract out the player name and strip
        any trailing/leading whitespace
        """
        return line[:23].strip()

    stats_list = []
    games_pitched = False
    for line in stats:
        # print(line)
        player_stats = [getPlayerName(line)]
        cleaned_line = line[23:]
        column_stats = cleaned_line.split()[:2]
        for stat in column_stats:
            player_stats.append(stat)
        if category_to_scrape == "Games Pitched:":
            player_stats = player_stats[:2]
            games_pitched = True
        if category_to_scrape in percentage_list or category_to_scrape not in forbidden_per_game_stat_list and games_pitched == False:
            win_loss_percentage = float(column_stats[0]) / float(column_stats[1])
            player_stats.append("{:.2f}".format(win_loss_percentage))
        stats_list.append(player_stats)
    stats_df = pd.DataFrame(stats_list, columns=headers)

    stats_df.to_csv('pitchingstats/careerleaders/{}.csv'.format(category_to_scrape[:-1]), index=False)


def main():
    category_list = getAllCategories(major_league_career_leaders)
    batting_categories = category_list[:22]
    pitching_categories = category_list[23:]

    for bat_category in batting_categories:
        getBattingStatsFromScrape(major_league_career_leaders, bat_category)

    for pitch_category in pitching_categories:
        getPitchingStatsFromScrape(major_league_career_leaders, pitch_category)

    print('All Time Leaders Data Loaded...')


if __name__ == "__main__":
    main()
