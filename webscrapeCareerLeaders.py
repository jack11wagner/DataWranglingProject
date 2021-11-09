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
    text = text[2:]
    category_lookup = {}
    cat_index = 0
    for category in text:
        category = category.split('\n')[0]
        category_lookup[category] = cat_index
        cat_index += 1
    raw_headers = text[category_lookup[category_to_scrape]].split('\n')[1]
    headers = [header.strip() for header in raw_headers[12:].split()]
    headers.insert(0, 'Player Name')
    headers = headers[:2]
    stats = text[category_lookup[category_to_scrape]].split('\n')[2:-1]

    def getPlayerName(line):
        return line[:23].strip()

    # the 23 represents the number of characters of padding between player name and stats for each header
    stats_list = []
    for line in stats:
        # print(line)
        player_stats = [getPlayerName(line)]
        cleaned_line = line[23:]
        column_stats = cleaned_line.split()[:1]
        for stat in column_stats:
            player_stats.append(stat)
        stats_list.append(player_stats)
    stats_df = pd.DataFrame(stats_list, columns=headers)
    print(stats_df)

    category_to_scrape = category_to_scrape[:-1]
    stats_df.to_csv('battingstats/{}.csv'.format(category_to_scrape), index=False)


def getPitchingStatsFromScrape(url, category_to_scrape):
    html = urlopen(url)
    soup = BeautifulSoup(html, features='html.parser')
    text = [words.getText() for words in soup.find_all('pre')]
    # Only look at Text After Pitching Leaders Header
    text = text[25:]
    category_lookup = {}
    cat_index = 0
    for category in text:
        category = category.split('\n')[0]
        category_lookup[category] = cat_index
        cat_index += 1
    raw_headers = text[category_lookup[category_to_scrape]].split('\n')[1]
    headers = [header.strip() for header in raw_headers[12:].split()]
    # Inserting Player Name Header in front of stat headers
    headers.insert(0, 'Player Name')
    # [:6] denotes first 6 headers in the retro sheet html
    headers = headers[:2]
    stats = text[category_lookup[category_to_scrape]].split('\n')[2:-1]

    def getPlayerName(line):
        """
        Helper function which uses string slicing to extract out the player name and strip
        any trailing/leading whitespace
        """
        return line[:23].strip()

    stats_list = []
    for line in stats:
        # print(line)
        player_stats = [getPlayerName(line)]
        cleaned_line = line[23:]
        column_stats = cleaned_line.split()[:1]
        for stat in column_stats:
            player_stats.append(stat)
        stats_list.append(player_stats)
    stats_df = pd.DataFrame(stats_list, columns=headers)
    print(stats_df)

    category_to_scrape = category_to_scrape[:-1]
    stats_df.to_csv('pitchingstats/{}.csv'.format(category_to_scrape), index=False)

    # print(category_lookup)
    # print(headers)


def main():
    category_list = getAllCategories(major_league_career_leaders)
    batting_categories = category_list[:22]
    pitching_categories = category_list[23:]
    print(category_list)

    for bat_category in batting_categories:
        print(bat_category)
        getBattingStatsFromScrape(major_league_career_leaders, bat_category)
    for pitch_category in pitching_categories:
        print(pitch_category)
        getPitchingStatsFromScrape(major_league_career_leaders, pitch_category)


if __name__ == "__main__":
    main()
