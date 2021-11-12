# webscrape file
from urllib.request import urlopen
from bs4 import BeautifulSoup
import pandas as pd

major_league_top_individual_performances = 'https://www.retrosheet.org/boxesetc/MISC/XOP.htm'

def getAllCategories(url):

    html = urlopen(url)
    soup = BeautifulSoup(html, features='html.parser')
    text = [words.getText() for words in soup.find_all('pre')]
    text = text[3:]

    category_list = []
    for category in text:
        if category.startswith("\nHit By Pitch:\n"):
            continue
        if category.startswith('\n'):
            category = category.split('\n')[1]
            category_list.append(category)

    return category_list


def getHitStreakStatsFromScrape(url, category_to_scrape):

    html = urlopen(url)
    soup = BeautifulSoup(html, features='html.parser')
    text = [words.getText() for words in soup.find_all('pre')]
    text = text[3:17]
    text.remove(text[1])

    category_lookup = {}
    cat_index = 0
    for category in text:
        if category.startswith('\n'):
            category = category.split('\n')[1]
            category_lookup[category] = cat_index
            cat_index += 1

    raw_headers = text[category_lookup[category_to_scrape]].split('\n')[2]
    headers = [header.strip() for header in raw_headers[12:].split()]
    headers.insert(0, 'Player Name')
    headers = headers[:4]
    stats = text[category_lookup[category_to_scrape]].split('\n')[3:-1]

    def getPlayerName(line):
        return line[:22].strip()

    stats_list = []
    for line in stats:
        player_stats = [getPlayerName(line)]
        cleaned_line = line[22:]
        game_stat = cleaned_line[:5].strip()
        start_game = cleaned_line[5:20].strip()

        improved_start_game = ''
        for character in start_game:
            if character == '(':
                break

            elif character != ' ':
                improved_start_game += character

        end_game = cleaned_line[20:34].strip()
        improved_end_game = ''

        for character in end_game:
            if character == '(':
                break

            elif character != ' ':
                improved_end_game += character

        player_stats.append(game_stat)
        player_stats.append(improved_start_game)
        player_stats.append(improved_end_game)
        stats_list.append(player_stats)

    stats_df = pd.DataFrame(stats_list, columns=headers)
    category_to_scrape = category_to_scrape[:-1]
    stats_df.to_csv('battingstats/singlegameleaders/{}.csv'.format(category_to_scrape), index=False)


def getCycleStatsFromScrape(url, category_to_scrape):

    html = urlopen(url)
    soup = BeautifulSoup(html, features='html.parser')
    text = [words.getText() for words in soup.find_all('pre')]
    text = text[3:17]
    text.remove(text[1])

    category_lookup = {}
    cat_index = 0
    for category in text:
        if category.startswith('\n'):
            category = category.split('\n')[1]
            category_lookup[category] = cat_index
            cat_index += 1

    raw_headers = text[category_lookup[category_to_scrape]].split('\n')[2]
    headers = [header.strip() for header in raw_headers[12:].split()]
    headers.insert(0, 'Player Name')
    headers = headers[:3]
    stats = text[category_lookup[category_to_scrape]].split('\n')[3:-1]

    def getPlayerName(line):
        return line[:20].strip()

    stats_list = []
    for line in stats:
        player_stats = [getPlayerName(line)]
        cleaned_line = line[20:]
        first_stat = cleaned_line[:6][:-1].strip()
        second_stat = cleaned_line[6:22].strip()

        improved_second_stat = ''
        for character in second_stat:
            if character == '(':
                break

            elif character.isdigit() is False and character != '-' and character != ' ':
                break

            elif character != ' ':
                improved_second_stat += character

        player_stats.append(first_stat)
        player_stats.append(improved_second_stat)
        stats_list.append(player_stats)

    stats_df = pd.DataFrame(stats_list, columns=headers)
    category_to_scrape = category_to_scrape[:-1]
    stats_df.to_csv('battingstats/singlegameleaders/{}.csv'.format(category_to_scrape), index=False)


def getBattingStatsFromScrape(url, category_to_scrape):

    html = urlopen(url)
    soup = BeautifulSoup(html, features='html.parser')
    text = [words.getText() for words in soup.find_all('pre')]
    text = text[3:20]
    text.remove(text[1])
    category_lookup = {}
    cat_index = 0
    for category in text:
        if category.startswith('\n'):
            category = category.split('\n')[1]
            category_lookup[category] = cat_index
            cat_index += 1

    raw_headers = text[category_lookup[category_to_scrape]].split('\n')[2]
    headers = [header.strip() for header in raw_headers[12:].split()]
    headers.insert(0, 'Player Name')
    headers = headers[:4]
    stats = text[category_lookup[category_to_scrape]].split('\n')[3:-1]

    def getPlayerName(line):
        return line[:20].strip()

    stats_list = []
    for line in stats:
        player_stats = [getPlayerName(line)]
        cleaned_line = line[20:]
        first_stat = cleaned_line[:7].strip()[:-1].strip()
        second_stat = cleaned_line[7:12].strip()
        third_stat = cleaned_line[12:28].strip()

        improved_third_stat = ''
        for character in third_stat:
            if character == '(':
                break

            elif character.isdigit() is False and character != '-' and character != ' ':
                break

            elif character != ' ':
                improved_third_stat += character

        player_stats.append(first_stat)
        player_stats.append(second_stat)
        player_stats.append(improved_third_stat)
        stats_list.append(player_stats)

    stats_df = pd.DataFrame(stats_list, columns=headers)
    category_to_scrape = category_to_scrape[:-1]
    stats_df.to_csv('battingstats/singlegameleaders/{}.csv'.format(category_to_scrape), index=False)


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

    stats_df.to_csv('pitchingstats/{}.csv'.format(category_to_scrape[:-1]), index=False)


def main():
    category_list = getAllCategories(major_league_top_individual_performances)
    batting_categories = category_list[:15]
    pitching_categories = category_list[15:]
    getHitStreakStatsFromScrape(major_league_top_individual_performances, batting_categories[0])
    getCycleStatsFromScrape(major_league_top_individual_performances, batting_categories[1])
    for i in range (2, len(batting_categories)):
        getBattingStatsFromScrape(major_league_top_individual_performances, batting_categories[i])
    """
    for pitch_category in pitching_categories:
        getPitchingStatsFromScrape(major_league_top_individual_performances, pitch_category)
    """

if __name__ == "__main__":
    main()