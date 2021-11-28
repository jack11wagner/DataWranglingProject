# webscrape file
from urllib.request import urlopen
from bs4 import BeautifulSoup
import pandas as pd

major_league_awards_and_honors = 'https://www.retrosheet.org/boxesetc/MISC/XOH.htm#A1'


def getAllCategories(url):
    html = urlopen(url)
    soup = BeautifulSoup(html, features='html.parser')
    text = [words.getText() for words in soup.find_all('h3')]
    category_list = []
    for category in text:
        category = category.split('\n')[0]
        category_list.append(category)
    return category_list


def getStreakStatsFromScrape(url, category_to_scrape):
    html = urlopen(url)
    soup = BeautifulSoup(html, features='html.parser')
    text = [words.getText() for words in soup.find_all('pre')]
    text = text[1:65]
    text.remove(text[30])
    text.remove(text[30])
    text.remove(text[30])
    text.remove(text[30])
    category_lookup = {}
    for i in range(0, 29):
        category_lookup[text[i]] = text[i + 30]
    headers = ['Year', 'Player Name']
    stats = category_lookup[category_to_scrape][:-2]

    stats_list = []

    stat_list = stats.split('\n')
    for value in stat_list:
        first_stat = value[:5].strip()
        second_stat = value[5:].strip()
        stats_list.append([first_stat, second_stat])

    stats_df = pd.DataFrame(stats_list, columns=headers)
    stats_df = stats_df[['Player Name', 'Year']]
    stats_df.to_csv('awards/{}.csv'.format(category_to_scrape), index=False)


def main():
    category_list = getAllCategories(major_league_awards_and_honors)
    getStreakStatsFromScrape(major_league_awards_and_honors, category_list[0])
    print(category_list)
    print('Awards & Honors Data Loaded...')


if __name__ == "__main__":
    main()
