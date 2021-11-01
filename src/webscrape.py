# webscrape file
from urllib.request import urlopen
from bs4 import BeautifulSoup
import pandas as pd

# 2016-2020 NFL Stats (Past 5 years)

def getStatsFromScrape(year, category_list):
    """

    :param year: which year to search for in Football Reference
    :param category_list:
    :return:
    """
    for category in category_list:
        url = ''
        html = urlopen(url)
        soup = BeautifulSoup(html, features='html.parser')
        headers = [header.getText() for header in soup.findAll('tr', limit=2)[0].findAll('th')]

        if headers[1] != 'Player':
            headers = [header.getText() for header in soup.findAll('tr', limit=2)[1].findAll('th')]
            rows = soup.findAll('tr')[2:]
        else:
            rows = soup.findAll('tr')[1:]
        headers = headers[1:]

        player_stats = [[td.getText() for td in rows[i].findAll('td')]
                        for i in range(len(rows))]
        stats = pd.DataFrame(player_stats, columns=headers)
        # Added Dataframe Column for our reference when looking at csv files
        stats['Year'] = year
        stats['Category'] = category
        stats.to_csv("stats/{}/{}-{}.csv".format(year, category, year), index=False, header=True)


def main():
    # years = [2016, 2017, 2018, 2019, 2020]
    categories = ['passing', 'rushing', 'receiving', 'defense']

    getStatsFromScrape(2016, categories)
    getStatsFromScrape(2017, categories)
    getStatsFromScrape(2018, categories)
    getStatsFromScrape(2019, categories)
    getStatsFromScrape(2020, categories)


if __name__ == "__main__":
    main()
