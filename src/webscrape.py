# webscrape file
from urllib.request import urlopen
from bs4 import BeautifulSoup
import pandas as pd

# 2020 NFL Stats ONLY

passing_url = 'https://www.pro-football-reference.com/years/2020/passing.htm'
rushing_url = 'https://www.pro-football-reference.com/years/2020/rushing.htm'
receiving_url = 'https://www.pro-football-reference.com/years/2020/receiving.htm'
defense_url = 'https://www.pro-football-reference.com/years/2020/defense.htm'


def getStatsFromScrape(category):
    url = 'https://www.pro-football-reference.com/years/2020/{}.htm'.format(category)
    html = urlopen(url)
    soup = BeautifulSoup(html, features='html.parser')
    headers = [header.getText() for header in soup.findAll('tr', limit=2)[0].findAll('th')]
    headers = headers[1:]
    rows = soup.findAll('tr')[1:]
    player_stats = [[td.getText() for td in rows[i].findAll('td')]
                    for i in range(len(rows))]
    stats = pd.DataFrame(player_stats, columns=headers)
    return stats


def main():
    # getStatsFromScrape('rushing')

    passing_df, receiving_df = getStatsFromScrape('passing'), getStatsFromScrape('receiving')

    passing_df.to_csv(path='stats/passing2020.csv', index=False)
    receiving_df.to_csv(path='stats/receiving2020.csv', index=False)


if __name__ == "__main__":
    main()
