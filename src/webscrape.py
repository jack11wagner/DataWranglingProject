# webscrape file
from urllib.request import urlopen
from bs4 import BeautifulSoup
import pandas as pd

major_league_career_leaders = 'https://www.retrosheet.org/boxesetc/M/XCL_ML.htm'
rushing_url = 'https://www.pro-football-reference.com/years/2020/rushing.htm'
receiving_url = 'https://www.pro-football-reference.com/years/2020/receiving.htm'
defense_url = 'https://www.pro-football-reference.com/years/2020/defense.htm'


def getStatsFromScrape(url):
    html = urlopen(url)
    soup = BeautifulSoup(html, features='html.parser')
    text = [words.getText() for words in soup.find_all('pre')]
    games_leaders = str(text[2])
    stats = games_leaders.split("\n")
    headers = stats[1].split("  ")
    new_headers = []
    for word in headers:
        if word != '':
            word = word.strip()
            new_headers.append(word)

        if word == 'G'

    print(new_headers)
    """
  
    if headers[1] != 'Player':
        headers = [header.getText() for header in soup.findAll('tr', limit=2)[1].findAll('th')]
        rows = soup.findAll('tr')[2:]
    else:
        rows = soup.findAll('tr')[1:]
    headers = headers[1:]
    
    player_stats = [[td.getText() for td in rows[i].findAll('td')]
                    for i in range(len(rows))]
    stats = pd.DataFrame(player_stats, columns=headers)
    return stats
    """

def main():

    batting_leaders = getStatsFromScrape(major_league_career_leaders)

    batting_leaders.to_csv('stats/careerleaders.csv', index=False)


if __name__ == "__main__":
    main()
