import asyncio
import aiohttp
from aiohttp import ClientSession, ClientResponseError
from bs4 import BeautifulSoup
import re
import pathlib
from csv import DictWriter
from typing import IO


headers = {
    'user-agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.84 Safari/537.36",
    'origin': "https://www.worldfootball.net",
    'referer': "https://www.worldfootball.net/",
    'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="99", "Google Chrome";v="99"'
}

league_slugs = [
    ('eng-premier-league', '38'),
    ('bundesliga', '34'),
    ('ita-serie-a', '34'), #changed from 18 to 20 teams in 2004
    ('fra-ligue-1', '38'), # from 1997 to 2002 had 18 teams
    ('esp-primera-division', '38'),
]

csv_headers = [
    'league',
    'season',
    'rank',
    'team',
    'matches',
    'wins',
    'draws',
    'losses',
    'goals',
    'goalDiff',
    'points',
]

def make_urls(leagues: list) -> list:
    """Creates urls for season tables in European top 5 leagues from 1992-2021"""
    urls = []
    for league in leagues:
        for i in range(1992, 2021):
            if i >= 2004 and league[0] == 'ita-serie-a': # serie a had 18 teams until 2004
                url = f"https://www.worldfootball.net/schedule/{league[0]}-{i}-{i+1}-spieltag/38/"
            elif i <= 1994 and league[0] == 'eng-premier-league': # premier league had 22 teams between 1992 and 1994
                url = f"https://www.worldfootball.net/schedule/{league[0]}-{i}-{i+1}-spieltag/42/"
            elif i > 1996 and i < 2002 and league[0] == 'fra-ligue-1': # ligue 1 had 18 teams between 97 and 01
                url = f"https://www.worldfootball.net/schedule/{league[0]}-{i}-{i+1}-spieltag/34/"
            else:
                url = f"https://www.worldfootball.net/schedule/{league[0]}-{i}-{i+1}-spieltag/{league[1]}/"
            urls.append(url)
    return urls

def write_headers_dict(file: IO) -> None:
    """Writes header row for csv that will store webscraping results"""
    with open(file, 'w', encoding='utf-8') as f:
        writer = DictWriter(f, fieldnames=csv_headers)
        writer.writeheader()

async def fetch_html(session: ClientSession, url: str) -> str:
    """Gets html for each season table's url"""
    try:
        async with session.get(url, headers=headers) as resp:
            resp.raise_for_status()
            return await resp.text()
    except ClientResponseError: # some urls have spieltag_2 instead of just spieltag
        url2 = re.sub('spieltag', 'spieltag_2', url)
        async with session.get(url2, headers=headers) as resp:
            resp.raise_for_status()
            return await resp.text()

async def get_table(session: ClientSession, url: str) -> list:
    """Extracts table contents and processes them to be written to the csv file"""
    html = await fetch_html(session, url)
    soup = BeautifulSoup(html, 'html.parser')
    h1 = soup.find('div', id="navi").find('div', class_="breadcrumb").find('h1').text.strip()
    h1_match = re.search(r'\b([\w\só]+)\s(\d{4}\/\d{4})', h1) # league and season info in header
    league_season = [h1_match.group(i) for i in range(1,3)]
    table = soup.find_all('table', class_="standard_tabelle")[1] # second table on page
    rows = table.find_all('tr')[1:]
    season_table = []
    for row in rows:
        tds = row.find_all('td')
        data = [str(td.text.strip())
                for i, td in enumerate(tds)
                if str(td.text.strip()) and i < 10] # filter out image column
        full_row = league_season + data
        row_object = {header: cell for header, cell in zip(csv_headers, full_row)}
        season_table.append(row_object)
    return season_table

def table_dictwriter(table: list, file: IO) -> None:
    """Writes a team table row from a given league season to a csv"""
    with open(file, 'a', encoding='utf-8') as f:
        writer = DictWriter(f, csv_headers)
        for team in table:
            writer.writerow(team)

async def get_tables(urls: list, file: IO) -> None:
    """Main driver, populates task queue and passes coroutines to table csv writer"""
    async with ClientSession() as session:
        tasks = [get_table(session, url) for url in urls]
        for task in asyncio.as_completed(tasks):
            table = await task
            table_dictwriter(table, file)

if __name__ == "__main__":
    import pathlib

    here = pathlib.Path(__file__).parent
    outfile = here.joinpath("top5leagueSeasons.csv")

    urls = make_urls(league_slugs)

    write_headers_dict(outfile)
    asyncio.run(get_tables(urls, outfile))