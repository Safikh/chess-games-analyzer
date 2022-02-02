import requests
import sys
import json
import os
import pandas as pd
import numpy as np
import re

def make_dir():
    '''
    Creates a directory for storing the output data.
    '''
    if not os.path.exists('data'):
        os.mkdir('data')


def get_months(username: str) -> list:
    '''
    Returns a list of URLs for the months of games for a user and stores them in a txt file.
    Input: username
    Output: list of URLs
    '''
    months = []
    url = f'https://api.chess.com/pub/player/{username}/games/archives'
    response = requests.get(url)
    data = response.json()
    for month in data['archives']:
        months.append(month)
    make_dir()
    with open(f'data/{username}_months.txt', 'w') as f:
        for month in months:
            f.write(month + '\n')
    return months


def get_games(username: str, months: list) -> list:
    '''
    Returns all the games for a user and stores them in a json file.
    Input: username, list of URLs
    Output: list of games
    '''
    games = []
    for month in months:
        url = month
        response = requests.get(url)
        data = response.json()
        for game in data['games']:
            games.append(game)
    make_dir()
    file = f'data/{username}_games.json'
    with open(file, 'w') as f:
        json.dump(games, f)
    print(f'{len(games)} games found.')
    return games


def get_accuracies_(element):
    '''
    Internal function for getting the accuracies of users
    '''
    if pd.isna(element):
        return np.nan, np.nan
    else:
        return element['white'], element['black']


def create_csv(username: str, download_games: bool = False) -> None:
    '''
    Creates a csv file for a user.
    Input: username, download_games (optional)
    Output: None
    '''
    if download_games:
        get_games(username, get_months(username))
    file = f'data/{username}_games.json'
    with open(file, 'r') as f:
        games = json.load(f)
    
    df = pd.DataFrame(games)
    cols_to_extract = ['Event', 'Site', 'Date', 'Round', 'White', 'Black',
                        'Result', 'ECO', 'ECOUrl', 'WhiteElo', 'BlackElo',
                        'Termination', 'StartTime', 'EndDate', 'EndTime']
    
    # Parse the pgn column
    for col in cols_to_extract:
        df[col] = df['pgn'].apply(lambda x: re.findall(f'{col} "(.*?)"', x)[0]) 
    
    df['moves'] = df['pgn'].apply(lambda x: x.split('\n')[-2])
    df.drop(columns=['pgn'], inplace=True)

    df['WhiteAccuracy'], df['BlackAccuracy'] = \
        zip(*df['accuracies'].apply(get_accuracies_))
    df.drop(columns=['accuracies'], inplace=True)

    df.drop(columns=['end_time', 'start_time', 'white', 'black',
                    'tcn', 'uuid']
            , inplace=True)
    
    # Extract the moves and timestamps
    df['white_moves'] = df.moves.apply(lambda x: re.findall(r'[\d]+[\.] ([a-zA-z0-9\+\=\-]{2,}) {', x))
    df['black_moves'] = df.moves.apply(lambda x: re.findall(r'[\d]+[\.]{3} ([a-zA-z0-9\+\=\-]{2,}) {', x))
    df['white_timestamps'] = df.moves.apply(lambda x: re.findall(r'[\d]+[\.] [a-zA-z0-9\+\=\-]{2,} \{\[%clk ([0-9\:\.]+)', x))
    df['black_timestamps'] = df.moves.apply(lambda x: re.findall(r'[\d]+[\.]{3} [a-zA-z0-9\+\=\-]{2,} \{\[%clk ([0-9\:\.]+)', x))

    # Game id as the primary key
    df['game_id'] = df.index.values + 1


    df.to_csv(f'data/{username}_games.csv', index=False)
    print(f'Games for {username} saved to data/{username}_games.csv')


    

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Usage: get_chess_games.py <username>')
        sys.exit(1)
    username = sys.argv[1]
    create_csv(username, True)