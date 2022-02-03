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


def create_csv(username: str, download_games: bool = False) -> str:
    '''
    Creates a csv file for a user.
    Input: username, download_games (optional)
    Output: filepath to csv file
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

    return f'data/{username}_games.csv'


def transform_dataset(filepath:str, username: str) -> str:
    '''
    Transforms the dataset from black vs white to player v opponent
    Input: 
        filepath: path to the csv file
        username: username of the player
    Output:
        filepath: path to the transformed csv file
    '''

    df = pd.read_csv(filepath)
    cols = df.columns

    
    df_white = df[df['White'] == username].copy()
    df_black = df[df['Black'] == username].copy()


    df_white.loc[: , 'Player'] = df_white['White']
    df_black.loc[: , 'Player'] = df_black['Black']
    df_white.loc[: , 'Opponent'] = df_white['Black']
    df_black.loc[: , 'Opponent'] = df_black['White']
    df_white.loc[: , 'Player_color'] = 'white'
    df_black.loc[: , 'Player_color'] = 'black'
    df_white.loc[: , 'Opponent_color'] = 'black'
    df_black.loc[: , 'Opponent_color'] = 'white'
    df_white.loc[: , 'Result'] = df_white['Result'].map({'1-0': 1, '0-1': -1, '1/2-1/2': 0})
    df_black.loc[: , 'Result'] = df_black['Result'].map({'1-0': -1, '0-1': 1, '1/2-1/2': 0})
    df_white.loc[: , 'Player_elo'] = df_white['WhiteElo']
    df_black.loc[: , 'Player_elo'] = df_black['BlackElo']
    df_white.loc[: , 'Opponent_elo'] = df_white['BlackElo']
    df_black.loc[: , 'Opponent_elo'] = df_black['WhiteElo']
    df_white.loc[: , 'Player_accuracy'] = df_white['WhiteAccuracy']
    df_black.loc[: , 'Player_accuracy'] = df_black['BlackAccuracy']
    df_white.loc[: , 'Opponent_accuracy'] = df_white['BlackAccuracy']
    df_black.loc[: , 'Opponent_accuracy'] = df_black['WhiteAccuracy']
    df_white.loc[: , 'Player_moves'] = df_white['moves']
    df_black.loc[: , 'Player_moves'] = df_black['moves']
    df_white.loc[: , 'Opponent_moves'] = df_white['moves']
    df_black.loc[: , 'Opponent_moves'] = df_black['moves']
    df_white.loc[: , 'Player_timestamps'] = df_white['white_timestamps']
    df_black.loc[: , 'Player_timestamps'] = df_black['black_timestamps']
    df_white.loc[: , 'Opponent_timestamps'] = df_white['black_timestamps']
    df_black.loc[: , 'Opponent_timestamps'] = df_black['white_timestamps']


    cols_to_be_dropped = [col for col in cols if col.lower().startswith(('white', 'black'))]

    
    df_new = pd.concat([df_white, df_black])
    df_new = df_new.sort_values(by=['game_id'], ascending=True).reset_index(drop=True)
    df_new.drop(columns=cols_to_be_dropped, inplace=True)

    df_new.to_csv(f'data/{username}_data.csv', index=False)
    print(f'Data for {username} saved to data/{username}_data.csv')

    return f'data/{username}_data.csv'
    

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Usage: get_chess_games.py <username>')
        sys.exit(1)
    username = sys.argv[1]
    path = create_csv(username, True)
    transform_dataset(path, username)