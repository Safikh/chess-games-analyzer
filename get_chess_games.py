import requests
import sys
import json

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
    file = f'data/{username}_games.json'
    with open(file, 'w') as f:
        json.dump(games, f)
    print(f'{len(games)} games found. Stroed in {file}.')
    return games
    

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Usage: get_chess_games.py <username>')
        sys.exit(1)
    username = sys.argv[1]
    months = get_months(username)
    get_games(username, months)