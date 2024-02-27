from igdb.wrapper import IGDBWrapper
import requests
import json
import time
import pandas as pd
from datetime import datetime
import re
from tqdm import tqdm
from pathlib import Path

acc_key= '2npvi68ap3vkyrzihmpa71k4aqg4w9'
pass_key = 'gndygyktikfny2aw2md9oy80l9mj4y'
base_url = "https://api.igdb.com/v4/games/"
limit = 500

base_path = Path(__file__).parent
file_path = (base_path / "../data/'IGDB_DATA.csv").resolve()

def get_acc_token():
    """
    We need to call this functions with acc_key and pass_key to get the acc_token and consult the apy
    """
    url = f'https://id.twitch.tv/oauth2/token?client_id={acc_key}&client_secret={pass_key}&grant_type=client_credentials'

    #Get acess token from here.
    response = requests.post(url)

    if response.status_code == 200:
        data = json.loads(response.text)
        acc_token = data.get('access_token')
    else:
        return
    return acc_token

def request_something(last):
    acc_token = get_acc_token()
    headers = {
        'Client-ID': f'{acc_key}',  # Replace with the appropriate content type
        'Authorization': f'Bearer {acc_token}'
    }
    data = 'fields *; limit {0}; offset {1};'.format(limit, last)
    response = requests.post(url=base_url, headers=headers, data=data)
    if response.status_code == 200:
        data = json.loads(response.text)
    else:
        data = {}

def get_games(last, wrapper):
    #Get games info, wait 1 seg to not get api block
    options = 'fields *; limit {0}; offset {1};'.format(limit, last)
    time.sleep(1)
    return wrapper.api_request('games', options)

def remove_non_alphanumeric(input_string):
    # Regex pattern to match only letters and numbers
    regex_pattern = r'[a-zA-Z0-9]'

    # Use re.sub to replace non-alphanumeric characters with an empty string
    result_string = re.sub(f'[^{regex_pattern}\s]', '', input_string)
    result_string = re.sub(r'\s+', '', result_string)

    return result_string.lower()

def convert_unix_timestamp_to_date(timestamp):
    # Convert Unix timestamp to datetime object
    if not timestamp or timestamp < 0:
         return None
    date_object = datetime.utcfromtimestamp(timestamp)

    # Format the datetime object as a string
    formatted_date = date_object.strftime("%Y-%m-%d")

    return formatted_date

def make_df_igdb(end=270000):
    """"
    Consult the API, limit is 500. The total games is around 265k. So each hit returns 500.
    """
    acc_token = get_acc_token()

    wrapper = IGDBWrapper(acc_key, acc_token) 
    
    columns = ['id', 'name_original', 'name_normalized', 'release_date', 'genres', 'game_modes', 'external_games', 'platform', 'url',  'summary',  'similar_games']
    games_df = pd.DataFrame(columns=columns)

    for i in tqdm(range(0, end, limit), desc="Processing"):
        games_data = json.loads(get_games(i, wrapper).decode('utf-8').replace("'", '"'))
        for item in games_data:
            #print(f"Progress:{progress} GameDataProgress:{games_data_progress}")
            id = item.get('id')
            name_original = item.get('name')
            name_normalized = remove_non_alphanumeric(name_original)
            release_date = convert_unix_timestamp_to_date(item.get('first_release_date'))
            game_genres = item.get('genres')
            game_modes = item.get('game_modes')
            external_games = item.get('external_games')
            platform = item.get('platforms')
            url = item.get('url')
            summary = item.get('summary')
            similar_games = item.get('similar_games')

            game_data = {
                 'id': id,
                 'name_original': name_original,
                 'name_normalized': name_normalized,
                 'release_date': release_date,
                 'genres': game_genres,
                 'game_modes': game_modes,
                 'external_genres': external_games,
                 'platform': platform,
                 'url': url,
                 'summary': summary,
                 'similar_games': similar_games
            }
            games_df.loc[len(games_df)] = game_data

    games_df.to_csv(file_path, sep='|', index=False)


if __name__ == "__main__":
	make_df_igdb()