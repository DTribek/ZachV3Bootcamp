from steam import Steam
from datetime import datetime
from pathlib import Path
import pandas as pd
from tqdm import tqdm

KEY = 'Your steam KEY here'
steam = Steam(KEY)

base_path = Path(__file__).parent
csv_path = str((base_path / "../data/igdb_data.csv").resolve())
steam_seach = str((base_path / "../data/steam_search.csv").resolve())


def find_games_by_term(term):
    #Search for tem and return all games with that term, with name, price, link and description
    
    #HARD CODED 'BR' TO AVOID TWO HITS ON API
    user_country = 'BR'
    seached_terms = steam.apps.search_games(term, country=user_country)
    df = pd.DataFrame(columns=['name', 'id', 'price', 'link'])
    
    #Parse information for each game
    for game in seached_terms.get('apps'):
        name = game.get('name')
        id = str(game.get('id')[0])
        price = game.get('price')
        link = game.get('link')
        data = {'name': name, 'id': id, 'price': price, 'link': link}
        df.loc[len(df)] = data
    
    if len(df)>0:
        df.to_csv(steam_seach, mode='a', index=False, header=False, sep="|")
    return df



def build_steam_dataset_from_igdb():
    #Get the games in igdb and consult them in steam#
    df_data = pd.read_csv(csv_path, sep="|")
    game_names_igdb = df_data['name_original'].unique()
    i = 0
    for game in tqdm(game_names_igdb, desc='processing'):
        i+=1
        #Last stopping point
        #if i < 156286:
        #    continue
        if str(game)=='nan':
            continue
        find_games_by_term(game)

if __name__ == "__main__":
    df = pd.read_csv(steam_seach, sep='|')
    print(len(df))
    #build_steam_dataset_from_igdb()