from steam import Steam
from decouple import config
import re
from datetime import datetime
from pathlib import Path
import pandas as pd
from tqdm import tqdm
import sys


KEY = '99442A67D45D5A04C1AE4798A071C140'
steam = Steam(KEY)

base_path = Path(__file__).parent
csv_path = str((base_path / "./data/igdb_data.csv").resolve())
steam_search = str((base_path / "./data/steam_search.csv").resolve())
games_source_1 = str((base_path / "./data/games_source_1.csv").resolve())
similar_games = str((base_path / "./data/igdb_similar_games.csv").resolve())
igdb_data = str((base_path / "./data/igdb_data.csv").resolve())


def convert_unix_timestamp_to_date(timestamp):
    # Convert Unix timestamp to datetime object
    if not timestamp or timestamp < 0:
         return None
    date_object = datetime.utcfromtimestamp(timestamp)

    # Format the datetime object as a string
    formatted_date = date_object.strftime("%Y-%m-%d")

    return formatted_date

def remove_non_alphanumeric(input_string):
    # Regex pattern to match only letters and numbers
    regex_pattern = r'[a-zA-Z0-9]'

    # Use re.sub to replace non-alphanumeric characters with an empty string
    result_string = re.sub(f'[^{regex_pattern}\s]', '', input_string)
    result_string = re.sub(r'\s+', '', result_string)

    return result_string.lower()

def find_games_by_term(user_country, term):
    #Search for tem and return all games with that term, with name, price, link and description
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
        df.to_csv(steam_search, mode='a', index=False, header=False, sep="|")
    return df

def parse_games_owned(steam_id):

    games_owned = steam.users.get_owned_games(steam_id)
    #Little 10hours or less
    #Moderate 20 hours or less
    #love 20+hours
    #fanactic 50+hours
    little = 10*60
    moderate = 20*60
    love = 50*60
    played_games = {'little':[], 'moderate':[], 'love':[], 'fanatic':[]}
    for game in games_owned.get('games'):
        
        play_time = game.get('playtime_forever')
        if play_time == 0:
            continue
        
        name = game.get('name')
        name_normalized = remove_non_alphanumeric(name)
        last_played = convert_unix_timestamp_to_date(game.get('rtime_last_played'))
        games_info = {'name': name, 'name_normalized': name_normalized,
                           'play_time': play_time, 'last_played':last_played }
        
        if play_time<= little:
            played_games['little'].append(games_info)
        elif play_time<= moderate:
            played_games['moderate'].append(games_info)
        elif play_time<= love:
            played_games['love'].append(games_info)
        else:
            played_games['fanatic'].append(games_info)
    return played_games


def get_games_per_category(category, played_games):
    l = [item.get('name_normalized') for item in played_games[category]]
    return l

def get_list_games(game_list, df_igdb_data, df_similar_games):
    filtered_df = df_igdb_data[df_igdb_data['name_normalized'].isin(game_list)]
    if len(filtered_df)>0:
        id_games = filtered_df['id'].tolist()
        filtered_similar = df_similar_games[df_similar_games['id'].isin(id_games)]
        if len(filtered_similar)>0:
            my_list = filtered_similar['similar_game_id'].unique()
            return my_list
    return None

def get_games_owned_id(steam_id):
    
    my_games_owned = []
    games_owned = steam.users.get_owned_games(steam_id)
    
    for game in games_owned.get('games'):
        my_games_owned.append(game.get('appid'))
    
    return my_games_owned

def build_lists(category, games_owned, good_games, top_games, my_list):

    my_list = list(set(my_list) - set(games_owned))

    my_category_values = [category] * len(my_list)
    # Set the third column based on conditions
    third_column_values = []
    for id_value in my_list:
        if id_value in good_games:
            third_column_values.append(2)
        elif id_value in top_games:
            third_column_values.append(3)
        else:
            third_column_values.append(1)

    # Create DataFrame
    df = pd.DataFrame({
        'name_normalized': my_list,
        'my_category': my_category_values,
        'game_rating': third_column_values
    })

    return df

def get_user_recommendation_df(steam_id):
    #Load every data from csvs#
    df_games_source_1 = pd.read_csv(games_source_1, sep=',')
    df_similar_games = pd.read_csv(similar_games, sep='|')
    df_igdb_data = pd.read_csv(igdb_data, sep='|')

    #Get good and top games from source_1
    top_games = df_games_source_1[df_games_source_1['rating_top'] == 1]['name'].unique()
    good_games = df_games_source_1[df_games_source_1['rating'] > 3]['name'].unique()

    top_games = [remove_non_alphanumeric(x) for x in top_games]
    good_games = [remove_non_alphanumeric(x) for x in good_games]

    #Get player games and bucket them into fanatic, love, moderate and little
    played_games = parse_games_owned(steam_id)
    search_list_fanatic = get_games_per_category('fanatic', played_games)
    search_list_love = get_games_per_category('love', played_games)
    search_list_moderate = get_games_per_category('moderate', played_games)
    search_list_little = get_games_per_category('little', played_games)
    
    #Gets player owned games.
    games_owned = get_games_owned_id(steam_id)
    
    #Build the df based on the list that player like and game rating
    my_list_fanatic = get_list_games(search_list_fanatic, df_igdb_data, df_similar_games)
    my_list_fanatic = df_igdb_data[df_igdb_data['id'].isin(my_list_fanatic)]['name_normalized'].tolist()

    my_list_love = get_list_games(search_list_love, df_igdb_data, df_similar_games)
    my_list_love = df_igdb_data[df_igdb_data['id'].isin(my_list_love)]['name_normalized'].tolist()
    
    my_list_moderate = get_list_games(search_list_moderate, df_igdb_data, df_similar_games)
    my_list_moderate = df_igdb_data[df_igdb_data['id'].isin(my_list_moderate)]['name_normalized'].tolist()
    
    my_list_little = get_list_games(search_list_little, df_igdb_data, df_similar_games)
    my_list_little = df_igdb_data[df_igdb_data['id'].isin(my_list_little)]['name_normalized'].tolist()

    df_fanatic = build_lists(4, games_owned, good_games, top_games, my_list_fanatic)
    df_love = build_lists(3, games_owned, good_games, top_games, my_list_love)
    df_moderatec = build_lists(2, games_owned, good_games, top_games, my_list_moderate)
    df_little = build_lists(1, games_owned, good_games, top_games, my_list_little)

    #Build a recommendation strength variable, by multiplying rating * player similar games preference
    concatenated_df = pd.concat([df_fanatic, df_love, df_moderatec, df_little], ignore_index=True)
    concatenated_df['recommendation_strength'] = concatenated_df['my_category'] * concatenated_df['game_rating']
    
    joined_df = pd.merge(concatenated_df, df_igdb_data, on='name_normalized', how='inner')
    if len(joined_df)>0:
        joined_df = joined_df[['name_original', 'recommendation_strength']]
        result_df = joined_df.groupby('name_original')['recommendation_strength'].max().reset_index()
        result_df = result_df.sort_values(by='recommendation_strength', ascending=False)
    return result_df

def steam_consume(steam_id):
    user_df = get_user_recommendation_df(steam_id)
    user = steam.users.get_user_details(steam_id)
    user_country = user.get('player').get('loccountrycode')

    games_name = user_df['name_original'].tolist()
    games_reccomendation = pd.DataFrame(columns=['name', 'id', 'price', 'link'])
    for game_name in tqdm(games_name, desc='processing'):
        game_info = find_games_by_term(user_country, game_name)
        games_reccomendation = pd.concat([games_reccomendation, game_info])
    
    games_reccomendation.to_excel(f'reccomendation_player_{steam_id}.xlsx', index=False)


def build_steam_dataset_from_igdb(steam_id):
    
    df_data = pd.read_csv(csv_path, sep="|")
    game_names_igdb = df_data['name_original'].unique()
    i = 0
    for game in tqdm(game_names_igdb, desc='processing'):
        i+=1
        if i < 156286:
            continue
        if str(game)=='nan':
            continue
        t = find_games_by_term(steam_id, game)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        steam_user_id = '76561198156352468'
    else:
        steam_user_id = sys.argv[1]
    steam_consume(steam_user_id)