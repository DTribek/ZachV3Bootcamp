# Simple parse of the 'games.json' file.
import os
import json
import pandas as pd
from pathlib import Path
import re
from tqdm import tqdm

base_path = Path(__file__).parent
file_path = (base_path / "../data/games.json").resolve()

#Dataset columns
columns = ['game_id', 'name_original', 'name_normalized', 'release_date', 'estimated_owners', 'required_age', 'price',
           'detailed_description', 'short_description', 'supported_languages', 'full_audio_languages',
           'reviews', 'website', 'support_url', 'support_email', 'support_windows',
           'support_mac', 'support_linux', 'metacritic_score', 'metacritic_url', 'user_score', 'positive',
           'negative', 'score_rank', 'achievements', 'recommendations', 'notes', 'average_playtime_forever',
           'average_playtime_2weeks', 'median_playtime_forever', 'median_playtime_2weeks', 'categories', 'single_player',
            'multiplayer', 'genres', 'developers', 'publishers']


def remove_non_alphanumeric(input_string):
    # Regex pattern to match only letters and numbers
    regex_pattern = r'[a-zA-Z0-9]'

    # Use re.sub to replace non-alphanumeric characters with an empty string
    result_string = re.sub(f'[^{regex_pattern}\s]', '', input_string)
    result_string = re.sub(r'\s+', '', result_string)

    return result_string.lower()

def get_json_data(file_path):
    dataset = {}
    if os.path.exists(file_path):
        with open(file_path, 'r', encoding='utf-8') as fin:
            text = fin.read()
            if len(text) > 0:
                dataset = json.loads(text)
    return dataset

def convert_date_string_to_date(original_string):
    # Original string
    original_string = "Oct 21, 2008"

    # Convert string to date format yyyy/mm/dd
    date_object = pd.to_datetime(original_string, format="%b %d, %Y")

    # Format the date as yyyy/mm/dd
    formatted_date = date_object.strftime("%Y-%m-%d")
    return formatted_date


def parse_json_into_dataframe(json_data):
    # Create an empty DataFrame
    games_df = pd.DataFrame(columns=columns)
    for app in tqdm(json_data, desc="Processing"):

        app_id = app                                                            # AppID, unique identifier for each app (string).
        game = json_data[app]             
        name_original = game.get('name')                                        # Game name (string).
        name_normalized = remove_non_alphanumeric(name_original)                # Name withouts spaces and special character, lower case
        release_date = convert_date_string_to_date(game.get('release_date'))    # Release date (string).
        estimated_owners = game.get('estimated_owners')                         # Estimated owners (string, e.g.: "0 - 20000").
        required_age = game.get('required_age')                                 # Age required to play, 0 if it is for all audiences (int).
        price = game.get('price')                                               # Price in USD, 0.0 if it's free (float).
        detailed_description = game.get('detailed_description')                 # Detailed description of the game (string).
        short_description = game.get('short_description')                       # Brief description of the game, does not contain HTML tags (string).
        supported_languages = game.get('supported_languages')                   # Comma-separated enumeration of supporting languages.
        full_audio_languages = game.get('full_audio_languages')                 # Comma-separated enumeration of languages with audio support.
        reviews = game.get('reviews')                                           # Game Reviews
        website = game.get('website')                                           # Game website (string).
        support_url = game.get('support_url')                                   # Game support URL (string).
        support_email = game.get('support_email')                               # Game support email (string).
        support_windows = game.get('windows')                                   # Does it support Windows? (bool).
        support_mac = game.get('mac')                                           # Does it support Mac? (bool).
        support_linux = game.get('linux')                                       # Does it support Linux? (bool).
        metacritic_score = game.get('metacritic_score')                         # Metacritic score, 0 if it has none (int).
        metacritic_url = game.get('metacritic_url')                             # Metacritic review URL (string).
        user_score = game.get('user_score')                                     # Users score, 0 if it has none (int).
        positive = game.get('positive')                                         # Positive votes (int).
        negative = game.get('negative')                                         # Negative votes (int).
        score_rank = game.get('score_rank')                                     # Score rank of the game based on user reviews (string).
        achievements = game.get('achievements')                                 # Number of achievements, 0 if it has none (int).
        recommendations = game.get('recommendations')                           # User recommendations, 0 if it has none (int).
        notes = game.get('notes')                                               # Extra information about the game content (string).
        average_playtime_forever = game.get('average_playtime_forever')         # Average playtime since March 2009, in minutes (int).
        average_playtime_2weeks = game.get('average_playtime_2weeks')           # Average playtime in the last two weeks, in minutes (int).
        median_playtime_forever = game.get('median_playtime_forever')           # Median playtime since March 2009, in minutes (int).
        median_playtime_2weeks = game.get('median_playtime_2weeks')             # Median playtime in the last two weeks, in minutes (int).
        
        categories = game.get('categories')                                     # Game categories. (Single player and multiplayer will be used only)
        single_player = False
        multiplayer = False
        
        if 'Single-player' in categories:
            single_player = True
        if 'Multi-player' in categories:
            multiplayer = True
        
        genres = game.get('genres')                                             # Game genres.
        genres = [item.lower() for item in genres]
        
        developers = game.get('developers')                                     # Game developers.

        publishers = game.get('publishers')                                     # Game publishers.
        
        game_data = {
            'app_id': app_id,
            'name_original': name_original,
            'name_normalized': name_normalized,
            'release_date': release_date,
            'estimated_owners': estimated_owners,
            'required_age': required_age,
            'price': price,
            'detailed_description': detailed_description,
            'short_description': short_description,
            'supported_languages': supported_languages,
            'full_audio_languages': full_audio_languages,
            'reviews': reviews,
            'website': website,
            'support_url': support_url,
            'support_email': support_email,
            'support_windows': support_windows,
            'support_mac': support_mac,
            'support_linux': support_linux,
            'metacritic_score': metacritic_score,
            'metacritic_url': metacritic_url,
            'user_score': user_score,
            'positive': positive,
            'negative': negative,
            'score_rank': score_rank,
            'achievements': achievements,
            'recommendations': recommendations,
            'notes': notes,
            'average_playtime_forever': average_playtime_forever,
            'average_playtime_2weeks': average_playtime_2weeks,
            'median_playtime_forever': median_playtime_forever,
            'median_playtime_2weeks': median_playtime_2weeks,
            'categories': categories,
            'single_player': single_player,
            'multiplayer': multiplayer,
            'genres': genres,
            'developers': developers,
            'publishers': publishers,
        }
        games_df.loc[len(games_df)] = game_data

    #Fix illegal character
    games_df = games_df.applymap(lambda x: x.encode('unicode_escape').
                 decode('utf-8') if isinstance(x, str) else x)
    games_df.to_excel('games_json_parsed.xlsx', index=False)

if __name__ == "__main__":
  json_data = get_json_data(file_path)
  parse_json_into_dataframe(json_data)