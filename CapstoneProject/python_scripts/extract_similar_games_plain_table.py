from pathlib import Path
import pandas as pd
from tqdm import tqdm

def extract_similar_games_info():
    """"
    Extract the information from igdb, the info is in list and pyspark explode didn't work
    """
    base_path = Path(__file__).parent
    csv_path = str((base_path / "../data/igdb_data.csv").resolve())
    csv_similar_path = str((base_path / "../data/igdb_similar_games.csv").resolve())
    similar_games_dataframe = pd.DataFrame(columns=['id', 'similar_game_id'])
    df = pd.read_csv(csv_path, sep="|")
    df = df.dropna(subset=['similar_games'])
    for index, row in tqdm(df.iterrows(), desc='processing'):
        id = row['id']
        similar_games = (row['similar_games']).replace("[","").replace("]","").replace(" ","").split(",")
        for game in similar_games:
            data = {
                "id": id,
                "similar_game_id": game
            }
            similar_games_dataframe.loc[len(similar_games_dataframe)] = data
    similar_games_dataframe.to_csv(csv_similar_path, sep='|', index=False)


if __name__ == "__main__":
	extract_similar_games_info()
    