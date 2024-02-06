import datetime
import json
import logging
import os
from typing import Dict, Tuple

import pandas as pd

logger = logging.getLogger()
logging.basicConfig()
logger.setLevel("INFO")

HOME_TEAM_PRICE = AWAY_TEAM_PRICE = TIE_PRICE = float


def get_odd_by_type(prices: str, game_info: dict) -> Tuple[HOME_TEAM_PRICE, AWAY_TEAM_PRICE, TIE_PRICE]:
    for price in prices:
        if price['key'] == 'h2h':
            home_team_price = away_team_price = tie_price = -1
            for outcome in price['outcomes']:
                if outcome['name'] == game_info['home_team']:
                    home_team_price = outcome['price']
                elif outcome['name'] == game_info['away_team']:
                    away_team_price = outcome['price']
                elif outcome['name'] == 'Draw':
                    tie_price = outcome['price']
                else:
                    raise ValueError()
            return home_team_price, away_team_price, tie_price
    raise ValueError("h2h not provided")


def process_odds_files(files_directory: str):
    def attach_bookmakers_odds():
        for bookmaker in game['bookmakers']:
            if not bookmaker['key'] in games[game['id']]['bookmakers']:
                games[game['id']]['bookmakers'][bookmaker['key']] = []

            home_team_price, away_team_price, tie_price = get_odd_by_type(prices=bookmaker['markets'],
                                                                          game_info=game_info)
            games[game['id']]['bookmakers'][bookmaker['key']].append({
                'updated_timestamp': bookmaker['last_update'],
                'outcome': {
                    'home_team': home_team_price,
                    'away_team': away_team_price,
                    'draw': tie_price
                }
            })

            games[game['id']]['bookmakers'][bookmaker['key']].sort(key=lambda obj: obj['updated_timestamp'])

    games = {}
    directory = os.fsencode(files_directory)
    for file in os.listdir(directory):
        filename = os.fsdecode(file)
        file_content = json.load(open(f'{files_directory}/{filename}'))

        for game in file_content['data']:
            game_info = {
                'home_team': game['home_team'],
                'away_team': game['away_team'],
                'sport_key': game['sport_key'],
                'game_time': game['commence_time'],
            }

            if not game['id'] in games:
                games[game['id']] = {
                    'id': game['id'],
                    'bookmakers': {},
                    **game_info
                }

            attach_bookmakers_odds()  # By ref

    return games


def save_data_as_csv(data):
    games_array = [game for game in data.values()]
    with open('final_data.json', 'w') as f:
        json.dump(games_array, f)

    df = pd.read_json('final_data.json')
    df = df[['id', 'home_team', 'away_team', 'sport_key', 'game_time', 'label', 'bookmakers']]
    df.to_csv('final_data.csv')


def enrich_odds_data_with_scores(games, historic_games) -> Dict:
    games_without_scores = 0

    relevant_scores_df = historic_games.loc[historic_games['Season_End_Year'] > 2020]
    for game_id, game_info in games.items():
        away_team, home_team = game_info['away_team'], game_info['home_team']
        game_date = str(datetime.datetime.strptime(game_info['game_time'], '%Y-%m-%dT%H:%M:%SZ').date())
        specific_game = relevant_scores_df.query(
            f"Home == '{home_team}' & Away == '{away_team}' & Date == '{game_date}' "
        ).head()
        if specific_game.empty:
            logger.warning(f"There is no data for this game...{away_team}-{home_team} in {game_date}")
            games_without_scores += 1
            continue

        home_goals, away_goals = specific_game['HomeGoals'].values[0], specific_game['AwayGoals'].values[0]
        if home_goals == away_goals:
            game_info['label'] = 'tie'
        else:
            game_info['label'] = 'home' if home_goals > away_goals else 'away'

    logger.info(f"Processed {len(games)} games.")
    logger.info(f"Couldn't find scores for  {games_without_scores} games.")
    return games


def load_historic_games_scores(path: str):
    def kaggle_data_pre_process(teams_df):
        from data_sources.mappings import odds_api_to_kaggle_mapping
        for kaggle_value, odds_api_value in odds_api_to_kaggle_mapping.items():
            teams_df = teams_df.replace(kaggle_value, odds_api_value)
        return teams_df

    return kaggle_data_pre_process(pd.read_csv(path))


if __name__ == '__main__':
    games_odds = process_odds_files(files_directory='../data_sources/odds_data')
    historic_games_df = load_historic_games_scores(path='../data_sources/historic_games/premier-league-matches.csv')
    games_odds_with_labels = enrich_odds_data_with_scores(games=games_odds, historic_games=historic_games_df)
    save_data_as_csv(data=games_odds_with_labels)
