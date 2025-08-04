# package imports
import requests

# function imports
from collections import defaultdict
from typing import Any

# local imports
from matchup_history.model import SleeperMatchup

def get_sleeper_matchups(sleeper_league_id: str, week: int) -> list[SleeperMatchup] | None:
    """
    Gets matchup data from the Sleeper API for a given week and returns a list of SleeperMatchup objects with roster ids mapped.

    Args:
        sleeper_league_id (str): The Sleeper id of the league.
        week (int): The week number of the matchups.

    Returns:
        list[SleeperMatchup] | None: A list of SleeperMatchup objects with Sleeper matchup data.
        Returns None if no matchups are returned in the API response.
    """
    try:
        # get matchup data from sleeper api
        matchup_response = requests.get(f"https://api.sleeper.app/v1/league/{sleeper_league_id}/matchups/{week}")
        matchup_response.raise_for_status()

        if not (matchup_data := matchup_response.json()):
            return None

        matchups: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
        for matchup in matchup_data:
            matchups[matchup["matchup_id"]].append(matchup)

        # get roster data from sleeper api
        roster_response = requests.get(f"https://api.sleeper.app/v1/league/{sleeper_league_id}/rosters")
        roster_response.raise_for_status()

        if not (roster_data := roster_response.json()):
            return None

        rosters: list[tuple[str, set[str]]] = []
        for roster in roster_data:
            rosters.append((roster["owner_id"], set(roster["players"])))

        sleeper_matchups: list[SleeperMatchup] = []

        # iterate through matchups to map rosters
        for matchup_id, (matchup_x, matchup_y) in matchups.items():
            players_x = set(matchup_x["players"])
            players_y = set(matchup_y["players"])

            # map sleeper ids
            sleeper_id_x, sleeper_id_y = map_sleeper_ids(matchup_id, players_x, players_y, rosters)

            # append the sleeper matchup
            sleeper_matchups.append(SleeperMatchup(week = week, sleeper_id_x = sleeper_id_x, score_x = float(matchup_x["points"]), sleeper_id_y = sleeper_id_y, score_y = float(matchup_y["points"])))

        # check for uniqueness
        sleeper_ids = [id for sm in sleeper_matchups for id in (sm.sleeper_id_x, sm.sleeper_id_y)]
        if len(sleeper_ids) != len(set(sleeper_ids)):
            raise RuntimeError(f"The same sleeper owner id was assigned to multiple matchup participants.")

        return sleeper_matchups

    except Exception as ex:
        raise RuntimeError(f"Querying the Sleeper API failed for sleeper league id: {sleeper_league_id=} and week: {week=}.") from ex

def map_sleeper_ids(matchup_id: str, players_x: set[str], players_y: set[str], rosters: list[tuple[str, set[str]]], threshold: float = 0.5) -> tuple[str, str]:
    sleeper_id_x = ""
    sleeper_id_y = ""
    
    for sleeper_id, roster_players in rosters:
        if not sleeper_id_x and len(players_x & roster_players) / len(players_x) >= threshold:
            sleeper_id_x = sleeper_id
        elif not sleeper_id_y and len(players_y & roster_players) / len(players_y) >= threshold:
            sleeper_id_y = sleeper_id
        
        if sleeper_id_x and sleeper_id_y:
            break
    
    if not sleeper_id_x or not sleeper_id_y:
        raise RuntimeError(f"Roster ids were not mapped for {matchup_id=}. Adjust the threshold.")
    
    return sleeper_id_x, sleeper_id_y