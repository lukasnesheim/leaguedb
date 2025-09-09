# package imports
import requests

# function imports
from collections import defaultdict
from typing import Any

def get_sleeper_matchups(sleeper_league_id: str, week: int) -> list[dict[str, Any]]:
    """
    Gets matchup data from the Sleeper API for a given week and returns a list of SleeperMatchup objects with roster ids mapped.

    Args:
        sleeper_league_id (str): The Sleeper id of the league.
        week (int): The week number of the matchups.

    Returns:
        list[dict[str, Any]] | None: A list of dictionaries with Sleeper matchup data.
        Returns None if no matchups are returned in the API response.
    """
    try:
        # get matchup data from sleeper api
        matchup_response = requests.get(f"https://api.sleeper.app/v1/league/{sleeper_league_id}/matchups/{week}")
        matchup_response.raise_for_status()

        if not (matchup_data := matchup_response.json()):
            raise RuntimeError(f"No matchup data returned from the Sleeper API for {sleeper_league_id=} and {week=}.")

        matchups: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
        for matchup in matchup_data:
            matchups[matchup["matchup_id"]].append(matchup)

        # get roster data from sleeper api
        roster_response = requests.get(f"https://api.sleeper.app/v1/league/{sleeper_league_id}/rosters")
        roster_response.raise_for_status()

        rosters = roster_response.json()
        if not (rosters := roster_response.json()):
            raise RuntimeError(f"No matchup data returned from the Sleeper API for {sleeper_league_id=}.")

        # roster id to sleeper id lookup
        sleeper_lookup = { roster["roster_id"]: roster["owner_id"] for roster in rosters }

        sleeper_matchups: list[dict[str, Any]] = []

        # iterate through matchups to map rosters
        for matchup_id, matchup in matchups.items():
            if len(matchup) != 2:
                raise RuntimeError(f"Expected 2 matchups for {matchup_id} but had {len(matchup)}.")
            
            matchup_x, matchup_y = matchup

            sleeper_id_x = sleeper_lookup[matchup_x["roster_id"]]
            sleeper_id_y = sleeper_lookup[matchup_y["roster_id"]]

            # append the sleeper matchup
            sleeper_matchups.append({
                "week": week,
                "sleeper_id_x": sleeper_id_x,
                "score_x": float(matchup_x["points"]),
                "sleeper_id_y": sleeper_id_y,
                "score_y": float(matchup_y["points"])
            })

        # check for uniqueness
        sleeper_ids = [id for sm in sleeper_matchups for id in (sm["sleeper_id_x"], sm["sleeper_id_y"])]
        if len(sleeper_ids) != len(set(sleeper_ids)) or len(sleeper_ids) not in (10, 12):
            raise RuntimeError(f"The same sleeper owner id was assigned to multiple matchup participants.")

        return sleeper_matchups

    except Exception as ex:
        raise RuntimeError(f"Querying the Sleeper API failed for sleeper league id: {sleeper_league_id=} and week: {week=}.") from ex