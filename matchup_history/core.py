# package imports
import polars as pl

# function imports
from supabase import Client
from typing import Any

# local imports
from matchup_history.api.sleeper import get_sleeper_matchups
from matchup_history.db.supabase import get_club_lookup, upsert_matchups
from shared.python.enum import get_enum

def update_matchups(client: Client, sleeper_league_id: str, season_id: str, week: int) -> pl.DataFrame:
    """
    Updates the matchups in the Supabase 'matchup' table with matchup data from the Sleeper API and returns a polars DataFrame.

    Args:
        client (Client): The Supabase client.
        sleeper_league_id (str): The Sleeper id of the league.
        season_id (str): The Supabase UUID of the season.
        week (int): The week number of the matchups.

    Returns:
        polars.DataFrame: A DataFrame with the records upserted into the 'matchup' table.
    """
    try:
        # get sleeper matchups
        if not (sleeper_matchups := get_sleeper_matchups(sleeper_league_id, week)):
            raise RuntimeError(f"No matchups were returned in the Sleeper API response for {sleeper_league_id=}.")

        # get club id lookup dict
        if not (club_lookup := get_club_lookup(client)):
            raise RuntimeError(f"No club records were returned for lookup.")
        
        matchups: list[dict[str, Any]] = [
            {
                "season": season_id,
                "week": week,
                "club_x": club_lookup[sleeper_matchup.sleeper_id_x],
                "score_x": sleeper_matchup.score_x,
                "club_y": club_lookup[sleeper_matchup.sleeper_id_y],
                "score_y": sleeper_matchup.score_y,
                "winner": (
                    club_lookup[sleeper_matchup.sleeper_id_x]
                    if sleeper_matchup.score_x > sleeper_matchup.score_y
                    else club_lookup[sleeper_matchup.sleeper_id_y]
                    if sleeper_matchup.score_y > sleeper_matchup.score_x
                    else None
                ),
                "stage": get_enum("stage")["regular"] if week <= 14 else None,
                "round": None
            } for sleeper_matchup in sleeper_matchups
            if sleeper_matchup.sleeper_id_x in club_lookup and sleeper_matchup.sleeper_id_y in club_lookup
        ]

        if (upsert_response := upsert_matchups(client, matchups)) is None:
            raise RuntimeError(f"No data was returned in the upsert response for {season_id=} and {week=}.")

        return upsert_response
    
    except Exception as ex:
        raise RuntimeError(f"Error updating matchups.") from ex