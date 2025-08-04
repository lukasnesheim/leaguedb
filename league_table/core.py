# package imports
import polars as pl

# function imports
from supabase import Client
from typing import Any

# local imports
from league_table.api.sleeper import get_rosters
from league_table.db.supabase import get_club_lookup, get_standings, upsert_standings

def get_league_table(client: Client, season_id: str, week: int) -> pl.DataFrame:
    """
    Gets the league table from the Supabase 'standing' table and returns a polars DataFrame.
    Makes use of the 'get_standings(client: Client, season_id: str, week: int)' function.
    Calculates a 'move' column in weeks other than week 1 based on previous standings.

    Args:
        client (Client): The Supabase client.
        season_id (str): The UUID of the season.
        week (int): The week number of the league table.

    Returns:
        polars.DataFrame: A DataFrame of the current league table.
    """
    try:
        standings: pl.DataFrame | None = get_standings(client, season_id)
        if standings is None:
            raise RuntimeError(f"No standing records were returned for {season_id=} and {week=}.")
        
        current_standings = standings.filter(pl.col("week") == week).with_columns([
            (pl.col("pf") / week).round(2).alias("ortg"),
            (pl.col("pa") / week).round(2).alias("drtg"),
            (pl.col("pf") / pl.col("mpf")).round(3).alias("eff"),
        ])

        if week == 1:
            return current_standings

        previous_standings = standings.filter(pl.col("week") == week - 1)

        return calculate_move(current_standings, previous_standings)

    except Exception as ex:
        raise RuntimeError(f"Error getting league table.") from ex
    
def update_standings(client: Client, sleeper_league_id: str, season_id: str, week: int) -> pl.DataFrame:
    """
    Updates the weekly standings in the Supabase 'standing' table with roster data from the Sleeper API and returns a polars DataFrame.
    Makes use of the 'get_rosters(sleeper_league_id: str)' and the 'upsert_standings(client: Client, season_id: str, week: int, rosters: list[dict[str, str]])' functions.

    Args:
        client (Client): The Supabase client.
        sleeper_league_id (str): The Sleeper id of the league.
        season_id (str): The UUID of the season.
        week (int): The week number for the standings.

    Returns:
        polars.DataFrame: A DataFrame with the records upserted into the 'standing' table.
    """
    try:
        # get sleeper rosters
        if not (rosters := get_rosters(sleeper_league_id)):
            raise RuntimeError(f"No rosters were returned in the Sleeper API response for {sleeper_league_id=}.")

        # get club id lookup dict
        if not (club_lookup := get_club_lookup(client)):
            raise RuntimeError(f"No club records were returned for lookup.")
        
        standings: list[dict[str, Any]] = [
            {
                "season": season_id,
                "club": club_lookup[roster["owner_id"]],
                "week": week,
                "standing": int(roster["standing"]),
                "win": int(roster["win"]),
                "loss": int(roster["loss"]),
                "draw": int(roster["draw"]),
                "pf": float(roster["pf"]),
                "pa": float(roster["pa"]),
                "mpf": float(roster["mpf"])
            } for roster in rosters
            if roster["owner_id"] in club_lookup
        ]

        if (upsert_response := upsert_standings(client, standings)) is None:
            raise RuntimeError(f"No data was returned in the upsert response for {season_id=} and {week=}.")
    
        return upsert_response

    except Exception as ex:
        raise RuntimeError(f"Error updating standings.") from ex
    
def calculate_move(current_standings: pl.DataFrame, previous_standings: pl.DataFrame) -> pl.DataFrame:
    """
    Calculates the 'move' column for the current standings by subtracting the standing from the previous standing.

    Args:
        current_standings (polars.DataFrame): The standings for the current week.
        previous_standings (polars.DataFrame): The standings for the previous week.

    Returns:
        polars.DataFrame: A DataFrame of the current standings with a 'move' column.
    """
    standings = current_standings.join(previous_standings.select(["club_id", "standing"]).rename({"standing": "previous"}), on="club_id", how="left")
    standings = standings.with_columns((pl.col("previous") - pl.col("standing")).alias("move"))
    
    return standings.with_columns(
        pl.when(pl.col("move") > 0)
          .then("+" + pl.col("move").cast(str))
          .when(pl.col("move") < 0)
          .then(pl.col("move").cast(str))
          .otherwise("")
          .alias("move")
    ).drop(["previous", "club_id"])