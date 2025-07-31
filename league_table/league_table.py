# package imports
import polars as pl
import sys

# function imports
from polars import DataFrame
from supabase import Client

# local imports
from api.sleeper import get_rosters
from db.supabase import get_standings, upsert_standings

def get_league_table(client: Client, season_id: str, week: int) -> DataFrame:
    """
    Gets the league table from the Supabase 'standing' table and returns a polars DataFrame.
    Makes use of the 'get_standings(client: Client, season_id: str, week: int)' function.
    Calculates a 'move' column in weeks other than week 1 based on previous standings.

    Args:
        client (Client): The Supabase client.
        season_id (str): The UUID of the season.
        week (int): The week number for the standings.

    Returns:
        pl.DataFrame: A DataFrame of the current league table.
    """
    try:
        standings: DataFrame | None = get_standings(client, season_id, week)
        if standings is None:
            raise RuntimeError(f"No standing records were returned for season_id: {season_id=} and week: {week=}.")
        
        if week <= 1:
            return standings
        
        previous_standings: DataFrame | None = get_standings(client, season_id, week - 1)
        if previous_standings is None:
            raise RuntimeError(f"No standing records were returned for season_id: {season_id=} and the previous week: week={week - 1}.")

        return calculate_move(standings, previous_standings)

    except Exception as ex:
        raise RuntimeError(f"Error getting league table.\nException: {ex}")
    
def update_standings(client: Client, sleeper_league_id: str, season_id: str, week: int) -> DataFrame:
    """
    Updates the weekly standings in the Supabase 'standing' table with roster data from the Sleeper API and returns a polars DataFrame.
    Makes use of the 'get_rosters(sleeper_league_id: str)' and the 'upsert_standings(client: Client, season_id: str, week: int, rosters: list[dict[str, str]])' functions.

    Args:
        client (Client): The Supabase client.
        sleeper_league_id (str): The Sleeper id of the league.
        season_id (str): The UUID of the season.
        week (int): The week number for the standings.

    Returns:
        pl.DataFrame: A DataFrame with the records upserted into the 'standing' table.
    """
    try:
        rosters: list[dict[str, str]] | None = get_rosters(sleeper_league_id)
        if rosters is None:
            raise RuntimeError(f"No rosters were returned in the API response for sleeper league id: {sleeper_league_id=}.")

        upsert_response: DataFrame | None = upsert_standings(client, season_id, week, rosters)
        if upsert_response is None:
            raise RuntimeError(f"No data was returned in the upsert response for season_id: {season_id=} and week: {week=}.")
    
        return upsert_response

    except Exception as ex:
        raise RuntimeError(f"Error updating league table.\nException: {ex}")
    
def calculate_move(current_standings: DataFrame, previous_standings: DataFrame) -> DataFrame:
    """
    Calculates the 'move' column for the current standings by subtracting the standing from the previous standing.

    Args:
        current_standings (DataFrame): The standings for the current week.
        previous_standings (DataFrame): The standings for the previous week.

    Returns:
        pl.DataFrame: A DataFrame of the current standings with a 'move' column.
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

def get_week() -> int:
    """
    Gets the fantasy football week from user input.

    Returns:
        int: The fantasy football week.
    """
    while True:
        try:
            user_input = input("Enter the week number for which to update and fetch the league table (or 'quit' to exit): ").strip()
            if user_input.lower() == "quit":
                print(f"Exiting the program.")
                sys.exit()

            week = int(user_input)
            if week < 1 or week > 14:
                print(f"Please enter a valid week of the fantasy season between 1 and 14.")
                continue

            return week
        
        except Exception as ex:
            print(f"Invalid input. Please enter a positive integer between one (1) and fourteen (14) representing a week of the fantasy season.")
            