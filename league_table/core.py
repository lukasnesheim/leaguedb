# package imports
import polars as pl

# function imports
from supabase import Client
from typing import Any

# local imports
from shared.sleeper.roster import get_sleeper_rosters
from shared.supabase.club import get_clubs
from shared.supabase.standing import get_standings, upsert_standings

def get_league_table(client: Client, season_id: str, week: int) -> pl.DataFrame:
    """
    Gets the league table for a given season and fantasy week.

    Args:
        client (Client): The Supabase client instance.
        season_id (str): The Supabase UUID of the season.
        week (int): The week number of the league table.

    Returns:
        polars.DataFrame: A DataFrame of the current league table.
    """
    try:
        # get standings
        standings = get_standings(client, season_id)
        if standings is None:
            raise RuntimeError(f"No standing records were returned for {season_id=}.")
        
        standings = pl.DataFrame(standings).rename({ "club": "club_id" })
        
        # get clubs
        clubs = get_clubs(client)
        if clubs is None:
            raise RuntimeError(f"No club records were returned.")
        
        clubs = pl.DataFrame(clubs).select([
            pl.col("id").alias("club_id"),
            pl.col("name").alias("club"),
            pl.col("manager").alias("manager")
        ])

        # merge club name into the standings dataframe
        standings = pl.DataFrame(standings).join(pl.DataFrame(clubs), on="club_id", how="inner")

        # calculate ortg, drtg, and eff for current standings
        current_standings = pl.DataFrame(standings).filter(pl.col("week") == week).with_columns([
            (pl.col("pf") / week).round(2).alias("ortg"),
            (pl.col("pa") / week).round(2).alias("drtg"),
            (pl.col("pf") / pl.col("mpf")).round(3).alias("eff")
        ])

        if week == 1:
            return current_standings

        # calculate move for current standings
        return calculate_move(current_standings, pl.DataFrame(standings).filter(pl.col("week") == week - 1))

    except Exception as ex:
        raise RuntimeError(f"Error getting league table.") from ex
    
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
          .then(pl.concat_str([pl.lit("+"), pl.col("move").cast(pl.Utf8)]))
          .when(pl.col("move") < 0)
          .then(pl.col("move").cast(pl.Utf8))
          .otherwise(pl.lit(""))
          .alias("move")
    ).drop(["previous"])

def update_standings(client: Client, sleeper_league_id: str, supabase_league_id: str, season_id: str, week: int) -> pl.DataFrame:
    """
    Updates the weekly standings in the Supabase 'standing' table with roster data from the Sleeper API and returns a polars DataFrame.
    Makes use of the 'get_rosters(sleeper_league_id: str)' and the 'upsert_standings(client: Client, season_id: str, week: int, rosters: list[dict[str, str]])' functions.

    Args:
        client (Client): The Supabase client.
        sleeper_league_id (str): The Sleeper id of the league.
        supabase_league_id: str: The Supabase UUID of the league.
        season_id (str): The Supabase UUID of the season.
        week (int): The week number for the standings.

    Returns:
        polars.DataFrame: A DataFrame with the records upserted into the 'standing' table.
    """
    try:
        # get sleeper rosters
        if not (rosters := get_sleeper_rosters(sleeper_league_id)):
            raise RuntimeError(f"No rosters were returned in the Sleeper API response for {sleeper_league_id=}.")

        # get clubs
        if not (clubs := get_clubs(client, supabase_league_id)):
            raise RuntimeError(f"No club records were returned from the Supabase database for {supabase_league_id=}.")

        # club lookup dict
        club_lookup = { club["sleeper"]: club["id"] for club in clubs if club.get("sleeper") }
        
        # create standings to upsert
        standings: list[dict[str, Any]] = [
            {
                "season": season_id,
                "club": club_lookup[roster["id"]],
                "week": week,
                "standing": int(roster["standing"]),
                "win": int(roster["win"]),
                "loss": int(roster["loss"]),
                "draw": int(roster["draw"]),
                "pf": float(roster["pf"]),
                "pa": float(roster["pa"]),
                "mpf": float(roster["mpf"])
            } for roster in rosters
            if roster["id"] in club_lookup
        ]

        # upsert the standings
        if (upsert_response := upsert_standings(client, standings)) is None:
            raise RuntimeError(f"No data was returned in the upsert response for {season_id=} and {week=}.")
    
        return pl.DataFrame(upsert_response)

    except Exception as ex:
        raise RuntimeError(f"Error updating standings.") from ex