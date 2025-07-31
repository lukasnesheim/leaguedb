# package imports
import polars as pl

# function imports
from polars import DataFrame
from supabase import Client

def get_club_ids(client: Client) -> dict[str, str] | None:
    """
    Gets a club lookup dict from the Supabase 'club' table.

    Args:
        client (Client): The Supabase client instance.

    Returns:
        dict[str, str] | None: A lookup dict of external club id to club id.
        For example: {external: id}. Returns None if no records are found.
    """
    try:
        # query club records
        response = client.table("club").select("id, external").execute()
        if not response.data:
            return None
        
        return { row["external"]: row["id"] for row in response.data }
    
    except Exception as ex:
        raise RuntimeError(f"Querying the Supabase database for club ids failed.\nException: {ex}")

def get_standings(client: Client, season_id: str, week: int) -> DataFrame | None:
    """
    Gets league standings from the Supabase 'standing' table for a given season and week.

    Args:
        client (Client): The Supabase client instance.
        season_id (str): The UUID of the season.
        week (int): The week number to filter standings by.

    Returns:
        pl.DataFrame | None: A DataFrame with calculated standings metrics (ortg, drtg, eff, etc.).
        Returns None if no records are found.
    """
    try:
        # query the supabase standing table
        response = client.table("standing").select("*, club(id, name)").eq("season", season_id).eq("week", week).execute()
        if not response.data:
            return None
        
        # map the response data to a polars data frame
        return pl.DataFrame([
            {
                "move": "",
                "standing": row["standing"],
                "club_id": row["club"]["id"],
                "club": row["club"]["name"],
                "week": row["week"],
                "win": row["win"],
                "loss": row["loss"],
                "draw": row["draw"],
                "ortg": round(float(row["pf"]) / week, 2),
                "drtg": round(float(row["pa"]) / week, 2),
                "mpf": row["mpf"],
                "eff": round(float(row["pf"]) / float(row["mpf"]), 3)
            } for row in response.data
        ])
    
    except Exception as ex:
        raise RuntimeError(f"Querying the Supabase database for league table failed for season id: {season_id=}.\nException: {ex}")
    
def upsert_standings(client: Client, season_id: str, week: int, rosters: list[dict[str, str]]) -> DataFrame | None:
    """
    Upserts weekly standings into the Supabase 'standing' table and returns a polars DataFrame.

    Args:
        client (Client): The Supabase client.
        season_id (str): The UUID of the season.
        week (int): The week number for the standings.
        rosters (list[dict[str, str]]): Roster data from Sleeper.

    Returns:
        pl.DataFrame | None: A DataFrame with the records upserted into the 'standing' table.
        Returns None if no records are found.
    """
    try:
        # get club id lookup dict
        clubs: dict[str, str] | None = get_club_ids(client)
        if clubs is None:
            raise RuntimeError(f"No club records were returned for lookup.")
        
        standings: list[dict[str, str]] = [
            {
                "season": season_id,
                "club": clubs[roster["owner_id"]],
                "week": str(week),
                "standing": roster["standing"],
                "win": roster["win"],
                "loss": roster["loss"],
                "draw": roster["draw"],
                "pf": roster["pf"],
                "pa": roster["pa"],
                "mpf": roster["mpf"]
            } for roster in rosters
            if clubs.get(roster["owner_id"])
        ]

        # upsert standing records
        response = client.table("standing").upsert(standings, on_conflict="season,club,week").execute()
        if not response.data:
            return None
        
        # map the response data to a polars data frame
        return pl.DataFrame([
            {
                "id": row["id"],
                "season": row["season"],
                "club": row["club"],
                "week": row["week"],
                "standing": row["standing"],
                "win": row["win"],
                "loss": row["loss"],
                "draw": row["draw"],
                "mpf": row["mpf"]
            } for row in response.data
        ])

    except Exception as ex:
        raise RuntimeError(f"Querying the Supabase database to upsert standings failed for season id: {season_id=} and week: {week=}.\nException: {ex}")