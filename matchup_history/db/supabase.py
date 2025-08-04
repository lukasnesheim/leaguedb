# package imports
import polars as pl

# function imports
from supabase import Client
from typing import Any

def get_club_lookup(client: Client) -> dict[str, str] | None:
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
        raise RuntimeError(f"Querying the Supabase database for club ids failed.") from ex

def get_matchups(client: Client, season_id: str | None = None) -> pl.DataFrame | None:
    """
    Gets matchups from the Supabase 'matchup' table.

    Args:
        client (Client): The Supabase client instance.
        season_id (str | None = None): The optional Supabase UUID of the season.

    Returns:
        polars.DataFrame: A DataFrame with the matchup records from the 'matchup' table.
        Returns None if no records are found.
    """
    try:
        query = client.table("matchup").select("*").eq("season", season_id) if season_id is not None else client.table("matchup").select("*")
        if not (response := query.execute()).data:
            return None
        
        return pl.DataFrame(response.data)
        
    except Exception as ex:
        raise RuntimeError(f"Querying the Supabase database to get matchups failed with {season_id=}.") from ex

def upsert_matchups(client: Client, matchups: list[dict[str, Any]]) -> pl.DataFrame | None:
    """
    Upserts weekly matchups into the Supabase 'matchup' table and returns a polars DataFrame.

    Args:
        client (Client): The Supabase client instance.
        matchups (list[dict[str, Any]]): Matchup data to upsert.

    Returns:
        polars.DataFrame: A DataFrame with the matchup records upserted into the 'matchup' table.
        Returns None if no records are returned in the Supabase client upsert response.
    """
    try:
        if not (response := client.table("matchup").upsert(matchups, on_conflict="season, club_x, club_y, week").execute()).data:
            return None
        
        return pl.DataFrame(response.data)
    
    except Exception as ex:
        raise RuntimeError(f"Querying the Supabase database to upsert matchups failed.") from ex