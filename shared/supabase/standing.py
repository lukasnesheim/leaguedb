# function imports
from supabase import Client
from typing import Any

def get_standings(client: Client, season_id: str | None = None) -> list[dict[str, Any]] | None:
    """
    Gets standings from the Supabase 'standing' table for a given season.

    Args:
        client (Client): The Supabase client instance.
        season_id (str | None = None): The optional Supabase UUID of the season.

    Returns:
        list[dict[str, Any]] | None: A list of standing rows returned from the Supabase get.
        Returns None if no records are found.
    """
    try:
        query = client.table("standing").select("*").eq("season", season_id) if season_id is not None else client.table("standing").select("*")
        response = query.execute()
        
        return response.data or None
    
    except Exception as ex:
        raise RuntimeError(f"Querying the Supabase database to get standings failed for {season_id=}.") from ex
    
def upsert_standings(client: Client, standings: list[dict[str, Any]]) -> list[dict[str, Any]] | None:
    """
    Upserts weekly standings into the Supabase 'standing' table.

    Args:
        client (Client): The Supabase client instance.
        standings (list[dict[str, Any]]): Standings data to upsert.

    Returns:
        list[dict[str, Any]] | None: A list of standing rows returned from the Supabase upsert.
        Returns None if no records are returned in the Supabase client upsert response.
    """
    try:
        query = client.table("standing").upsert(standings, on_conflict="season, club, week")
        response = query.execute()

        return response.data or None

    except Exception as ex:
        raise RuntimeError(f"Querying the Supabase database to upsert standings failed.") from ex