# function imports
from supabase import Client
from typing import Any

def get_matchups(client: Client, season_id: str | None = None) -> list[dict[str, Any]] | None:
    """
    Gets matchups from the Supabase 'matchup' table for a given league.

    Args:
        client (Client): The Supabase client instance.
        season_id (str | None = None): The optional Supabase UUID of the season.

    Returns:
        list[dict[str, Any]] | None: A list of matchup rows returned from the Supabase get.
        Returns None if no records are found.
    """
    try:
        query = client.table("matchup").select("*").eq("season", season_id) if season_id is not None else client.table("matchup").select("*")
        response = query.execute()
        
        return response.data or None
        
    except Exception as ex:
        raise RuntimeError(f"Querying the Supabase database to get matchups failed with {season_id=}.") from ex

def upsert_matchups(client: Client, matchups: list[dict[str, Any]]) -> list[dict[str, Any]] | None:
    """
    Upserts weekly matchups into the Supabase 'matchup' table.

    Args:
        client (Client): The Supabase client instance.
        matchups (list[dict[str, Any]]): Matchup data to upsert.

    Returns:
        list[dict[str, Any]] | None: A list of matchup rows returned from the Supabase upsert.
        Returns None if no records are returned in the Supabase client upsert response.
    """
    try:
        query = client.table("matchup").upsert(matchups, on_conflict="season, club_x, club_y, week")
        response = query.execute()

        return response.data or None
    
    except Exception as ex:
        raise RuntimeError(f"Querying the Supabase database to upsert matchups failed.") from ex