# function imports
from supabase import Client
from typing import Any

def get_clubs(client: Client, league_id: str | None = None) -> list[dict[str, Any]] | None:
    """
    Gets clubs from the Supabase 'club' table for a given league.

    Args:
        client (Client): The Supabase client instance.
        league_id (str | None = None): The optional Supabase UUID of the league.

    Returns:
        list[dict[str, Any]] | None: A list of club rows returned from the Supabase get.
        Returns None if no records are found.
    """
    try:
        query = client.table("club").select("*, manager(name, sleeper)").eq("league", league_id) if league_id is not None else client.table("club").select("*, manager(name, sleeper)")
        response = query.execute()

        if not response.data:
            return None
        
        return [
            {
                **{key: value for key, value in row.items() if key != "manager"},
                "manager": row["manager"]["name"],
                "sleeper": row["manager"]["sleeper"]
            } for row in response.data
        ]
        
    except Exception as ex:
        raise RuntimeError(f"Querying the Supabase database to get clubs failed for {league_id=}.") from ex