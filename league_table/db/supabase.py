# package imports
import polars as pl

# function imports
from polars import DataFrame
from supabase import Client

def get_league_table(client: Client, season_id: str, week: int) -> DataFrame | None:
    try:
        # query the supabase podium table
        response = client.table("podium").select("*, club(name)").eq("season", season_id).eq("week", week).execute()
        if not response.data:
            return None
        
        # map the response data to a polars data frame
        return pl.DataFrame([
            {
                "rank": row["regular"],
                "club": row["club"]["name"],
                "win": row["win_regular"],
                "loss": row["loss_regular"],
                "draw": row["draw_regular"],
                "pf": row["for"],
                "pa": row["against"],
                "ortg": round(float(row["for"]) / (week if week != 0 else 14), 2),
                "drtg": round(float(row["against"]) / (week if week != 0 else 14), 2),
                "mpf": row["max"]
            } for row in response.data
        ])
    
    except Exception as ex:
        raise RuntimeError(f"Querying the Supabase database failed for season id: {season_id=}.\nException: {ex}")