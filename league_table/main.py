# package imports
import os
import sys

# function imports
from dotenv import load_dotenv
from polars import DataFrame
from supabase import Client, create_client

# local imports
from db.supabase import get_league_table

load_dotenv()

week: int = 0

season_id: str | None = os.getenv("HOMIES_2024_SUPABASE")
if not season_id:
    raise ValueError(f"Failed to retrieve the environment variable for season id.")

supabase_url: str | None = os.getenv("SUPABASE_URL")
if not supabase_url:
    raise ValueError(f"Failed to retrieve the environment variable for supabase url.")

supabase_key: str | None = os.getenv("SUPABASE_KEY")
if not supabase_key:
    raise ValueError(f"Failed to retrieve the environment variable for supabase key.")

try:
    client: Client = create_client(supabase_url, supabase_key)

    league_table: DataFrame | None = get_league_table(client, season_id, week)
    if league_table is None:
        raise RuntimeError(f"No podium records were returned for season id: {season_id=}.")

    league_table.sort("rank").write_csv("league_table/data/league_table.csv")

except Exception as ex:
    print(f"An error occurred:\n{ex}\nExiting now.")
    sys.exit()