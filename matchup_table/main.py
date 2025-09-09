# package imports
import os
import polars as pl
import sys
import traceback

# function imports
from dotenv import load_dotenv
from supabase import Client, create_client

# local imports
from matchup_table.core import get_matchup_table
from shared.python.utils import get_week

load_dotenv()

league: str = "HOMIES"

season_id: str | None = os.getenv(f"{league}_2025_SUPABASE")
if not season_id:
    raise ValueError(f"Failed to retrieve the environment variable for season id.")

supabase_league_id: str | None = os.getenv(f"{league}_ID")
if not supabase_league_id:
    raise ValueError(f"Failed to retrieve the environment variable for supabase league id.")

supabase_url: str | None = os.getenv("SUPABASE_URL")
if not supabase_url:
    raise ValueError(f"Failed to retrieve the environment variable for supabase url.")

supabase_key: str | None = os.getenv("SUPABASE_KEY")
if not supabase_key:
    raise ValueError(f"Failed to retrieve the environment variable for supabase key.")

try:
    # get the matchup week
    week: int = get_week()

    # initialize the Supabase client
    client: Client = create_client(supabase_url, supabase_key)

    # get matchups from Supabase
    matchups: pl.DataFrame = get_matchup_table(client, season_id, supabase_league_id, week) 
    matchups.write_csv("matchup_table/data/matchup_table.csv")

except Exception as ex:
    print(f"An error occurred:")
    traceback.print_exception(type(ex), ex, ex.__traceback__)
    print(f"Exiting now...")
    sys.exit()