# package imports
import os
import polars as pl
import sys
import traceback

# function imports
from dotenv import load_dotenv
from supabase import Client, create_client

# local imports
from league_table.core import get_league_table, update_standings
from shared.python.utils import get_week

load_dotenv()

season_id: str | None = os.getenv("HOMIES_2024_SUPABASE")
if not season_id:
    raise ValueError(f"Failed to retrieve the environment variable for season id.")

sleeper_league_id: str | None = os.getenv("HOMIES_2024_SLEEPER")
if not sleeper_league_id:
    raise ValueError(f"Failed to retrieve the environment variable for sleeper league id.")

supabase_league_id: str | None = os.getenv("HOMIES_ID")
if not supabase_league_id:
    raise ValueError(f"Failed to retrieve the environment variable for supabase league id.")

supabase_url: str | None = os.getenv("SUPABASE_URL")
if not supabase_url:
    raise ValueError(f"Failed to retrieve the environment variable for supabase url.")

supabase_key: str | None = os.getenv("SUPABASE_KEY")
if not supabase_key:
    raise ValueError(f"Failed to retrieve the environment variable for supabase key.")

try:
    # get the league table week
    week: int = get_week()

    # initialize the Supabase client
    client: Client = create_client(supabase_url, supabase_key)

    # update the most recent standings
    _ = update_standings(client, sleeper_league_id, supabase_league_id, season_id, week)

    # get the most recent league table
    league_table: pl.DataFrame = get_league_table(client, season_id, week)

    # save the league table as a .csv
    league_table.drop(["id", "season", "club_id"]).sort("standing").write_csv("league_table/data/league_table.csv")

except Exception as ex:
    print(f"An error occurred:")
    traceback.print_exception(type(ex), ex, ex.__traceback__)
    print(f"Exiting now...")
    sys.exit()