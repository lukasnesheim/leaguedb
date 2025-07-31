# package imports
import os
import sys

# function imports
from dotenv import load_dotenv
from polars import DataFrame
from supabase import Client, create_client

# local imports
from league_table.league_table import get_league_table, get_week, update_standings

load_dotenv()

season_id: str | None = os.getenv("HOMIES_2023_SUPABASE")
if not season_id:
    raise ValueError(f"Failed to retrieve the environment variable for season id.")

sleeper_league_id: str | None = os.getenv("HOMIES_2023_SLEEPER")
if not sleeper_league_id:
    raise ValueError(f"Failed to retrieve the environment variable for sleeper league id.")

supabase_url: str | None = os.getenv("SUPABASE_URL")
if not supabase_url:
    raise ValueError(f"Failed to retrieve the environment variable for supabase url.")

supabase_key: str | None = os.getenv("SUPABASE_KEY")
if not supabase_key:
    raise ValueError(f"Failed to retrieve the environment variable for supabase key.")

try:
    # get the league table week
    week: int = get_week()

    client: Client = create_client(supabase_url, supabase_key)

    # update the most recent standings
    _ = update_standings(client, sleeper_league_id, season_id, week)

    # get the most recent league table
    league_table: DataFrame = get_league_table(client, season_id, week)

    # save the league table as a .csv
    league_table.sort("standing").write_csv("league_table/data/league_table.csv")

except Exception as ex:
    print(f"An error occurred:\n{ex}\nExiting now.")
    sys.exit()