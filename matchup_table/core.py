# package imports
import polars as pl

# function imports
from supabase import Client

# local imports
from shared.supabase.club import get_clubs
from shared.supabase.matchup import get_matchups

def get_matchup_table(client: Client, season_id: str, supabase_league_id: str, week: int) -> pl.DataFrame:
    """
    """
    try:
        # get matchups
        matchups = get_matchups(client)
        if matchups is None:
            raise RuntimeError(f"No matchup records were returned for {season_id=}.")
        
        matchups = pl.DataFrame(matchups)

        # get clubs
        clubs = get_clubs(client, supabase_league_id)
        if clubs is None:
            raise RuntimeError(f"No club records were returned for {supabase_league_id=}.")
        
        clubs = pl.DataFrame(clubs).drop(["league", "founded", "title", "active"])

        # add club info for club_x
        matchups = matchups.join(clubs.rename({"id": "club_x"}), on="club_x", how="inner").rename({"name": "name_x", "manager": "manager_x"}).drop("sleeper")

        # add club info for club_y
        matchups = matchups.join(clubs.rename({"id": "club_y"}), on="club_y", how="inner").rename({"name": "name_y", "manager": "manager_y"}).drop("sleeper")

        # create a unique key for matchup history
        matchups = matchups.with_columns((pl.min_horizontal("club_x", "club_y") + "-" + pl.max_horizontal("club_x", "club_y")).alias("key"))

        # store matchup history
        history = matchups.group_by(pl.col("key")).agg([
            (pl.col("winner") == pl.col("club_x")).sum().alias("wins_x"),
            (pl.col("winner") == pl.col("club_y")).sum().alias("wins_y"),
            ((pl.col("winner").is_null()) & (pl.col("score_x").is_not_null()) & (pl.col("score_y").is_not_null())).sum().alias("draws")
        ])

        # filter matchups for season and week with matchup history
        matchup_table = matchups.filter((pl.col("season") == season_id) & (pl.col("week") == week)).join(history, on="key", how="left").drop(["id", "season", "club_x", "club_y", "winner", "stage", "round", "key"])

        return matchup_table

    except Exception as ex:
        raise RuntimeError(f"Error getting matchup table.") from ex