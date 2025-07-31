# package imports
import requests

def get_rosters(sleeper_league_id: str) -> list[dict[str, str]] | None:
    """
    Gets current roster data from the Sleeper API for a Sleeper league.

    Args:
        sleeper_league_id (str): The Sleeper id of the league.

    Returns:
        list[dict[str, str]] | None: A list of dictionaries with Sleeper roster data.
        Returns None if no rosters are returned in the API response.
    """
    try:
        # get roster data from sleeper api
        response = requests.get(f"https://api.sleeper.app/v1/league/{sleeper_league_id}/rosters")
        
        # raise any http error
        response.raise_for_status()

        if not response.json():
            return None

        # map the response data to a list of dictionaries
        rosters = [
            {
                "owner_id": item["owner_id"],
                "win": item["settings"]["wins"],
                "loss": item["settings"]["losses"],
                "draw": item["settings"]["ties"],
                "pf": item["settings"]["fpts"] + item["settings"]["fpts_decimal"] / 100,
                "pa": item["settings"]["fpts_against"] + item["settings"]["fpts_against_decimal"] / 100,
                "mpf": item["settings"]["ppts"] + item["settings"]["ppts_decimal"] / 100
            } for item in response.json()
        ]

        # sort by wins and points for to add standing
        rosters.sort(key=lambda x: (x["win"], x["pf"]), reverse=True)

        for index, roster in enumerate(rosters, start=1):
            roster["standing"] = index
        
        return rosters
    
    except Exception as ex:
        raise RuntimeError(f"Querying the Sleeper API failed for season id: {sleeper_league_id=}.\nException: {ex}")