# package imports
import pytest

# function imports
from unittest.mock import patch, Mock

# local imports
from matchup_history.api.sleeper import get_sleeper_matchups, map_sleeper_ids
from matchup_history.model import SleeperMatchup

# mock data
MOCK_MATCHUP_RESPONSE = [
    {"matchup_id": "1", "players": ["p1", "p2"], "points": 10},
    {"matchup_id": "1", "players": ["p3", "p4"], "points": 20},
    {"matchup_id": "2", "players": ["p5", "p6"], "points": 20},
    {"matchup_id": "2", "players": ["p7", "p8"], "points": 10}
]

MOCK_ROSTER_RESPONSE = [
    {"owner_id": "team_a", "players": ["p1", "p2"]},
    {"owner_id": "team_b", "players": ["p3", "p4"]},
    {"owner_id": "team_c", "players": ["p5", "p6"]},
    {"owner_id": "team_d", "players": ["p7", "p8"]}
]

# helpers
def mock_get_requests(url):
    mock_response = Mock()

    mock_response.json.return_value = MOCK_MATCHUP_RESPONSE if "matchups" in url else MOCK_ROSTER_RESPONSE
    mock_response.raise_for_status = Mock()

    return mock_response

def mock_get_requests_empty_matchups(url):
    mock_response = Mock()
    
    mock_response.json.return_value = [] if "matchups" in url else MOCK_ROSTER_RESPONSE
    mock_response.raise_for_status = Mock()
    
    return mock_response

def mock_get_requests_empty_rosters(url):
    mock_response = Mock()
    
    mock_response.json.return_value = MOCK_MATCHUP_RESPONSE if "matchups" in url else []
    mock_response.raise_for_status = Mock()
    
    return mock_response

# test functions
@patch("requests.get", side_effect=mock_get_requests_empty_matchups)
def test_get_sleeper_matchups_empty_matchup_response(mock_get):
    result = get_sleeper_matchups("TEST", 1)
    assert result is None

@patch("requests.get", side_effect=mock_get_requests_empty_rosters)
def test_get_sleeper_matchups_empty_roster_response(mock_get):
    result = get_sleeper_matchups("TEST", 1)
    assert result is None

@patch("requests.get")
def test_get_sleeper_matchups_http_error(mock_get):
    mock_response = Mock()
    mock_response.raise_for_status.side_effect = Exception("Sleeper API HTTP Status Error.")
    
    mock_get.return_value = mock_response

    with pytest.raises(RuntimeError, match="Querying the Sleeper API failed"):
        get_sleeper_matchups("TEST", 1)

@patch("requests.get", side_effect=mock_get_requests)
def test_get_sleeper_matchups_success(mock_get):
    result = get_sleeper_matchups("TEST", 1)
    
    assert result is not None
    assert isinstance(result, list)
    assert all(isinstance(sleeper_matchup, SleeperMatchup) for sleeper_matchup in result)

    sleeper_ids = [sleeper_matchup.sleeper_id_x for sleeper_matchup in result] + [sleeper_matchup.sleeper_id_y for sleeper_matchup in result]

    assert "team_a" in sleeper_ids and "team_b" in sleeper_ids and "team_c" in sleeper_ids and "team_d" in sleeper_ids

def test_map_sleeper_ids_full_match():
    players_x = {"a", "b", "c"}
    players_y = {"d", "e", "f"}
    
    rosters = [
        ("id_x", {"a", "b", "c", "z"}),
        ("id_y", {"d", "e", "f", "x"})
    ]
    
    result = map_sleeper_ids("test_1", players_x, players_y, rosters)
    assert result == ("id_x", "id_y")

def test_map_sleeper_ids_partial_match_threshold_met():
    players_x = {"a", "b", "c", "d"}
    players_y = {"e", "f", "g", "h"}
    
    rosters = [
        ("id_x", {"a", "b"}),     # 2/4 = 0.5 → meets threshold
        ("id_y", {"e", "f", "g"}) # 3/4 = 0.75 → meets threshold
    ]
    
    result = map_sleeper_ids("test_2", players_x, players_y, rosters, threshold=0.5)
    assert result == ("id_x", "id_y")

def test_map_sleeper_ids_no_match_threshold_not_met():
    players_x = {"a", "b", "c", "d"}
    players_y = {"e", "f", "g", "h"}
    
    rosters = [
        ("id_x", {"a"}),          # 1/4 = 0.25 < 0.5
        ("id_y", {"e", "f"})      # 2/4 = 0.5 == threshold
    ]
    
    with pytest.raises(RuntimeError, match=r"Roster ids were not mapped for matchup_id='.*'. Adjust the threshold."):
        map_sleeper_ids("test_3", players_x, players_y, rosters, threshold=0.5)

def test_map_sleeper_ids_multiple_candidates():
    players_x = {"a", "b"}
    players_y = {"c", "d"}
    
    rosters = [
        ("id_x1", {"a", "b"}), # matches X
        ("id_x2", {"a", "b", "x"}), # also matches X: skip
        ("id_y1", {"c", "d"}), # matches Y
    ]
    
    result = map_sleeper_ids("test_4", players_x, players_y, rosters)
    assert result == ("id_x1", "id_y1")

def test_map_sleeper_ids_no_match():
    players_x = {"a", "b"}
    players_y = {"c", "d"}
    
    rosters = [
        ("id_a", {"x", "y"}),
        ("id_b", {"z"})
    ]
    
    with pytest.raises(RuntimeError, match=r"Roster ids were not mapped for matchup_id='.*'. Adjust the threshold."):
        map_sleeper_ids("test_5", players_x, players_y, rosters)