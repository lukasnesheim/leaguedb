# package imports
import polars as pl
import pytest

# function imports
from unittest.mock import Mock

# local imports
from matchup_history.db.supabase import get_club_lookup, get_matchups, upsert_matchups

# helpers
class MockResponse:
    def __init__(self, data):
        self.data = data

# test functions
def test_get_club_lookup_success():
    mock_client = Mock()
    mock_response = MockResponse(data=[{"id": "id1", "external": "ext1"}, {"id": "id2", "external": "ext2"}])

    mock_client.table.return_value.select.return_value.execute.return_value = mock_response

    result = get_club_lookup(mock_client)
    
    assert result == {"ext1": "id1", "ext2": "id2"}

def test_get_club_lookup_empty():
    mock_client = Mock()
    mock_response = MockResponse(data=[])

    mock_client.table.return_value.select.return_value.execute.return_value = mock_response

    result = get_club_lookup(mock_client)
    
    assert result == None

def test_get_club_lookup_exception():
    mock_client = Mock()
    mock_client.table.side_effect = Exception("Supabase .table() Error")

    with pytest.raises(RuntimeError, match="Querying the Supabase database for club ids failed."):
        get_club_lookup(mock_client)

def test_get_matchups_with_season():
    mock_client = Mock()
    mock_response = MockResponse(data=[{"id": 1, "season": "2023"}])

    mock_client.table.return_value.select.return_value.eq.return_value.execute.return_value = mock_response

    result = get_matchups(mock_client, season_id="2023")
    
    assert isinstance(result, pl.DataFrame)
    assert not result.is_empty()

def test_get_matchups_without_season():
    mock_client = Mock()
    mock_response = MockResponse(data=[{"id": 1, "season": "2023"}, {"id": 2, "season": "2024"}])

    mock_client.table.return_value.select.return_value.execute.return_value = mock_response

    result = get_matchups(mock_client)

    assert isinstance(result, pl.DataFrame)
    assert len(result) == 2

def test_get_matchups_empty():
    mock_client = Mock()
    mock_response = MockResponse(data=[])

    mock_client.table.return_value.select.return_value.execute.return_value = mock_response

    result = get_matchups(mock_client)

    assert result is None

def test_get_matchups_exception():
    mock_client = Mock()
    mock_client.table.side_effect = Exception("Supabase .table() Error")

    with pytest.raises(RuntimeError, match="Querying the Supabase database to get matchups failed"):
        get_matchups(mock_client, season_id="2023")

def test_upsert_matchups_success():
    mock_client = Mock()
    mock_response = MockResponse(data=[{"id": 1, "season": "2023", "club_x": "a", "club_y": "b", "week": 1}, {"id": 2, "season": "2024", "club_x": "a", "club_y": "b", "week": 1}])

    mock_client.table.return_value.upsert.return_value.execute.return_value = mock_response

    dummy_matchups = [{"season": "2023", "club_x": "a", "club_y": "b", "week": 1}]
    result = upsert_matchups(mock_client, dummy_matchups)

    assert isinstance(result, pl.DataFrame)
    assert len(result) == 2

def test_upsert_matchups_empty():
    mock_client = Mock()
    mock_response = MockResponse(data=[])

    mock_client.table.return_value.upsert.return_value.execute.return_value = mock_response

    dummy_matchups = [{"season": "2023", "club_x": "a", "club_y": "b", "week": 1}]
    result = upsert_matchups(mock_client, dummy_matchups)

    assert result is None

def test_upsert_matchups_exception():
    mock_client = Mock()
    mock_client.table.return_value.upsert.side_effect = Exception("Supabase .table() Error")

    with pytest.raises(RuntimeError, match="Querying the Supabase database to upsert matchups failed."):
        upsert_matchups(mock_client, [{"season": "2023"}])