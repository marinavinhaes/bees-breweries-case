# src/tests/test_fetcher.py
import json
import os
import tempfile
from unittest.mock import patch, MagicMock
from fetcher import write_bronze, fetch_all_breweries

def test_write_bronze_creates_file():
    data = [{"id": "1", "name": "Test Brewery"}]
    tmpdir = tempfile.mkdtemp()
    path = write_bronze(data, tmpdir)
    assert os.path.exists(path)
    with open(path, "r") as f:
        loaded = json.load(f)
    assert loaded == data

@patch("fetcher.requests.get")
def test_fetch_all_breweries_pagination(mock_get):
    # simulate two pages then empty
    mock_get.side_effect = [
        MagicMock(status_code=200, json=lambda: [{"id":"1"}]),
        MagicMock(status_code=200, json=lambda: [{"id":"2"}]),
        MagicMock(status_code=200, json=lambda: [])
    ]
    res = fetch_all_breweries(per_page=1, max_pages=10)
    assert len(res) == 2