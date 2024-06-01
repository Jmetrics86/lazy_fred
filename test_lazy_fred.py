import pandas as pd
from lazy_fred import get_and_validate_api_key, get_fred_search_results
import pytest
from unittest.mock import patch
from hypothesis import given
from hypothesis.strategies import text, lists
import logging

# ... your project's imports ...


logger = logging.getLogger(__name__)
logging.basicConfig(filename='app.log', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# Mocking FRED API for isolation
@pytest.fixture
def mock_fred():
    with patch("your_project.fred") as mock_fred:
        yield mock_fred

# Testing get_and_validate_api_key
@pytest.mark.parametrize(
    "env_api_key, input_api_key, expected_result",
    [
        ("valid_key", None, "valid_key"),
        (None, "valid_key", "valid_key"),
        (None, None, None),  # Should raise an error 
    ],
)
def test_get_and_validate_api_key(
    mock_fred, monkeypatch, env_api_key, input_api_key, expected_result
):
    # Mock environment variables
    monkeypatch.setenv("API_KEY", env_api_key)

    # Mock user input if needed
    if input_api_key is not None:
        monkeypatch.setattr("builtins.input", lambda _: input_api_key)

    # Mock FRED API response
    mock_fred.search.return_value = pd.DataFrame()  # Valid response

    if expected_result is None:
        with pytest.raises(Exception):  # Expect an error
            get_and_validate_api_key()
    else:
        result = get_and_validate_api_key()
        assert result == expected_result

# Property-based testing for search_categories
@given(lists(text(), min_size=1))
def test_search_loop(mock_fred, categories):
    # Mock API response (you can vary the response based on categories)
    mock_fred.search.return_value = pd.DataFrame(
        {"id": ["series1", "series2"], "popularity": [55, 48]}
    )

    # Call your search loop logic
    result_df = get_fred_search_results(categories)

    # Assertions (check for duplicates, data types, etc.)
    assert result_df.shape[0] == len(categories) * 2  # Each category returns 2 series
    assert result_df["popularity"].dtype == int
    assert not result_df.duplicated(subset=["id"]).any()  # No duplicate series
