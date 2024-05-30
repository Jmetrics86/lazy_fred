import pytest
from fredapi import Fred

import lazy_fred

# Test `get_and_validate_api_key` with valid/invalid keys
def test_valid_api_key(monkeypatch):  
    # Mock a valid API key
    monkeypatch.setenv("API_KEY", "your_valid_api_key")
    assert get_and_validate_api_key() == "your_valid_api_key"  # noqa: F821

# def test_invalid_api_key(monkeypatch):
#     # Mock an invalid API key or unset it
#     monkeypatch.delenv("API_KEY", raising=False)
#     with pytest.raises(FredAPIException):
#         get_and_validate_api_key()

# # Test `get_series_data` with a small set of series IDs
# def test_get_series_data(valid_api_key):
#     # Replace with a few sample series IDs
#     test_ids = ["DGS10", "DEXUSEU"]  
#     fred = Fred(api_key=valid_api_key)
#     data = get_series_data(fred, test_ids, "D")
#     assert not data.empty  # Check if data was fetched

# # ... (Add more tests to check for error handling, specific columns in data, etc.)
