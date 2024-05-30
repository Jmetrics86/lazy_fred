from lazy_fred import get_and_validate_api_key
from fredapi import Fred

def test_valid_api_key(monkeypatch):
    # Mock a valid API key and FRED instance
    monkeypatch.setenv("API_KEY", "your_valid_api_key")
    fred = Fred(api_key="your_valid_api_key")  # Create a Fred instance

    # Call the function with the Fred instance
    result = get_and_validate_api_key(fred)  
    assert result == "your_valid_api_key"
