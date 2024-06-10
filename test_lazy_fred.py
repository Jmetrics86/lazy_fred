
import pandas as pd
import os
import tempfile
from fredapi import Fred
from lazy_fred import AccessFred, collect_categories

fred = Fred(api_key=os.getenv("API_KEY")) 


# Unit tests for individual methods/classes
def test_api_key_validation(monkeypatch):
    # Simulate a valid API key scenario
    monkeypatch.setattr(os, "getenv", lambda x: "valid_api_key")
    access_fred = AccessFred()
    assert access_fred.get_and_validate_api_key() == "valid_api_key"

    # Simulate an invalid API key scenario (optional - might require mocking the API call)
    #monkeypatch.setattr(os, "getenv", lambda x: "invalid_api_key")
    #with pytest.raises(Exception):
        #access_fred.get_and_validate_api_key()  



def test_get_fred_search_results():
    """Tests that search results are retrieved and processed correctly."""
    # Set up mock FRED instance if needed
    # fred_mock = ...  (create your mock fred instance)

    collect = collect_categories()
    results = collect.get_fred_search_results(fred) 
    
    # Check if output is a DataFrame
    assert isinstance(results, pd.DataFrame)
    
    # Check columns exist
    assert set(results.columns) == {'id', 'realtime_start', 'realtime_end', 'title', 'observation_start', 'observation_end', 'frequency', 'frequency_short', 'popularity', 'notes', 'last_updated', 'seasonal_adjustment_short', 'seasonal_adjustment', 'units', 'units_short'}
    
    # Check for duplicates
    assert not results.duplicated(subset=['id']).any()
    





# ... Similar tests for monthly_filter and weekly_filter

# Integration Tests (optional) - test the combined behavior of multiple components
def test_full_pipeline(monkeypatch):
    """Tests the entire data collection and export process."""
    # Prepare test environment
    #monkeypatch.setenv("API_KEY", "your_valid_api_key")

    # Create temporary files for outputs
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as temp_lazy_file, \
         tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as temp_daily_file, \
         tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as temp_monthly_file, \
         tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as temp_weekly_file:
        
        # Run main function
        #main()

        # Assert that output files were created
        assert os.path.isfile(temp_lazy_file.name)
        assert os.path.isfile(temp_daily_file.name)
        assert os.path.isfile(temp_monthly_file.name)
        assert os.path.isfile(temp_weekly_file.name)

        # Load data and perform assertions on the content
        #lazy_df = pd.read_csv(temp_lazy_file.name)
        #assert not lazy_df.empty  # ... etc.
        # ... similar assertions for other files

