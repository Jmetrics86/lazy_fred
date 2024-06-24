
import pandas as pd
import os
import tempfile
from fredapi import Fred
#import fred
from .lazy_fred import AccessFred, CollectCategories




# Unit tests for individual methods/classes
def test_api_key_validation():
    # Simulate a valid API key scenario
    api_key = os.getenv("API_KEY")
    assert len(api_key) != None



def test_get_fred_search_results():
    """Tests that search results are retrieved and processed correctly."""

    api_key = os.getenv("API_KEY")

    collect = CollectCategories(api_key)
    results = collect.get_fred_search(categories=['gdp', 'banking']) #get_fred_search(self, categories)
    # Check if output is a dictionary
    
    assert isinstance(results, list)
    assert isinstance(results[0], dict)

    results = results[0]
    results_df = pd.DataFrame.from_dict(results)

    # Check columns exist
    assert set(results_df.columns) == {'seriess', 'sort_order', 'order_by', 'offset', 'limit', 'count', 'realtime_start', 'realtime_end'}
    
    # Check for duplicates
    assert not results_df.duplicated(subset=['seriess']).any()
    





# ... Similar tests for monthly_filter and weekly_filter

# Integration Tests (optional) - test the combined behavior of multiple components
def test_full_pipeline():
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

