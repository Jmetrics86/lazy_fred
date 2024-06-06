
from lazy_fred import get_fred_search_results

def test_dummy():
    dummyvariable = 5+5
    assert dummyvariable == 10

def test_get_fred_search_results():
    assert len(get_fred_search_results()) > 0
