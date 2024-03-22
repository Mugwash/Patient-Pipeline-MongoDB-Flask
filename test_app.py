from unittest.mock import MagicMock
from app import get_raw_data,raw_collection
from app import app, get_data, transformed_collection
import pytest
import pandas as pd



def test_get_raw_data_success():
    # Mock the raw_collection.find() method to return some data
    raw_collection.find = MagicMock(return_value=[{'name': 'John', 'age': 25}, {'name': 'Jane', 'age': 30}])
    
    # Call the get_raw_data() function
    result = get_raw_data()
    
    # Assert that the result is an HTML table
    assert '<table' in result
    assert '</table>' in result
    assert 'John' in result
    assert 'Jane' in result


def test_get_data_success():
    # Mock the transformed_collection.find() method to return some data
    transformed_collection.find = MagicMock(return_value=[{'name': 'John', 'age': 25}, {'name': 'Jane', 'age': 30}])
    
    # Call the get_data() function
    result = get_data()
    
    # Assert that the result is an HTML table
    assert '<table' in result
    assert '</table>' in result
    assert 'John' in result
    assert 'Jane' in result