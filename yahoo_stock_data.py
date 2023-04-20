# This DAG extracts, transforms and loads data for Disney stock daily. 
import yfinance as yf
from datetime import datetime
from typing import Dict
import pandas as pd

def get_stock_data(company: str, start_date: str, end_date: str) -> pd.DataFrame: 
    """
    Returns stock data broken down by minute for argument company and time period. Note that the requested date range i.e. [start_date, end_date] must be within the last 30 days. This is a yahoo finance API restriction.
        Args:
           company: Company for which data is desired (e.g. 'MSFT')
            start_date: Start of time period in format YYYY-MM-DD
            end_date: End of time period in format YYYY-MM-DD
        Returns:
           Stock data as a json object.
    """
    ticker = yf.Ticker(company)

    data = ticker.history(interval='1m', start=start_date, end=end_date)
    
    return data

