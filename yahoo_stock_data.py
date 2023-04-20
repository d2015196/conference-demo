# This DAG extracts, transforms and loads data for Disney stock daily. 
import yfinance as yf
from datetime import datetime
from typing import Dict

def get_stock_data(company: str, start_date: str, end_date: str) -> Dict: 
    """
    Returns stock data broken down by minute for argument company and time period. Note that the requested date range i.e. [start_date, end_date] must be within the last 30 days. This is a yahoo finance API restriction.
        Args:
           company: Company for which data is desired (e.g. 'MSFT')
            start_date: Start of time period
            end_date: End of time period 
        Returns:
           Stock data as a json object.
    """
    ticker = yf.Ticker(company)

    data = ticker.history(interval='1m', start=start_date, end=end_date)
    
    return data

stock_data = get_stock_data("msft", "2023-04-18", "2023-04-19")

print(stock_data)

