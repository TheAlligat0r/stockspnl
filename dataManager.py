import boto3
import pandas as pd
from typing import List, Dict
from datetime import datetime
import os
from io import BytesIO
import streamlit as st

class OHLCVDataManager:
    # def __init__(self, data_directory: str):
        # self.data_directory = data_directory

    def __init__(self, bucket_name: str, aws_access_key: str, aws_secret_key: str, region: str):
        
        self.bucket_name = bucket_name
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=region
        )
        self.data_cache: Dict[str, pd.DataFrame] = {}

    def _get_s3_key(self, symbol: str, date: datetime.date) -> str:
        print(f"{symbol}-/{date.strftime('%Y/%m/%d')}.parquet")
        return f"{symbol}-/{date.strftime('%Y/%m/%d')}.parquet"
    
    # def _get_parquet_path(self, symbol: str, date: datetime.date) -> str:

    #     filename = f"{symbol}-{date.strftime('%Y-%m-%d')}.parquet"
    #     return os.path.join(self.data_directory, filename)
    
    def load_ohlcv_data(self, symbol: str, date: datetime.date) -> pd.DataFrame:

        # Create a unique cache key combining symbol and date
        cache_key = f"{symbol}_{date.strftime('%Y-%m-%d')}"
        
        if cache_key not in self.data_cache:
            # parquet_path = self._get_parquet_path(symbol, date)
            s3_key = self._get_s3_key(symbol, date)

            try:
                obj = self.s3_client.get_object(Bucket=self.bucket_name, Key=s3_key)
                df = pd.read_parquet(BytesIO(obj['Body'].read()))
                self.data_cache[cache_key] = df
            except Exception as e:
                raise FileNotFoundError(f"Error fetching data for {symbol} on {date}: {e}")

            # if not os.path.exists(parquet_path):
            #     raise FileNotFoundError(f"No data found for {symbol} on {date}")
            
            # df = pd.read_parquet(parquet_path)

            self.data_cache[cache_key] = df
        
        return self.data_cache[cache_key]
    
    def fetch_parquet_pnl(self, trade_queue: List[Dict]) -> List[Dict]:

        results = []
        
        # Group trades by symbol and date to minimize data loading
        trades_by_symbol_date = {}
        for trade in trade_queue:
            symbol = trade['symbol']
            start_date = trade['start_time'].date()
            
            if (symbol, start_date) not in trades_by_symbol_date:
                trades_by_symbol_date[(symbol, start_date)] = []
            trades_by_symbol_date[(symbol, start_date)].append(trade)


        for (symbol, date), dict_grouped_trades in trades_by_symbol_date.items():
            try:
                ohlcv_data = self.load_ohlcv_data(symbol, date)
                
                for trade in dict_grouped_trades:

                    trade_data = ohlcv_data.loc[[trade['start_time'], trade['end_time']]]
                    
                    if trade_data.empty:
                        raise ValueError(f"No data found for the specified time range")

                    entry_price = trade_data.iloc[0]['close']
                    exit_price = trade_data.iloc[-1]['close']
                    number_shares = trade['amount'] // trade_data.iloc[0]['open']
                    
                    if trade['direction'] == 'long':
                        pnl = (exit_price - entry_price) * number_shares
                    else:
                        pnl = (entry_price - exit_price) * number_shares
                    # pnl = (exit_price - entry_price) * number_shares if trade['direction'] == 'long' else (entry_price - exit_price) * number_shares

                    trade_result = trade.copy()
                    trade_result['pnl'] = pnl
                    trade_result['#shares'] = number_shares
                    results.append(trade_result)
            
            except Exception as e:

                for trade in dict_grouped_trades:
                    error_result = trade.copy()
                    error_result['pnl'] = None
                    error_result['error'] = str(e)
                    results.append(error_result)
        
        return results

def calculate_pnl(trade_queue):
    # data_manager = OHLCVDataManager('D:/Python venv/Streamlit/Parquet')
    data_manager = OHLCVDataManager(
        bucket_name="stockpnl",
        aws_access_key=st.secrets['access_key'],
        aws_secret_key=st.secrets['secret_key'],
        region=st.secrets['region'],
    )

    results = data_manager.fetch_parquet_pnl(trade_queue)
    return results
    