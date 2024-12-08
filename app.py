import streamlit as st
import pandas as pd
from datetime import datetime, timedelta, date, time
import pytz
from dataManager import calculate_pnl

class TradingSimulator:
    def __init__(self):
        if 'trades' not in st.session_state:
            st.session_state.trades = []

        self.timezone = pytz.timezone('Asia/Kolkata')
        
        if 'start_time' not in st.session_state:
            st.session_state.start_time = datetime.now().replace(hour=9, minute=30, second=0, microsecond=0)
        if 'end_time' not in st.session_state:
            st.session_state.end_time = datetime.now().replace(hour=15, minute=25, second=0, microsecond=0)
        

        self.stocks = ['ASIANPAINT-EQ','BRITANNIA-EQ', 'CIPLA-EQ', 'NESTLEIND-EQ', 'GRASIM-EQ', 'HEROMOTOCO-EQ', 'HINDALCO-EQ', 'HINDUNILVR-EQ',
             'ITC-EQ', 'TRENT-EQ', 'LT-EQ', 'M&M-EQ', 'RELIANCE-EQ', 'TATACONSUM-EQ', 'TATAMOTORS-EQ', 'TATASTEEL-EQ', 'WIPRO-EQ', 'APOLLOHOSP-EQ',
             'DRREDDY-EQ', 'TITAN-EQ', 'SHRIRAMFIN-EQ', 'SBIN-EQ', 'BPCL-EQ', 'BEL-EQ', 'KOTAKBANK-EQ', 'INFY-EQ', 'BAJFINANCE-EQ', 'ADANIENT-EQ',
             'SUNPHARMA-EQ', 'JSWSTEEL-EQ', 'TCS-EQ', 'HDFCBANK-EQ', 'ICICIBANK-EQ', 'POWERGRID-EQ', 'MARUTI-EQ', 'INDUSINDBK-EQ', 'AXISBANK-EQ', 'HCLTECH-EQ',
             'ONGC-EQ', 'NTPC-EQ', 'COALINDIA-EQ', 'BHARTIARTL-EQ', 'TECHM-EQ', 'ADANIPORTS-EQ', 'HDFCLIFE-EQ', 'SBILIFE-EQ', 'ULTRACEMCO-EQ', 'BAJAJ-AUTO-EQ', 'BAJFINANCE-EQ'
             ]
    
    def render_ui(self):
        st.title('Stock')
        results = []
        
        selected_stock = st.selectbox('Select Stock', self.stocks)
        col1, col2 = st.columns(2)

        with col1:
            start_date = st.date_input('Start Date', 
                # value=st.session_state.start_time.date(),
            
                value=date(2024, 1, 1),
                min_value=date(2024, 1, 1),
                max_value=date(2024, 11, 29))
            
            start_time = st.time_input('Start Time', 
                value=st.session_state.start_time.time(),
                key='start_time_input', step=300)
            
            if start_time < time(9, 15) or start_time > time(15, 25):
                st.error("Start time must be between 09:15 and 15:25")
        
        with col2:
            # end_date = st.date_input('End Date', 
            #     value=st.session_state.end_time.date())
            
            end_date = st.date_input('End Date', 
                value=start_date, 
                disabled=True)
            
            end_time = st.time_input('End Time', 
                value=st.session_state.end_time.time(),
                key='end_time_input', step=300)
        
        amount = st.number_input('Investment Amount (₹)', 
            min_value=1, value=10000, step=1000)
        
        direction = st.radio('Trade Direction', ['Long', 'Short'])
        
        col3, col4 = st.columns(2)
        with col3:
            if st.button('Add Trade'):
                trade = {
                    'id': len(st.session_state.trades),
                    'symbol': selected_stock,
                    'start_time': pd.Timestamp(f"{start_date} {start_time}"),
                    'end_time': pd.Timestamp(f"{end_date} {end_time}"),
                    'amount': amount,
                    'direction': direction
                }
                st.session_state.trades.append(trade)
                st.success(f"Trade for {selected_stock} added to queue!")
        
        with col4:
            is_time_valid = (
                start_time >= time(9, 15) and 
                start_time <= time(15, 25) and
                end_time >= start_time and
                end_time <= time(15, 25)
            )
    
            disabled = not is_time_valid

            if st.button('Calculate PnL', disabled=disabled):
                if is_time_valid:
                    results = calculate_pnl(st.session_state.trades)
                else:
                    st.error("Invalid time selection. Please select times between 09:15 and 15:25, with end time not before start time.")
                        
            if results:
                results_df = pd.DataFrame(results)
                st.subheader('PnL Calculation Results')
                st.dataframe(results_df)
                print(results_df)
                
                total_pnl = results_df['pnl'].sum()
                st.metric('Total Portfolio PnL', 
                    f'₹ {total_pnl:,.2f}', 
                    delta_color='off')
            else:
                st.warning("No trades to calculate or data fetch failed.")
                        
            st.subheader('Current Trade Queue')
        
        if st.session_state.trades:
            queue_df = pd.DataFrame(st.session_state.trades)
            
            queue_df['Remove'] = False
            
            edited_df = st.data_editor(
                queue_df, 
                column_config={
                    'Remove': st.column_config.CheckboxColumn(
                        "Remove",
                        default=False
                    )
                },
                disabled=['id', 'stock', 'start_date', 'end_date', 'amount', 'direction']
            )
            
            if st.button('Remove Selected Trades'):
                st.session_state.trades = [
                    trade for trade in st.session_state.trades 
                    if not edited_df.loc[edited_df['id'] == trade['id'], 'Remove'].iloc[0]
                ]
                st.rerun()

def main():
    simulator = TradingSimulator()
    simulator.render_ui()

if __name__ == '__main__':
    main()