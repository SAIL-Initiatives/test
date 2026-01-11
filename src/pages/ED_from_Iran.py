
import streamlit as st 
import os
import pandas as pd
#import polars as pol

import numpy as np
from supabase import create_client
from datetime import datetime
now = datetime.now()    
st.write( now )

import pandas as pd
import psycopg2

# === phygital  
import pydeck as pdk

st.set_page_config(layout="wide")
st.markdown( 'Dataset from https://doi.org/10.1016/j.dib.2024.110827')

def infer_pg_type(series: pd.Series) -> str:
    # 1. Handle Integers (Check for 64-bit/BigInt)
    if pd.api.types.is_integer_dtype(series):
        if series.max() > 2147483647 or series.min() < -2147483648:
            return "bigint"
        return "integer"
    
    # 2. Handle Floating Point
    if pd.api.types.is_float_dtype(series):
        return "double precision"
    
    # 3. Handle Booleans
    if pd.api.types.is_bool_dtype(series):
        return "boolean"
    
    # 4. Handle Datetime and Timedelta (Intervals)
    if pd.api.types.is_datetime64_any_dtype(series):
        return "timestamptz"
    if pd.api.types.is_timedelta64_dtype(series):
        return "interval"  
        
    # 5. Handle Objects (Strings, UUIDs, JSON)
    if pd.api.types.is_object_dtype(series):
        # Sample first non-null value for more specific inference
        first_val = series.dropna().iloc[0] if not series.dropna().empty else None        
        if isinstance(first_val, (dict, list)):
            return "jsonb"  # Best practice for structured data        
        # Basic check for UUID strings (8-4-4-4-12 pattern)
        if isinstance(first_val, str) and len(first_val) == 36 and first_val.count('-') == 4:
            return "uuid"            
    return "text"  # Default for everything else
    
def report_types( df ):
    for i, col in enumerate(df.columns):
        pg_type = infer_pg_type(df[col])
        st.write( f"\"{col} {pg_type},\"" )     
def insert(table_id, df):
    n=5000
    rows = df.to_dict(orient="records")            
    st.markdown( f'Inserting into {table_id}')
    n=5000
    for i in range(0, df.shape[0], n):
        st.html( '.' )
        st.write( rows[i] )
        supabase.table( table_id ).insert(rows[i:i+n]).execute()
    st.write('done')

supabase = create_client(
    os.environ['SUPABASE_URL'],
    os.environ['SUPABASE_SERVICE_KEY']  # IMPORTANT: use service role key
)
tabs = st.tabs( ['SQL'] )
  
with tabs[0]: 
    try:
        for tid in ['iran_ed_admission','iran_ed_triage']:            
            response = supabase.table( tid ).select("*").execute()
            res = response.data
            st.write( tid )
            st.dataframe( pd.DataFrame(res).sample(10) )         
    except Exception as e:
        st.markdown('# Read from source')
        dfs={}
        c=0
        dfs[c] = pd.read_csv( '../data/iran_ed/ED_admission.csv', index_col=[0] )
        dfs[c].replace({np.nan: None}, inplace=True) 
        c+=1
        dfs[c] = pd.read_csv( '../data/iran_ed/ED_triage.csv', index_col=[0] )
        dfs[c].replace({np.nan: None}, inplace=True)         
        tids = ['admissions','triage']
        for c in [0,1]:
            st.write( tids[c] )
            report_types(dfs[c])
        for c in [0,1]:    
            try:
                insert(tids[c], dfs[c])
            except Exception as e:
                st.write( e )
