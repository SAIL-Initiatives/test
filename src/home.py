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

def infer_pg_type(series: pd.Series) -> str:
    if pd.api.types.is_integer_dtype(series):
        return "integer"
    if pd.api.types.is_float_dtype(series):
        return "double precision"
    if pd.api.types.is_bool_dtype(series):
        return "boolean"
    if pd.api.types.is_datetime64_any_dtype(series):
        return "timestamptz"
    return "text"
 

df = pd.read_csv( '../data/nhanes_before.csv',  )
df = df.replace({np.nan: None}) 
st.dataframe( df.sample(10)) 
st.write( df.shape ) 

for col in df.columns:
    pg_type = infer_pg_type(df[col])
    st.write( col, pg_type)

supabase = create_client(
    os.environ['SUPABASE_URL'],
    os.environ['SUPABASE_SERVICE_KEY']  # IMPORTANT: use service role key
)

rows = df.to_dict(orient="records")

for i in range(0, len(rows), 500):
    st.html( '.' )
    supabase.table("nhanes").insert(rows[i:i+500]).execute()

