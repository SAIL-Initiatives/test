import streamlit as st 
import os
import pandas as pd
#import polars as pol

import numpy as np
from supabase import create_client
from datetime import datetime
now = datetime.now()    
st.write( now )

df = pd.read_excel( 'cchs2014.xls', sheet_name=0 )
st.write( df.columns)

supabase = create_client(
    os.environ['SUPABASE_URL'],
    os.environ['SUPABASE_SERVICE_KEY']  # IMPORTANT: use service role key
)

rows = df.to_dict(orient="records")

for i in range(0, len(rows), 500):
    supabase.table("Table").insert(rows[i:i+500]).execute()

