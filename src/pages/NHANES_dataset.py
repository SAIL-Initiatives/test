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


st.set_page_config(layout="wide")


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
        return "interval"  #
    
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

supabase = create_client(
    os.environ['SUPABASE_URL'],
    os.environ['SUPABASE_SERVICE_KEY']  # IMPORTANT: use service role key
)

tabs = st.tabs( ['SQL'] )

with tabs[0]: 
    
    st.markdown('## Original CSV database')
    df = pd.read_csv( '../data/nhanes_before.csv', index_col=[0] )
    df = df.replace({np.nan: None}) 
    st.dataframe( df.sample(100) ) 
    st.write( df.shape )
    
    
    st.markdown('## Augmented database hosted on Supabase')
    response = supabase.table("nhanes").select("*").order("UID", desc=True).execute()
    res = response.data
    
    if res:
        res = pd.DataFrame( res )
        st.write( res.shape )
    
        st.dataframe( res )
        if 0:
            for i,r in enumerate(rows):                
                st.markdown(f"{r['UID']} {r['Gender']} {r['Age_y']} {r['Ethnicity']} {r['Red_edu']} {r['Ref_marital']} {r['Smoke_home']}")
                #st.html( '<hr>')
                if i>10:
                    break 
    else:     
        for i, col in enumerate(df.columns):
            pg_type = infer_pg_type(df[col])
            st.write( i, col, pg_type)       
        
        rows = df.to_dict(orient="records")    
        n=5000
        for i in range(0, len(rows), n):
            st.html( '.' )
            st.write( rows[i] )
            supabase.table("nhanes").insert(rows[i:i+n]).execute()
    
    
    #new_file = getUserFile()
    #data = supabase.storage.from_(bucket_name).upload("/user1/profile.png", new_file)
    
    
    # .explain() feature is disabled by default on the PostgREST server, to enable, issue SQL:
    #
    #     ALTER ROLE authenticator SET pgrst.db_plan_enabled = true;
    #     NOTIFY pgrst, 'reload config';
    
    response = (
        supabase.table("nhanes")
        .select("*")
        .explain()
        .execute()
    )
    
    st.markdown( '# Explain' )
    st.write(response)
    
    
    st.markdown('# ')
    response = supabase.table("nhanes").select("Age_y","Ethnicity","Gender","Ref_marital","Red_edu", "Smoke_home").eq("Gender", "Male").order("UID").execute()
    df2 = pd.DataFrame(response.data)
    st.dataframe(df2, use_container_width=True)
    
    response = supabase.table("nhanes").select("Age_y","Ethnicity","Gender","Ref_marital","Red_edu", "Smoke_home").eq("Gender", "Female").order("UID").execute()
    df1 = pd.DataFrame(response.data)
    st.dataframe(df1, use_container_width=True)
