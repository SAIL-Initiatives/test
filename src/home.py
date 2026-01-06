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

tabs = st.tabs( ['Phygital', 'SQL'] )
with tabs[0]:
    # ---------------------------
    # 1. Public / simulated data
    # ---------------------------
    
    if "buildings" not in st.session_state:
        st.session_state.buildings = pd.DataFrame([
            {"id": 1, "name": "Hospital", "lat": 49.2827, "lon": -123.1207},
            {"id": 2, "name": "School",   "lat": 49.2750, "lon": -123.1300},
            {"id": 3, "name": "Shelter",  "lat": 49.2900, "lon": -123.1000},
        ])
    
    df = st.session_state.buildings
    
    # ---------------------------
    # 2. Interaction panel
    # ---------------------------
    
    st.header("Interact with the Map")
    
    selected_id = st.selectbox(
        "Select building (physical block)",
        df["id"]
    )
    
    row = df[df["id"] == selected_id].iloc[0]
    
    new_lat = st.slider(
        "Latitude",
        min_value=49.25,
        max_value=49.32,
        value=float(row["lat"]),
        step=0.0005
    )
    
    new_lon = st.slider(
        "Longitude",
        min_value=-123.18,
        max_value=-123.05,
        value=float(row["lon"]),
        step=0.0005
    )
    
    if st.button("Move building"):
        df.loc[df["id"] == selected_id, ["lat", "lon"]] = [new_lat, new_lon]
        st.session_state.buildings = df
    
    # ---------------------------
    # 3. Map visualization
    # ---------------------------
    
    layer = pdk.Layer(
        "ScatterplotLayer",
        data=df,
        get_position="[lon, lat]",
        get_radius=120,
        pickable=True,
        get_fill_color="[200, 30, 0, 160]"
    )
    
    view_state = pdk.ViewState(
        latitude=49.2827,
        longitude=-123.1207,
        zoom=12,
        pitch=0
    )
    
    deck = pdk.Deck(
        layers=[layer],
        initial_view_state=view_state,
        tooltip={"text": "{name} (ID: {id})"}
    )
    
    st.pydeck_chart(deck)
    
    # ---------------------------
    # 4. Digital twin state
    # ---------------------------
    
    st.subheader("Digital Twin State")
    st.dataframe(df, use_container_width=True)















with tabs[1]: 
    
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
