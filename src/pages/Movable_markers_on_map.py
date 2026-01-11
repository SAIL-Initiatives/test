
import pydeck as pdk
import streamlit as st
tabs = st.tabs(['1'])

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








