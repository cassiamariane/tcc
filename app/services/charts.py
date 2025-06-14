import streamlit as st

def plot_sector_bar_chart(df):
    grouped = df.groupby(['Setor']).size()
    st.bar_chart(grouped, height=500, use_container_width=True)