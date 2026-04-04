import streamlit as st

st.set_page_config(page_title="ESG Signal", page_icon="🟢", layout="wide")
st.title("ESG Signal")
st.info("Pipeline not yet connected. Agents coming in M2.")

ticker = st.text_input("Enter ticker (e.g. BP, MSFT)")
if ticker:
    st.write(f"Will analyse: **{ticker}**")
