import streamlit as st
import requests

st.title("📊 Fault-Tolerant Data System")

st.subheader("Send Event")

source = st.text_input("Client ID", "client_A")
metric = st.text_input("Metric", "sales")
amount = st.text_input("Amount", "1200")
timestamp = st.text_input("Timestamp", "2024/01/01")

simulate_fail = st.checkbox("Simulate Failure")

if st.button("Send Event"):
    event = {
        "source": source,
        "payload": {
            "metric": metric,
            "amount": amount,
            "timestamp": timestamp
        }
    }

    if simulate_fail:
        st.error("Simulated failure")
    else:
        res = requests.post("http://127.0.0.1:5000/ingest", json=event)
        st.success(res.json())

st.subheader("📈 Aggregated Results")

if st.button("Get Aggregation"):
    res = requests.get("http://127.0.0.1:5000/aggregate")
    st.json(res.json())
