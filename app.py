import streamlit as st
import pandas as pd
import time
import os
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from botocore.exceptions import ClientError

# Import the optimized DynamoDB client
from aws_client import dynamodb_client

load_dotenv()
st.set_page_config(page_title="DynamoDB Streamlit UI", layout="wide")

# ------------------------------------------------------------------
# State initialization
# ------------------------------------------------------------------
if "log" not in st.session_state:
    st.session_state.log = []
if "table_name" not in st.session_state:
    st.session_state.table_name = ""
if "performance" not in st.session_state:
    st.session_state.performance = {"insert": [], "fetch": [], "query": []}

# ------------------------------------------------------------------
# Helper: High-precision latency measurement
# ------------------------------------------------------------------
def measure_latency(func, *args, **kwargs):
    start = time.perf_counter_ns()
    response = func(*args, **kwargs)
    latency = (time.perf_counter_ns() - start) / 1_000_000
    return response, latency

# ------------------------------------------------------------------
# Cache repeated get_item calls for 5 seconds
# ------------------------------------------------------------------
@st.cache_data(ttl=5)
def cached_get_item(table, key):
    return dynamodb_client.get_item(TableName=table, Key=key)

# ------------------------------------------------------------------
# UI Styling
# ------------------------------------------------------------------
st.markdown("""
<style>
    .main-title {
        font-size: 26px;
        font-weight: 700;
        color: #003366;
        margin-bottom: 20px;
    }
    .card {
        background-color: #f9f9f9;
        padding: 1rem;
        border-radius: 12px;
        box-shadow: 0 4px 10px rgba(0,0,0,0.05);
        margin-bottom: 1rem;
    }
    .metric-card {
        background: linear-gradient(90deg, #1E88E5, #42A5F5);
        color: white;
        padding: 1rem;
        border-radius: 10px;
        text-align: center;
        font-weight: 600;
        margin-top: 10px;
    }
    .stButton>button {
        border-radius: 10px;
        background-color: #0066cc;
        color: white;
        border: none;
        font-weight: 600;
    }
    .stButton>button:hover {
        background-color: #004c99;
    }
</style>
""", unsafe_allow_html=True)

# ------------------------------------------------------------------
# Sidebar + Performance Summary
# ------------------------------------------------------------------
st.sidebar.title("⚙️ DynamoDB Utilities")
st.sidebar.markdown("---")

def avg_latency(lat_list):
    return f"{(sum(lat_list) / len(lat_list)):.2f}" if lat_list else "–"

st.sidebar.subheader("📈 Performance Summary")
st.sidebar.markdown(f"""
| Operation | Avg Latency (ms) |
|-----------|-------------------|
| Insert | {avg_latency(st.session_state.performance['insert'])} |
| Fetch | {avg_latency(st.session_state.performance['fetch'])} |
| Query | {avg_latency(st.session_state.performance['query'])} |
""")

menu = st.sidebar.radio(
    "Select an Action:",
    ["🏗️ Create Table", "🧾 Insert Records", "📦 Fetch Records", "🔍 Query Record", "🗒️ Activity Log"]
)

# ------------------------------------------------------------------
# DynamoDB Operations
# ------------------------------------------------------------------
def create_table(table_name):
    try:
        tables = dynamodb_client.list_tables()["TableNames"]
        if table_name in tables:
            st.warning(f"Table '{table_name}' already exists.")
            return

        dynamodb_client.create_table(
            TableName=table_name,
            KeySchema=[
                {"AttributeName": "id", "KeyType": "HASH"},
                {"AttributeName": "date", "KeyType": "RANGE"}
            ],
            AttributeDefinitions=[
                {"AttributeName": "id", "AttributeType": "S"},
                {"AttributeName": "date", "AttributeType": "N"}
            ],
            BillingMode="PAY_PER_REQUEST"
        )

        dynamodb_client.get_waiter("table_exists").wait(TableName=table_name)
        st.success(f"Created table '{table_name}'.")
        st.session_state.log.append(f"Created table '{table_name}'.")
    except Exception as e:
        st.error(f"Error: {e}")

def insert_records(table_name, num_rows):
    try:
        base_date = datetime(2025, 11, 1, tzinfo=timezone.utc)
        items, batch_count = [], 0
        progress = st.progress(0)
        start = time.perf_counter()

        for i in range(num_rows):
            dt = base_date + timedelta(days=i)
            epoch = int(dt.timestamp())
            iso = dt.strftime("%Y-%m-%dT%H:%M:%SZ")

            items.append({
                "PutRequest": {
                    "Item": {
                        "id": {"S": str(1000 + i)},
                        "date": {"N": str(epoch)},
                        "date_str": {"S": iso},
                        "factory_name": {"S": f"Factory_{(i % 3) + 1}"},
                        "metric": {"S": f"Metric_{i+1}"},
                        "value": {"N": str(i * 10)}
                    }
                }
            })

            # flush batch at 25 items
            if len(items) == 25:
                dynamodb_client.batch_write_item(RequestItems={table_name: items})
                items.clear()
                batch_count += 25
                progress.progress(min(1.0, batch_count / num_rows))

        # insert remaining items
        if items:
            dynamodb_client.batch_write_item(RequestItems={table_name: items})
            batch_count += len(items)

        latency = (time.perf_counter() - start) * 1000
        st.session_state.performance["insert"].append(latency)
        st.success(f"Inserted {num_rows} items in {latency:.2f} ms.")
        st.session_state.log.append(f"Inserted {num_rows} items.")
    except Exception as e:
        st.error(f"Error: {e}")

def fetch_records(table_name):
    try:
        (response, latency) = measure_latency(
            dynamodb_client.scan,
            TableName=table_name,
            Limit=50,
            ProjectionExpression="#d, id, date_str, factory_name, metric, #v",
            ExpressionAttributeNames={"#d": "date", "#v": "value"}
        )

        st.session_state.performance["fetch"].append(latency)

        items = response.get("Items", [])
        if not items:
            st.info("No records found.")
            return

        df = pd.DataFrame([{k: list(v.values())[0] for k, v in item.items()} for item in items])
        st.dataframe(df, use_container_width=True)
        st.success(f"Latency: {latency:.2f} ms")
        st.session_state.log.append(f"Fetched {len(df)} records.")
    except Exception as e:
        st.error(f"Error: {e}")

def query_record_ui():
    st.markdown("### 🔍 Query Record")

    table_name = st.text_input("Enter Table Name:", st.session_state.table_name)
    st.session_state.table_name = table_name

    if not table_name:
        return

    try:
        ids_resp = dynamodb_client.scan(
            TableName=table_name,
            ProjectionExpression="id"
        )
        ids = sorted({item["id"]["S"] for item in ids_resp.get("Items", [])})
        if not ids:
            st.warning("No IDs found.")
            return

        selected_id = st.selectbox("Select ID:", ids)

        date_resp = dynamodb_client.query(
            TableName=table_name,
            KeyConditionExpression="id = :id",
            ExpressionAttributeValues={":id": {"S": selected_id}},
            ProjectionExpression="#d, date_str",
            ExpressionAttributeNames={"#d": "date"}
        )

        dates = sorted(date_resp.get("Items", []), key=lambda x: int(x["date"]["N"]))
        date_map = {item["date_str"]["S"]: item["date"]["N"] for item in dates}
        selected_date = st.selectbox("Select Date:", list(date_map.keys()))
        epoch = date_map[selected_date]

        if st.button("Query Record"):
            key = {"id": {"S": selected_id}, "date": {"N": str(epoch)}}

            (result, latency) = measure_latency(cached_get_item, table_name, key)
            st.session_state.performance["query"].append(latency)

            item = result.get("Item")
            if not item:
                st.warning("Not found")
                return

            item_dict = {k: list(v.values())[0] for k, v in item.items()}

            st.markdown(f"<div class='card'><b>ID:</b> {item_dict['id']}<br><b>Date:</b> {item_dict['date_str']}</div>", unsafe_allow_html=True)
            st.markdown(f"<div class='metric-card'>Latency: {latency:.2f} ms</div>", unsafe_allow_html=True)

            st.session_state.log.append(f"Queried record ({selected_id}, {selected_date}).")

    except Exception as e:
        st.error(f"Error: {e}")

def activity_log():
    st.subheader("🗒️ Activity Log")
    for entry in reversed(st.session_state.log):
        st.markdown(f"<div class='card'>{entry}</div>", unsafe_allow_html=True)

# ------------------------------------------------------------------
# Main Navigation
# ------------------------------------------------------------------
if menu == "🏗️ Create Table":
    st.markdown("<div class='main-title'>🏗️ Create DynamoDB Table</div>", unsafe_allow_html=True)
    table = st.text_input("Table Name:", st.session_state.table_name)
    if st.button("Create Table"):
        create_table(table)

elif menu == "🧾 Insert Records":
    st.markdown("<div class='main-title'>🧾 Insert Records</div>", unsafe_allow_html=True)
    table = st.text_input("Table Name:", st.session_state.table_name)
    num = st.number_input("Number of Records:", min_value=1, value=10)
    if st.button("Insert Records"):
        insert_records(table, num)

elif menu == "📦 Fetch Records":
    st.markdown("<div class='main-title'>📦 Fetch Records</div>", unsafe_allow_html=True)
    table = st.text_input("Table Name:", st.session_state.table_name)
    if st.button("Fetch Records"):
        fetch_records(table)

elif menu == "🔍 Query Record":
    query_record_ui()

elif menu == "🗒️ Activity Log":
    activity_log()
