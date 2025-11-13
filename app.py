import streamlit as st
import boto3
import pandas as pd
import time
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key

# ---------------------------------------
# LOAD ENVIRONMENT
# ---------------------------------------
load_dotenv()

# Streamlit page setup
st.set_page_config(page_title="DynamoDB Streamlit UI", layout="wide")

# ---------------------------------------
# DYNAMODB CLIENT (optimized)
# ---------------------------------------
if "dynamodb_client" not in st.session_state:
    st.session_state.dynamodb_client = boto3.client(
        "dynamodb",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION")
    )

dynamodb_client = st.session_state.dynamodb_client

# Warm up connection once (reduces latency)
try:
    dynamodb_client.list_tables()
except Exception:
    pass

# Persistent session variables
if "table_name" not in st.session_state:
    st.session_state.table_name = ""

if "log" not in st.session_state:
    st.session_state.log = []


# ---------------------------------------
# CUSTOM STYLING
# ---------------------------------------
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
            padding: 1.2rem;
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


# ---------------------------------------
# SIDEBAR NAVIGATION
# ---------------------------------------
st.sidebar.title("⚙️ DynamoDB Utilities")
st.sidebar.markdown("---")
menu = st.sidebar.radio(
    "Select an Action:",
    ["🏗️ Create Table", "🧾 Insert Records", "📦 Fetch Records", "🔍 Query Record", "🗒️ Activity Log"]
)


# ---------------------------------------
# CREATE TABLE
# ---------------------------------------
def create_table(table_name):
    try:
        tables = dynamodb_client.list_tables().get("TableNames", [])
        if table_name in tables:
            st.warning(f"⚠️ Table '{table_name}' already exists.")
            return

        dynamodb_client.create_table(
            TableName=table_name,
            KeySchema=[
                {"AttributeName": "id", "KeyType": "HASH"},   # Partition key
                {"AttributeName": "date", "KeyType": "RANGE"} # Sort key
            ],
            AttributeDefinitions=[
                {"AttributeName": "id", "AttributeType": "S"},
                {"AttributeName": "date", "AttributeType": "S"}
            ],
            BillingMode="PAY_PER_REQUEST"
        )
        st.success(f"✅ Table '{table_name}' created successfully!")
        st.session_state.log.append(f"✅ Created table '{table_name}'.")
    except Exception as e:
        st.error(f"❌ Error creating table: {e}")
        st.session_state.log.append(f"❌ Create table failed: {e}")


# ---------------------------------------
# INSERT RECORDS
# ---------------------------------------
def insert_records(table_name, num_rows):
    try:
        start_time = time.perf_counter_ns()

        # Generate valid dates for each record
        base_date = datetime(2025, 11, 1)
        with st.spinner("Inserting records..."):
            for i in range(num_rows):
                date_str = (base_date + timedelta(days=i)).strftime("%Y-%m-%dT%H:%M:%SZ")
                item = {
                    "id": {"S": f"{1000 + i}"},  # e.g., id = 1000, 1001, ...
                    "date": {"S": date_str},
                    "factory_name": {"S": f"Factory_{(i % 3) + 1}"},
                    "metric": {"S": f"Metric_{i+1}"},
                    "value": {"N": str(i * 10)}
                }
                dynamodb_client.put_item(TableName=table_name, Item=item)

        end_time = time.perf_counter_ns()
        latency_ms = (end_time - start_time) / 1_000_000
        st.success(f"✅ Inserted {num_rows} records successfully in {latency_ms:.2f} ms.")
        st.session_state.log.append(f"✅ Inserted {num_rows} into '{table_name}'.")
    except Exception as e:
        st.error(f"❌ Error inserting records: {e}")
        st.session_state.log.append(f"❌ Insert failed: {e}")


# ---------------------------------------
# FETCH RECORDS
# ---------------------------------------
def fetch_records(table_name):
    """Scan and display all records with latency info."""
    try:
        start = time.perf_counter_ns()
        response = dynamodb_client.scan(
            TableName=table_name,
            ProjectionExpression="#d, id, date_str, factory_name, metric, #v",
            ExpressionAttributeNames={
                "#d": "date",
                "#v": "value"
            }
        )
        end = time.perf_counter_ns()
        latency_ms = (end - start) / 1_000_000

        items = response.get("Items", [])
        if not items:
            st.info("No records found.")
            return

        df = pd.DataFrame([{k: list(v.values())[0] for k, v in item.items()} for item in items])
        df["date"] = df["date"].astype(str)
        st.markdown("### 📊 Fetched Data")
        st.dataframe(df, use_container_width=True)
        st.markdown(f"**⏱️ Latency:** {latency_ms:.2f} ms")
        st.session_state.log.append(f"✅ Fetched {len(df)} records from '{table_name}' in {latency_ms:.2f} ms.")
    except Exception as e:
        st.error(f"❌ Error fetching records: {e}")
        st.session_state.log.append(f"❌ Fetch failed: {e}")



# ---------------------------------------
# QUERY RECORD
# ---------------------------------------
def query_record_ui():
    st.markdown("### 🔍 Query a Specific Record")

    table_name = st.text_input("Enter Table Name:", st.session_state.table_name)
    st.session_state.table_name = table_name

    if not table_name:
        st.info("Please enter a table name to continue.")
        return

    try:
        # Get all ids
        response = dynamodb_client.scan(TableName=table_name, ProjectionExpression="id")
        ids = sorted({item["id"]["S"] for item in response.get("Items", [])})
        if not ids:
            st.warning("⚠️ No IDs found in the table.")
            return

        selected_id = st.selectbox("Select ID:", ids)

        # Get dates for the selected ID
        query_resp = dynamodb_client.query(
    TableName=table_name,
    KeyConditionExpression="id = :id",
    ExpressionAttributeValues={":id": {"S": selected_id}},
    ProjectionExpression="#d, date_str",
    ExpressionAttributeNames={"#d": "date"}
)

        dates = sorted({item["date"]["S"] for item in query_resp.get("Items", [])})

        if not dates:
            st.warning("⚠️ No dates found for this ID.")
            return

        selected_date = st.selectbox("Select Date:", dates)

        if st.button("Query Record"):
            start = time.perf_counter_ns()
            result = dynamodb_client.get_item(
                TableName=table_name,
                Key={"id": {"S": selected_id}, "date": {"S": selected_date}}
            )
            end = time.perf_counter_ns()
            latency_ms = (end - start) / 1_000_000

            item = result.get("Item")
            if not item:
                st.warning("⚠️ Record not found.")
                return

            item_dict = {k: list(v.values())[0] for k, v in item.items()}

            # Display the record neatly
            st.markdown("### 🎯 Query Result")
            col1, col2 = st.columns(2)
            with col1:
                st.markdown(f"<div class='card'><b>ID:</b> {item_dict['id']}<br><b>Date:</b> {item_dict['date']}</div>", unsafe_allow_html=True)
            with col2:
                st.markdown(f"<div class='card'><b>Factory:</b> {item_dict['factory_name']}<br><b>Metric:</b> {item_dict['metric']}<br><b>Value:</b> {item_dict['value']}</div>", unsafe_allow_html=True)

            st.markdown(f"<div class='metric-card'>⏱️ Latency: {latency_ms:.2f} ms</div>", unsafe_allow_html=True)
            st.session_state.log.append(f"✅ Queried record ({selected_id}, {selected_date}) in {latency_ms:.2f} ms.")

    except ClientError as e:
        st.error(f"AWS Error: {e.response['Error']['Message']}")
    except Exception as e:
        st.error(f"Error: {e}")


# ---------------------------------------
# MAIN LAYOUT HANDLER
# ---------------------------------------
if menu == "🏗️ Create Table":
    st.markdown("<div class='main-title'>🏗️ Create DynamoDB Table</div>", unsafe_allow_html=True)
    table_name = st.text_input("Enter Table Name:", st.session_state.table_name)
    st.session_state.table_name = table_name
    if st.button("Create Table"):
        create_table(table_name)

elif menu == "🧾 Insert Records":
    st.markdown("<div class='main-title'>🧾 Insert Records</div>", unsafe_allow_html=True)
    table_name = st.text_input("Enter Table Name:", st.session_state.table_name)
    st.session_state.table_name = table_name
    num_rows = st.number_input("Enter Number of Records:", min_value=1, value=10, step=1)
    if st.button("Insert Records"):
        insert_records(table_name, num_rows)

elif menu == "📦 Fetch Records":
    st.markdown("<div class='main-title'>📦 Fetch Records</div>", unsafe_allow_html=True)
    table_name = st.text_input("Enter Table Name:", st.session_state.table_name)
    st.session_state.table_name = table_name
    if st.button("Fetch Records"):
        fetch_records(table_name)

elif menu == "🔍 Query Record":
    query_record_ui()

elif menu == "🗒️ Activity Log":
    st.markdown("<div class='main-title'>🗒️ Activity Log</div>", unsafe_allow_html=True)
    if st.session_state.log:
        for entry in reversed(st.session_state.log):
            st.markdown(f"<div class='card'>{entry}</div>", unsafe_allow_html=True)
    else:
        st.info("No activities yet.")
