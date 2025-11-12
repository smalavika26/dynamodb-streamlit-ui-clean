import streamlit as st
import boto3
import pandas as pd
import time
import os
from dotenv import load_dotenv
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key

# -------------------------------
# INITIAL SETUP
# -------------------------------
load_dotenv()

# Persistent DynamoDB connection
if "dynamodb" not in st.session_state:
    st.session_state.dynamodb = boto3.resource(
        "dynamodb",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION")
    )

dynamodb = st.session_state.dynamodb

# Persistent table name and log
if "table_name" not in st.session_state:
    st.session_state.table_name = ""

if "log" not in st.session_state:
    st.session_state.log = []

# -------------------------------
# APP STYLE CONFIG
# -------------------------------
st.set_page_config(page_title="DynamoDB Utility Dashboard", layout="wide")

# Apply custom CSS
st.markdown("""
    <style>
        .main-title {
            font-size: 28px !important;
            font-weight: 700 !important;
            color: #003366 !important;
            margin-bottom: 20px;
        }
        .card {
            background-color: #f9f9f9;
            padding: 1.5rem;
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

# -------------------------------
# SIDEBAR NAVIGATION
# -------------------------------
st.sidebar.title("⚙️ DynamoDB Utilities")
st.sidebar.markdown("---")
menu = st.sidebar.radio(
    "📂 Select an action:",
    ["🏗️ Create Table", "🧾 Insert Records", "📦 Fetch Records", "🔍 Query Record", "🗒️ Activity Log"]
)

# -------------------------------
# CORE FUNCTIONS
# -------------------------------
def create_table(table_name):
    try:
        existing_tables = [t.name for t in dynamodb.tables.all()]
        if table_name in existing_tables:
            st.warning(f"⚠️ Table '{table_name}' already exists.")
            return
        table = dynamodb.create_table(
            TableName=table_name,
            KeySchema=[
                {"AttributeName": "date", "KeyType": "HASH"},
                {"AttributeName": "id", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "date", "AttributeType": "S"},
                {"AttributeName": "id", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        table.meta.client.get_waiter("table_exists").wait(TableName=table_name)
        st.success(f"✅ Table '{table_name}' created successfully!")
        st.session_state.log.append(f"✅ Created table '{table_name}'.")
    except Exception as e:
        st.error(f"❌ Error creating table: {e}")
        st.session_state.log.append(f"❌ Create table failed: {e}")

def insert_records(table_name, num_rows):
    try:
        table = dynamodb.Table(table_name)
        start = time.time()
        with table.batch_writer() as batch:
            for i in range(num_rows):
                item = {
                    "date": f"2025-11-{10+i}",
                    "id": str(i + 1),
                    "factory_name": f"Factory_{(i % 3) + 1}",
                    "metric": f"Metric_{i+1}",
                    "value": i * 10,
                }
                batch.put_item(Item=item)
        end = time.time()
        latency = (end - start) * 1000
        st.success(f"✅ Inserted {num_rows} records successfully in {latency:.2f} ms.")
        st.session_state.log.append(f"✅ Inserted {num_rows} into '{table_name}'.")
    except Exception as e:
        st.error(f"❌ Error inserting: {e}")
        st.session_state.log.append(f"❌ Insert failed: {e}")

def fetch_records(table_name):
    try:
        table = dynamodb.Table(table_name)
        start = time.time()
        response = table.scan()
        end = time.time()
        latency = (end - start) * 1000
        data = response.get("Items", [])
        if not data:
            st.info("No records found.")
            return
        df = pd.DataFrame(data)
        st.markdown("### 📊 Fetched Data")
        st.dataframe(df, use_container_width=True)
        st.markdown(f"**⏱️ Latency:** {latency:.2f} ms")
    except Exception as e:
        st.error(f"❌ Error fetching: {e}")

def query_record_ui():
    st.markdown("### 🔍 Query a Specific Record")
    table_name = st.text_input("Enter Table Name:", st.session_state.table_name)
    st.session_state.table_name = table_name

    if not table_name:
        st.info("Enter a valid table name to continue.")
        return

    table = dynamodb.Table(table_name)
    try:
        scan_dates = table.scan(ProjectionExpression="#d", ExpressionAttributeNames={"#d": "date"})
        dates = sorted({item["date"] for item in scan_dates.get("Items", [])})
        if not dates:
            st.warning("⚠️ No data available.")
            return

        selected_date = st.selectbox("Select Date:", dates)
        ids = [item["id"] for item in table.query(KeyConditionExpression=Key("date").eq(selected_date))["Items"]]
        selected_id = st.selectbox("Select Record ID:", ids)

        if st.button("Query Record"):
            start = time.perf_counter()
            result = table.get_item(Key={"date": selected_date, "id": selected_id})
            end = time.perf_counter()
            latency = (end - start) * 1000

            item = result.get("Item")
            if not item:
                st.warning("⚠️ No record found.")
                return

            st.markdown("### 🎯 Query Result")
            with st.container():
                col1, col2 = st.columns(2)
                with col1:
                    st.markdown(f"<div class='card'><b>Date:</b> {item['date']}<br><b>ID:</b> {item['id']}</div>", unsafe_allow_html=True)
                with col2:
                    st.markdown(f"<div class='card'><b>Factory:</b> {item['factory_name']}<br><b>Metric:</b> {item['metric']}<br><b>Value:</b> {item['value']}</div>", unsafe_allow_html=True)

            st.markdown(f"<div class='metric-card'>⏱️ Latency: {latency:.2f} ms</div>", unsafe_allow_html=True)
    except Exception as e:
        st.error(f"❌ Error querying record: {e}")

# -------------------------------
# MAIN APP LAYOUT
# -------------------------------
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
