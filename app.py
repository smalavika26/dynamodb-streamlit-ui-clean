import streamlit as st
import boto3
import pandas as pd
import time
import os
from dotenv import load_dotenv
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr

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
# SIDEBAR NAVIGATION
# -------------------------------
st.sidebar.title("🔧 DynamoDB Utilities")
menu = st.sidebar.radio(
    "Choose an option:",
    ["Create Table", "Insert Records", "Fetch Records", "Query Record", "Activity Log"]
)

# -------------------------------
# HELPER FUNCTIONS
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
        st.success(f"✅ Inserted {num_rows} records successfully in {(end-start)*1000:.2f} ms.")
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
        data = response.get("Items", [])
        if not data:
            st.info("No records found.")
            return
        df = pd.DataFrame(data)
        st.dataframe(df)
        st.success(f"✅ Fetched {len(df)} records in {(end-start)*1000:.2f} ms.")
        st.session_state.log.append(f"✅ Fetched {len(df)} from '{table_name}'.")
    except Exception as e:
        st.error(f"❌ Error fetching: {e}")
        st.session_state.log.append(f"❌ Fetch failed: {e}")

# -------------------------------
# IMPROVED QUERY RECORD SECTION
# -------------------------------
def query_record_ui():
    st.subheader("🔍 Query a Specific Record")

    # Step 1: Get table name (persistent)
    table_name = st.text_input("Enter Table Name:", st.session_state.table_name, placeholder="Enter your DynamoDB table name")
    st.session_state.table_name = table_name

    if not table_name:
        st.info("Please enter a table name to continue.")
        return

    table = dynamodb.Table(table_name)

    try:
        # Get all dates for dropdown
        scan_dates = table.scan(
            ProjectionExpression="#d",
            ExpressionAttributeNames={"#d": "date"}
        )
        dates = sorted({item["date"] for item in scan_dates.get("Items", [])})

        if not dates:
            st.warning("⚠️ No records found in table.")
            return

        selected_date = st.selectbox("Select Date:", dates)

        # Fetch IDs for that date
        query_ids = table.query(
            KeyConditionExpression=Key("date").eq(selected_date),
            ProjectionExpression="#d, id",
            ExpressionAttributeNames={"#d": "date"}
        )
        ids = [item["id"] for item in query_ids.get("Items", [])]

        if not ids:
            st.warning("⚠️ No IDs found for this date.")
            return

        selected_id = st.selectbox("Select Record ID:", ids)

        # Add optional filter for factory name
        with st.expander("🔎 Optional Filters"):
            filter_factory = st.text_input("Filter by Factory Name (optional):", "")

        # Add column selector
        with st.expander("📋 Choose Columns to Display"):
            all_columns = ["date", "id", "factory_name", "metric", "value"]
            selected_columns = st.multiselect("Select columns to show:", all_columns, default=all_columns)

        if st.button("Query Record"):
            start = time.perf_counter()
            result = table.get_item(Key={"date": selected_date, "id": selected_id})
            end = time.perf_counter()
            latency_ms = (end - start) * 1000

            item = result.get("Item")

            if not item:
                st.warning("⚠️ No record found.")
                return

            if filter_factory and item.get("factory_name", "").lower() != filter_factory.lower():
                st.warning("⚠️ Record found, but does not match factory filter.")
                return

            filtered_item = {k: item[k] for k in selected_columns if k in item}
            df = pd.DataFrame([filtered_item])
            st.dataframe(df, use_container_width=True)
            st.success(f"✅ Query executed in {latency_ms:.2f} ms")
            st.session_state.log.append(f"✅ Query record ({selected_date}, {selected_id}) in {latency_ms:.2f} ms")
    except ClientError as e:
        st.error(f"AWS Error: {e.response['Error']['Message']}")
    except Exception as e:
        st.error(f"Error: {e}")

# -------------------------------
# MAIN APP LAYOUT
# -------------------------------
if menu == "Create Table":
    st.header("🧱 Create Table")
    table_name = st.text_input("Enter Table Name:", st.session_state.table_name, placeholder="Enter a new table name")
    st.session_state.table_name = table_name
    if st.button("Create Table"):
        create_table(table_name)

elif menu == "Insert Records":
    st.header("📝 Insert Records")
    table_name = st.text_input("Enter Table Name:", st.session_state.table_name, placeholder="Enter existing table name")
    st.session_state.table_name = table_name
    num_rows = st.number_input("Rows to Insert:", min_value=1, value=10, step=1)
    if st.button("Insert"):
        insert_records(table_name, num_rows)

elif menu == "Fetch Records":
    st.header("📦 Fetch All Records")
    table_name = st.text_input("Enter Table Name:", st.session_state.table_name, placeholder="Enter table name")
    st.session_state.table_name = table_name
    if st.button("Fetch"):
        fetch_records(table_name)

elif menu == "Query Record":
    query_record_ui()

elif menu == "Activity Log":
    st.header("🗒️ Activity Log")
    if st.session_state.log:
        for entry in reversed(st.session_state.log):
            st.write(entry)
    else:
        st.info("No activities yet.")
