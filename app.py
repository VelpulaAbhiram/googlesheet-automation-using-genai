import streamlit as st
import json
import requests
import gspread
import pandas as pd
import os # To check for credentials file

# --- Google Sheet Configuration ---
# Path to your service account credentials JSON file
# IMPORTANT: For local development, place this file in the same directory as your Streamlit app.
CREDENTIALS_FILE = 'credentials.json'
# The name or URL of your Google Sheet (from the URL you provided, the name is likely "Copy of products")
GOOGLE_SHEET_NAME = 'nxt' # Update this to your exact Google Sheet name
# The name of the worksheet within the Google Sheet (e.g., 'Sheet1', 'Products')
WORKSHEET_NAME = 'jul riunning' # Update this to your exact worksheet name

# IMPORTANT: For local development, you need to provide your Gemini API Key here.
# In a production environment, use environment variables or a secure secret management system.
# You can get an API key from Google AI Studio: https://aistudio.google.com/app/apikey
GEMINI_API_KEY = "AIzaSyCn15R8V3lgXq8H8yc1GA0MqG5NaI4u144" # Replace with your actual Gemini API Key for local use

# --- Functions to interact with Google Sheets ---
@st.cache_resource
def get_service_account_client():
    """Authenticates with Google Sheets API using a service account."""
    if not os.path.exists(CREDENTIALS_FILE):
        st.error(f"Error: Credentials file '{CREDENTIALS_FILE}' not found.")
        st.stop() # Stop the app if credentials are missing
    try:
        # Renamed gc to _gc to indicate it shouldn't be hashed by Streamlit's cache
        _gc = gspread.service_account(filename=CREDENTIALS_FILE)
        st.success("Successfully authenticated with Google Sheets API.")
        return _gc
    except Exception as e:
        st.error(f"Error authenticating with Google Sheets API: {e}")
        st.stop()

@st.cache_data(ttl=3600) # Cache data for 1 hour to avoid hitting API limits on every refresh
# Renamed 'client' to '_client' to tell Streamlit not to hash this unhashable object
def get_sheet_data(_client, sheet_name, worksheet_name):
    """Fetches data from the specified Google Sheet and worksheet."""
    try:
        spreadsheet = _client.open(sheet_name) # Use _client here
        worksheet = spreadsheet.worksheet(worksheet_name)
        data = worksheet.get_all_records() # Gets data as list of dictionaries
        if not data:
            st.warning(f"No data found in '{sheet_name}' - '{worksheet_name}'. Please ensure your sheet has headers and data.")
            return pd.DataFrame() # Return empty DataFrame
        st.success(f"Successfully fetched {len(data)} rows from '{sheet_name}' - '{worksheet_name}'.")
        return pd.DataFrame(data)
    except gspread.exceptions.SpreadsheetNotFound:
        st.error(f"Error: Google Sheet '{sheet_name}' not found. Check the name or URL.")
        st.stop()
    except gspread.exceptions.WorksheetNotFound:
        st.error(f"Error: Worksheet '{worksheet_name}' not found in '{sheet_name}'. Check the worksheet name.")
        st.stop()
    except Exception as e:
        st.error(f"Error fetching data from Google Sheet: {e}")
        st.stop()

# --- Dynamic Schema Definition ---
def generate_schema_definition(dataframe):
    if dataframe.empty:
        return "The database table is empty."

    schema_parts = []
    for col in dataframe.columns:
        # Attempt to infer type based on first non-null value
        col_type = "string" # Default
        if not dataframe[col].empty:
            first_val = dataframe[col].dropna().iloc[0]
            if isinstance(first_val, (int, float)):
                col_type = "number"
            elif isinstance(first_val, bool):
                col_type = "boolean"
        schema_parts.append(f"- {col} ({col_type})")

    return "The database has one table called 'products' with the following schema:\n" + "\n".join(schema_parts)


# Function to simulate database query execution (now works with a DataFrame)
def execute_query(structured_query, dataframe):
    results = dataframe.to_dict('records') # Convert DataFrame to list of dicts for filtering

    # Apply WHERE conditions
    if "where" in structured_query and structured_query["where"]:
        filtered_results = []
        for item in results:
            match = True
            for key, value in structured_query["where"].items():
                if key.endswith('_gt'):
                    col = key.replace('_gt', '')
                    if not (col in item and isinstance(item[col], (int, float)) and item[col] > value):
                        match = False
                        break
                elif key.endswith('_lt'):
                    col = key.replace('_lt', '')
                    if not (col in item and isinstance(item[col], (int, float)) and item[col] < value):
                        match = False
                        break
                elif key.endswith('_contains'):
                    col = key.replace('_contains', '')
                    if not (col in item and isinstance(item[col], str) and str(value).lower() in str(item[col]).lower()):
                        match = False
                        break
                elif key.endswith('_in'):
                    col = key.replace('_in', '')
                    if not (col in item and item[col] in value):
                        match = False
                        break
                elif key == 'logical_operator':
                    continue # Handled implicitly by ANDing conditions
                else:
                    # Exact match
                    if not (key in item and item[key] == value):
                        match = False
                        break
            if match:
                filtered_results.append(item)
        results = filtered_results

    # Apply ORDER BY
    if "orderBy" in structured_query and structured_query["orderBy"]:
        column = structured_query["orderBy"]["column"]
        direction = structured_query["orderBy"]["direction"]
        # Ensure column exists before sorting
        if column in dataframe.columns:
            results.sort(key=lambda x: x.get(column), reverse=(direction == 'DESC'))
        else:
            st.warning(f"Warning: Cannot sort by non-existent column '{column}'.")


    # Apply SELECT columns
    if "columns" in structured_query and structured_query["columns"] and structured_query["columns"][0] != '*':
        projected_results = []
        for item in results:
            new_item = {}
            for col in structured_query["columns"]:
                if col in item:
                    new_item[col] = item[col]
            projected_results.append(new_item)
        results = projected_results

    # Apply LIMIT
    if "limit" in structured_query and structured_query["limit"] is not None:
        results = results[:structured_query["limit"]]

    return results

# Function to call Gemini API
def call_gemini_api(prompt, response_schema=None):
    api_url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={GEMINI_API_KEY}"
    
    payload = {
        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
    }

    if response_schema:
        payload["generationConfig"] = {
            "responseMimeType": "application/json",
            "responseSchema": response_schema
        }

    headers = {'Content-Type': 'application/json'}

    try:
        response = requests.post(api_url, headers=headers, data=json.dumps(payload))
        response.raise_for_status() # Raise an exception for HTTP errors
        result = response.json()

        if result.get("candidates") and result["candidates"][0].get("content") and result["candidates"][0]["content"].get("parts"):
            return result["candidates"][0]["content"]["parts"][0]["text"]
        else:
            st.error(f"Unexpected LLM response structure: {result}")
            return None
    except requests.exceptions.RequestException as e:
        st.error(f"API call failed: {e}")
        return None
    except json.JSONDecodeError:
        st.error(f"Failed to decode JSON from API response: {response.text}")
        return None

# Streamlit UI
st.set_page_config(page_title="NL to SQL Query", layout="centered")

st.markdown("""
    <style>
    .stApp {
        background: linear-gradient(to bottom right, #f0f2f6, #e0e4eb);
        font-family: 'Inter', sans-serif;
    }
    h1 {
        color: #1a202c;
        text-align: center;
        font-weight: 800;
    }
    .stTextArea label, .stButton {
        font-weight: 600;
        color: #2d3748;
    }
    .stButton > button {
        background-color: #4299e1;
        color: white;
        border-radius: 0.5rem;
        padding: 0.75rem 1.5rem;
        font-size: 1.125rem;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        transition: all 0.2s ease-in-out;
        width: 100%;
    }
    .stButton > button:hover {
        background-color: #3182ce;
        transform: translateY(-2px);
        box-shadow: 0 6px 8px rgba(0, 0, 0, 0.15);
    }
    .stTextArea textarea {
        border-radius: 0.5rem;
        border: 1px solid #cbd5e0;
        padding: 1rem;
        box-shadow: inset 0 1px 2px rgba(0, 0, 0, 0.05);
    }
    .result-box {
        background-color: #e0f2f7;
        border: 1px solid #b2ebf2;
        border-radius: 0.5rem;
        padding: 1.5rem;
        margin-top: 1.5rem;
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05);
    }
    .result-box h2 {
        color: #00796b;
        margin-bottom: 0.75rem;
    }
    .info-box {
        background-color: #f0f4f8;
        border: 1px solid #d1d9e6;
        border-radius: 0.5rem;
        padding: 1rem;
        margin-top: 2rem;
        font-size: 0.9rem;
        color: #4a5568;
    }
    </style>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700;800&display=swap" rel="stylesheet">
""", unsafe_allow_html=True)


st.title("SQL to Natural Language Query")

# User ID display (if needed for a multi-user app, otherwise can be removed)
# In Streamlit, user IDs are not directly available like in Canvas.
# You could implement a simple session-based ID if needed for local testing.
# For now, let's just indicate it's for Canvas environment.
st.markdown(
    """
    <div class="text-sm text-center text-gray-600 mb-4 p-2 bg-blue-50 rounded-lg">
        <p>User ID functionality is typically for multi-user Canvas environments. Not directly applicable for local Streamlit demo.</p>
    </div>
    """, unsafe_allow_html=True
)

# --- Fetch data from Google Sheet ---
# Renamed gc to _gc to match the _client parameter in get_sheet_data
_gc = get_service_account_client()
df = get_sheet_data(_gc, GOOGLE_SHEET_NAME, WORKSHEET_NAME) # Pass _gc here

if not df.empty:
    SCHEMA_DEFINITION = generate_schema_definition(df)
    st.write("Data loaded from Google Sheet:")
    st.dataframe(df) # Display the loaded data for verification
else:
    st.warning("No data available from Google Sheet to query.")
    st.stop() # Stop the app if no data is loaded

nl_query = st.text_area(
    "Enter your natural language query:",
    placeholder="e.g., Show me all electronics products with price less than 200. Or, List the names of products with stock greater than 100, ordered by price descending.",
    height=150
)

if st.button("Get Natural Language Result"):
    if not nl_query.strip():
        st.error("Please enter a natural language query.")
    else:
        with st.spinner("Processing your query..."):
            try:
                # Step 1: Convert Natural Language to Structured Query using LLM
                nl_to_structured_prompt = f"""
                    You are an expert in converting natural language questions into structured queries for a database.
                    {SCHEMA_DEFINITION}

                    Your task is to convert the user's natural language question into a JSON object representing a query.
                    The JSON object should have the following structure:
                    {{
                      "operation": "SELECT", // Always "SELECT" for now
                      "columns": ["column1", "column2", ...], // List of columns to select, use "*" for all
                      "where": {{ // Optional: conditions for filtering
                        "column_name": "value", // Exact match
                        "column_name_gt": "value", // Greater than
                        "column_name_lt": "value", // Less than
                        "column_name_contains": "substring", // String contains (case-insensitive)
                        "column_name_in": ["value1", "value2"], // Value in list
                        "logical_operator": "AND" // or "OR" if multiple conditions are present (for more complex parsing)
                      }},
                      "orderBy": {{ // Optional: sorting
                        "column": "column_name",
                        "direction": "ASC" // or "DESC"
                      }},
                      "limit": 5 // Optional: number of results to limit
                    }}

                    If no specific columns are requested, default to all columns ("*").
                    If no specific conditions are requested, omit the "where" clause.
                    If no specific order is requested, omit the "orderBy" clause.
                    If no limit is requested, omit the "limit" clause.
                    Ensure column names match the schema exactly (id, name, category, price, stock).

                    Example 1: "Show me all electronics products."
                    Output:
                    {{
                      "operation": "SELECT",
                      "columns": ["*"],
                      "where": {{
                        "category": "Electronics"
                      }}
                    }}

                    Example 2: "What are the names and prices of products with stock less than 100?"
                    Output:
                    {{
                      "operation": "SELECT",
                      "columns": ["name", "price"],
                      "where": {{
                        "stock_lt": 100
                      }}
                    }}

                    Example 3: "List top 3 most expensive products."
                    Output:
                    {{
                      "operation": "SELECT",
                      "columns": ["*"],
                      "orderBy": {{
                        "column": "price",
                        "direction": "DESC"
                      }},
                      "limit": 3
                    }}

                    Now, convert the following question:
                    {nl_query}
                """
                
                structured_query_json_str = call_gemini_api(
                    nl_to_structured_prompt,
                    response_schema={
                        "type": "OBJECT",
                        "properties": {
                            "operation": {"type": "STRING"},
                            "columns": {"type": "ARRAY", "items": {"type": "STRING"}},
                            "where": {
                                "type": "OBJECT",
                                "additionalProperties": {"type": ["STRING", "NUMBER", "ARRAY"]}
                            },
                            "orderBy": {
                                "type": "OBJECT",
                                "properties": {
                                    "column": {"type": "STRING"},
                                    "direction": {"type": "STRING"}
                                }
                            },
                            "limit": {"type": "NUMBER"}
                        },
                        "required": ["operation", "columns"]
                    }
                )

                if structured_query_json_str:
                    structured_query = json.loads(structured_query_json_str)
                    st.write("Structured Query from LLM:")
                    st.json(structured_query)

                    # Step 2: Execute the structured query against the simulated database
                    query_results = execute_query(structured_query, df) # Pass the DataFrame
                    st.write("Simulated Query Results:")
                    st.json(query_results)

                    # Step 3: Convert Structured Data to Natural Language using LLM
                    structured_to_nl_prompt = f"""
                        You are an expert in summarizing database query results in a clear and concise natural language format.
                        Given the following query results in JSON format, describe them in a human-readable way.
                        If no results are found, state "No results found for your query."
                        
                        Query Results:
                        {json.dumps(query_results, indent=2)}
                    """
                    nl_result = call_gemini_api(structured_to_nl_prompt)

                    if nl_result:
                        st.markdown(f"""
                            <div class="result-box">
                                <h2>Natural Language Result:</h2>
                                <p>{nl_result}</p>
                            </div>
                        """, unsafe_allow_html=True)
                else:
                    st.error("Could not generate a structured query or natural language result.")

            except json.JSONDecodeError:
                st.error("Error: The LLM returned invalid JSON. Please try rephrasing your query.")
            except Exception as e:
                st.error(f"An unexpected error occurred: {e}")

st.markdown("""
    <div class="info-box">
        <h3>How it works:</h3>
        <ul>
            <li>Your natural language query is sent to the Gemini API.</li>
            <li>The Gemini API converts it into a structured query (like SQL) based on a predefined schema.</li>
            <li>This structured query is then "executed" against the data fetched from your Google Sheet.</li>
            <li>The results are sent back to the Gemini API.</li>
            <li>The Gemini API summarizes these results into human-readable natural language.</li>
        </ul>
        <p>
            <b>Important:</b> Ensure your `credentials.json` file is in the same directory as this script and your Google Sheet is shared with the service account's email.
            <br>
            <b>Important:</b> For local testing, remember to add your Gemini API Key in the `GEMINI_API_KEY` variable within the script.
        </p>
    </div>
""", unsafe_allow_html=True)