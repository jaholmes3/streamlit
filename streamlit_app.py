import streamlit as st
import os
import io
#import boto3

import pandas as pd
import numpy as np
from io import StringIO, BytesIO

#import fsspec


import requests
from datetime import datetime, timedelta
import zipfile
import xml.etree.ElementTree as ET

# import streamlit as st
# import boto3
# import pandas as pd
# from io import BytesIO

# API setup
st.set_page_config(layout="wide")

api_key = "key"
endpoint = "https://api.openai.com/v1/chat/completions"
headers = {
    "Authorization": f"Bearer {api_key}",
    "Content-Type": "application/json"
}
 

def load_parquet_from_s3():
    s3 = boto3.client('s3')
    bucket_name = 'grantsgov'
    try:
        response = s3.list_objects_v2(Bucket=bucket_name)
        parquet_files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.parquet')]
        if not parquet_files:
            return None, "No Parquet files found in the bucket."
        
        key = parquet_files[0]
        obj = s3.get_object(Bucket=bucket_name, Key=key)
        df = pd.read_parquet(BytesIO(obj['Body'].read()))
        return df, key
    except Exception as e:
        return None, str(e)

def call_chat_gpt(prompt):
    data = {
        "model": "gpt-3.5-turbo",
        "messages": [
            {"role": "system", "content": "You are a helpful assistant."},
            {"role": "user", "content": prompt}
        ],
        "max_tokens": 150
    }
    
    response = requests.post(endpoint, headers=headers, json=data)
    if response.status_code == 200:
        response_json = response.json()
        return response_json['choices'][0]['message']['content'].strip()
    else:
        return "Failed to fetch response: " + response.text

def display_data_insights(df, file_name):
    if df.empty:
        st.write("No data available to display insights.")
        return

    st.write(f"Data loaded from: {file_name}")

    # Ensure CloseDate is a datetime type for comparison
    if 'CloseDate' in df.columns:
        df['CloseDate'] = pd.to_datetime(df['CloseDate'], errors='coerce')

    # Record count
    record_count = len(df)
    st.write(f"Record count: {record_count}")

    # Date range for CloseDate
    if 'CloseDate' in df.columns:
        min_close_date = df['CloseDate'].min()
        max_close_date = df['CloseDate'].max()
        st.write(f"Earliest Close Date: {min_close_date.strftime('%Y-%m-%d') if pd.notna(min_close_date) else 'N/A'}")
        st.write(f"Latest Close Date: {max_close_date.strftime('%Y-%m-%d') if pd.notna(max_close_date) else 'N/A'}")

def format_chatgpt_responses(responses):
    formatted_responses = []
    for response in responses:
        formatted_response = response.replace("<strong>", "").replace("</strong>", "").replace(":", ":\n")
        formatted_responses.append(formatted_response)
    return "\n\n".join(formatted_responses)

def format_grant_themes_responses(responses):
    formatted_responses = []
    for response in responses:
        formatted_response = response.replace("General type of grant:", "\nGeneral type of grant:").replace("Related theme:", "\nRelated theme:")
        # Remove the word "Relevant" if it's present
        formatted_response = formatted_response.replace("Relevant", "").strip()
        formatted_responses.append(formatted_response)
    return "\n\n".join(formatted_responses)

def main():
    st.title("Grants.Gov Data Viewer")
    
    if 'chatgpt_responses_data_viewer' not in st.session_state:
        st.session_state['chatgpt_responses_data_viewer'] = []

    if 'chatgpt_responses_grant_themes' not in st.session_state:
        st.session_state['chatgpt_responses_grant_themes'] = []

    tab1, tab2, tab3, tab4 = st.tabs(["Data Viewer", "Analytics", "Grant Themes", "Run Script"])
    
    with tab1:
        st.header("Data Viewer Tab")
        st.markdown("""
        ### Instructions for Data Viewer Tab
        - **Days Input**: Specify the number of days to look ahead for grant opportunities closing soon.
        - **Search Terms**: Enter keywords to focus the analysis by ChatGPT on grants related to specific topics or needs.
        - **Load Data**: Click the button to load and display the filtered grant data.
        """)
        
        days_input = st.number_input("Enter number of days to filter for upcoming CloseDate:", min_value=0, value=20, step=1)
        search_terms = st.text_input("Enter search terms for ChatGPT:")
        
        if st.button("Load and Display Parquet Data"):
            df, file_name = load_parquet_from_s3()
            if df is not None:
                st.success("Data loaded successfully!")
                df['CloseDate'] = pd.to_datetime(df['CloseDate'], format='%m%d%Y', errors='coerce')

                today = pd.to_datetime('today').normalize()
                future_date = today + pd.Timedelta(days=days_input)

                filtered_df = df[(df['FundingInstrumentType'] == 'G') & 
                                 (df['CloseDate'] >= today) & 
                                 (df['CloseDate'] <= future_date)]
                st.session_state['filtered_df'] = filtered_df
                st.session_state['file_name'] = file_name
                record_count = len(filtered_df)
                st.write(f"Record count: {record_count}")
                st.write(f"Filter applied for the next {days_input} days.")
            else:
                st.error(f"Failed to load data: {file_name}")
        
        if 'filtered_df' in st.session_state and not st.session_state['filtered_df'].empty:
            if st.button("Search ChatGPT"):
                with st.spinner("Processing..."):
                    chat_responses = []
                    for index, row in st.session_state['filtered_df'].iterrows():
                        description = row['Description']
                        opportunity_number = row['OpportunityNumber']
                        opportunity_title = row['OpportunityTitle']
                        opportunity_id = row['OpportunityID']
                        grant_url = f"https://www.grants.gov/search-results-detail/{opportunity_id}"
                        # prompt = f"Review the following grant description related to '{search_terms}'. Confirm if it's relevant by responding with 'Yes' or 'No' or 'Relevant', and provide a concise explanation. Highlight key eligibility criteria for US grants. Opportunity ID: {opportunity_number}, Title: '{opportunity_title}':\n\n{description}"
                        prompt = f"Review the following grant description related to '{search_terms}'. Confirm if it's relevant by responding with 'Yes' or 'No' or 'Relevant', and provide a concise explanation. Highlight key eligibility criteria for US grants. Suggest potential search terms based on the grant description. Opportunity ID: {opportunity_number}, Title: '{opportunity_title}':\n\n{description}"
                        response = call_chat_gpt(prompt)
                        if "Yes" in response:
                            response += f"\n\n[View Grant Details]({grant_url})"
                            chat_responses.append(f"**Opportunity Number {opportunity_number}:** {response}")
                    st.session_state['chatgpt_responses_data_viewer'] = chat_responses
            formatted_responses = format_chatgpt_responses(st.session_state['chatgpt_responses_data_viewer'])
            st.markdown(formatted_responses, unsafe_allow_html=True)

    with tab2:
        st.header("Analytics Tab")
        st.markdown("""
        ### Instructions for Analytics Tab
        - **Data Insights**: This tab displays insights from the filtered grant data.
        - **DataFrame**: View the filtered data in a tabular format.
        """)
        
        if 'filtered_df' in st.session_state and not st.session_state['filtered_df'].empty:
            st.write("Displaying filtered data insights and DataFrame:")
            display_data_insights(st.session_state['filtered_df'], st.session_state['file_name'])
            st.dataframe(st.session_state['filtered_df'], height=300)
        else:
            st.write("No data available or filter conditions not met.")

    with tab3:
        st.header("Grant Themes Tab")
        st.markdown("""
        ### Instructions for Grant Themes Tab
        - **Analyze Grant Themes**: Click the button to analyze the themes of the grants using ChatGPT.
        - **Results**: The results will show the general type and related theme for each grant.
        """)
        
        if 'filtered_df' in st.session_state and not st.session_state['filtered_df'].empty:
            if st.button("Analyze Grant Themes with ChatGPT"):
                with st.spinner("Processing..."):
                    chat_responses = []
                    for index, row in st.session_state['filtered_df'].iterrows():
                        description = row['Description']
                        opportunity_number = row['OpportunityNumber']
                        opportunity_title = row['OpportunityTitle']
#                         prompt = f"""
#                         Review the following grant description. Confirm if it's relevant by responding with 'Yes', 'No', or 'Relevant', and provide a concise explanation. Identify the general type of grant and its related theme. Group the grants by their themes if possible. Please ensure your response is formatted as:

#                         Opportunity Number {opportunity_number}:
#                         General type of grant: [general type]
#                         Related theme: [theme]

#                         Please do not include any additional explanation.

#                         Opportunity ID: {opportunity_number}, Title: '{opportunity_title}':
#                         Description:
#                         {description}

#                         Review the grant and provide your analysis.
#                         """
                        prompt = f"""
                        Review the following grant description. Confirm if it's relevant by responding with 'Yes', 'No', or 'Relevant', and provide a concise explanation. Identify the general type of grant and its related theme. Suggest potential search terms based on the grant description. Group the grants by their themes if possible. Please ensure your response is formatted as:

                        Opportunity Number {opportunity_number}:
                        General type of grant: [general type]
                        Related theme: [theme]
                        Suggested search terms: [search terms]

                        Please do not include any additional explanation.

                        Opportunity ID: {opportunity_number}, Title: '{opportunity_title}':
                        Description:
                        {description}

                        Review the grant and provide your analysis.
                        """

                        response = call_chat_gpt(prompt)
                        # Remove the duplicate opportunity number and the word "Relevant"
                        formatted_response = response.replace(f"Opportunity Number {opportunity_number}:", "").replace("Relevant", "").strip()
                        final_response = f"Opportunity Number {opportunity_number}:\n{formatted_response}\n"
                        chat_responses.append(final_response)
                    st.session_state['chatgpt_responses_grant_themes'] = chat_responses
            formatted_responses = format_grant_themes_responses(st.session_state['chatgpt_responses_grant_themes'])
            st.text_area("ChatGPT Responses (Grant Themes):", value=formatted_responses, height=400, key="grant_themes_responses")

    with tab4:
        st.header("Run Script Tab")
        st.markdown("""
        ### Instructions for Run Script Tab
        - **Execute Script**: Click the button to execute a Python script located in the same directory as this Streamlit app.
        """)
        
        script_name = st.text_input("Enter the script filename (e.g., process_grants_data.py):", value="process_grants_data.py")
        
        if st.button("Execute Script"):
            with st.spinner("Running script..."):
                try:
                    with open(script_name) as f:
                        code = f.read()
                        exec(code, globals())
                    st.success(f"Script '{script_name}' executed successfully!")
                except Exception as e:
                    st.error(f"Error executing script: {e}")


if __name__ == "__main__":
    main()
