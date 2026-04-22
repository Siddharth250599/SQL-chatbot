# SQL Chatbot

An AI-powered chatbot that lets non-technical users query databases using plain English. Built by a business analyst to eliminate the need for manual SQL writing.

## Demo
Upload any CSV or Excel file and ask questions like:
- "What is the total revenue by customer segment?"
- "Show me the top 5 customers by total spending"
- "What percentage of orders are from each city?"

## Features
- Natural language to SQL conversion using LLaMA 3.1
- Upload CSV or Excel files instantly
- Combine two sheets and query across both
- Interactive charts (bar, pie, line, scatter)
- Switch chart types with one click
- Line by line SQL explanation
- Safety layer to block destructive queries
- Auto retry on errors
- Sample dataset included
- 95% accuracy on eval test suite

## Tech Stack
- Python
- Streamlit
- Ollama + LLaMA 3.1 (runs locally, no API key needed)
- SQLite
- Pandas
- Plotly

## How to run locally

1. Install Ollama from ollama.com and pull the model:
ollama pull llama3.1

2. Install dependencies:
pip install streamlit pandas plotly openpyxl ollama

3. Run the app:
streamlit run sql_chatbot_web.py

4. Open your browser and upload a CSV or Excel file to get started!

## Project Structure
- sql_chatbot_web.py — main web app
- sql_chatbot.py — terminal version
- sample_data.csv — sample dataset to try instantly
- evals.py — evaluation script to test accuracy