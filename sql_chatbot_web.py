# sql_chatbot_web.py
import streamlit as st
import pandas as pd
import sqlite3
import ollama
import os
import re
import plotly.express as px

# ─── PAGE SETUP ──────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="SQL Chatbot",
    page_icon="🤖",
    layout="wide"
)

st.title("SQL Chatbot")
st.caption("Upload an Excel or CSV file and ask questions in plain English!")

# ─── LOAD SHEETS INTO DATABASE ───────────────────────────────────────────────

def load_sheets(file, selected_sheets):
    conn = sqlite3.connect(":memory:")
    schemas = []
    dfs = {}

    for sheet in selected_sheets:
        if isinstance(file, str):
            df = pd.read_csv(file)
        elif file.name.endswith(".csv"):
            df = pd.read_csv(file)
        else:
            df = pd.read_excel(file, sheet_name=sheet)

        df.columns = df.columns.str.strip().str.replace(" ", "_").str.lower()
        table_name = sheet.lower().replace(" ", "_")
        df.to_sql(table_name, conn, index=False, if_exists="replace")
        dfs[table_name] = df

        columns = ", ".join([f"{col} ({str(df[col].dtype)})" for col in df.columns])
        sample = df.head(2).to_string(index=False)
        schemas.append(f"""Table: {table_name}
Columns: {columns}
Sample:
{sample}""")

    return conn, dfs, schemas

# ─── SAFETY CHECK ────────────────────────────────────────────────────────────

BLOCKED = ["drop", "delete", "insert", "update", "alter", "truncate"]

def is_safe(sql):
    return not any(word in sql.lower() for word in BLOCKED)

# ─── CLEAN SQL ───────────────────────────────────────────────────────────────

def clean_sql(text):
    text = text.strip()
    if "```sql" in text:
        text = text.split("```sql")[1].split("```")[0]
    elif "```" in text:
        text = text.split("```")[1].split("```")[0]
    text = text.strip().rstrip(";").strip()
    return text

# ─── BUILD EXPLANATION ───────────────────────────────────────────────────────

def build_explanation(sql):
    sql_clean = sql.strip()
    lines = []

    select_match = re.search(r'SELECT\s+(.*?)\s+FROM', sql_clean, re.IGNORECASE | re.DOTALL)
    if select_match:
        select_part = select_match.group(1).strip()
        for col in select_part.split(","):
            col = col.strip()
            col_upper = col.upper()
            if "COUNT(*)" in col_upper:
                lines.append(f"`{col}` — counts the total number of rows")
            elif "SUM(" in col_upper:
                lines.append(f"`{col}` — adds up all the values in that column")
            elif "AVG(" in col_upper:
                lines.append(f"`{col}` — calculates the average value")
            elif "MAX(" in col_upper:
                lines.append(f"`{col}` — finds the highest value")
            elif "MIN(" in col_upper:
                lines.append(f"`{col}` — finds the lowest value")
            elif "ROUND(" in col_upper:
                lines.append(f"`{col}` — calculates the value and rounds it to 2 decimal places")
            elif "DISTINCT" in col_upper:
                lines.append(f"`{col}` — counts only unique values, no duplicates")
            else:
                lines.append(f"`{col}` — selects this column to show in the result")

    from_match = re.search(r'FROM\s+(\w+)', sql_clean, re.IGNORECASE)
    if from_match:
        table = from_match.group(1)
        lines.append(f"`FROM {table}` — gets the data from the **{table}** table")

    join_match = re.search(
        r'(INNER|LEFT|RIGHT)?\s*JOIN\s+(\w+)\s+ON\s+(.*?)(?=WHERE|GROUP|ORDER|LIMIT|$)',
        sql_clean, re.IGNORECASE | re.DOTALL
    )
    if join_match:
        join_table = join_match.group(2)
        join_condition = join_match.group(3).strip()
        lines.append(f"`JOIN {join_table}` — combines data from the **{join_table}** table")
        lines.append(f"`ON {join_condition}` — matches rows where this condition is true")

    where_match = re.search(r'WHERE\s+(.*?)(?=GROUP|ORDER|LIMIT|$)', sql_clean, re.IGNORECASE | re.DOTALL)
    if where_match:
        condition = where_match.group(1).strip()
        lines.append(f"`WHERE {condition}` — filters rows to only include ones where this is true")

    group_match = re.search(r'GROUP\s+BY\s+(.*?)(?=ORDER|LIMIT|HAVING|$)', sql_clean, re.IGNORECASE | re.DOTALL)
    if group_match:
        group_cols = group_match.group(1).strip()
        lines.append(f"`GROUP BY {group_cols}` — groups rows together so we can calculate totals per **{group_cols}**")

    having_match = re.search(r'HAVING\s+(.*?)(?=ORDER|LIMIT|$)', sql_clean, re.IGNORECASE | re.DOTALL)
    if having_match:
        having_cond = having_match.group(1).strip()
        lines.append(f"`HAVING {having_cond}` — filters the groups after grouping")

    order_match = re.search(r'ORDER\s+BY\s+(.*?)(?=LIMIT|$)', sql_clean, re.IGNORECASE | re.DOTALL)
    if order_match:
        order_cols = order_match.group(1).strip()
        if "DESC" in order_cols.upper():
            lines.append(f"`ORDER BY {order_cols}` — sorts results from highest to lowest")
        else:
            lines.append(f"`ORDER BY {order_cols}` — sorts results from lowest to highest")

    limit_match = re.search(r'LIMIT\s+(\d+)', sql_clean, re.IGNORECASE)
    if limit_match:
        n = limit_match.group(1)
        lines.append(f"`LIMIT {n}` — returns only the top **{n}** results")

    if not lines:
        return None

    return "\n\n".join(lines)

# ─── AUTO CHART ──────────────────────────────────────────────────────────────

def auto_chart(df, question):
    """Decide the default chart type based on the data"""
    if df is None or len(df) < 2 or len(df.columns) > 6:
        return None

    cols = df.columns.tolist()
    num_cols = df.select_dtypes(include=['number']).columns.tolist()
    cat_cols = df.select_dtypes(include=['object']).columns.tolist()
    question_lower = question.lower()

    if any(word in question_lower for word in ["percentage", "percent", "share", "proportion"]):
        if len(num_cols) >= 1 and len(cat_cols) >= 1:
            return "Pie"

    date_cols = [c for c in cols if any(
        word in c.lower() for word in ["date", "month", "year", "quarter", "time"]
    )]
    if date_cols and num_cols:
        return "Line"

    if len(cat_cols) >= 1 and len(num_cols) >= 1:
        return "Bar"

    return None

# ─── BUILD CHART ─────────────────────────────────────────────────────────────

def build_chart(df, chart_type):
    """Build a Plotly chart based on selected type"""
    if df is None or chart_type == "No chart" or chart_type is None:
        return None

    cols = df.columns.tolist()
    num_cols = df.select_dtypes(include=['number']).columns.tolist()
    cat_cols = df.select_dtypes(include=['object']).columns.tolist()
    date_cols = [c for c in cols if any(
        word in c.lower() for word in ["date", "month", "year", "quarter", "time"]
    )]

    cat = cat_cols[0] if cat_cols else cols[0]
    num = num_cols[0] if num_cols else cols[-1]
    date = date_cols[0] if date_cols else (cat_cols[0] if cat_cols else cols[0])

    try:
        if chart_type == "Bar":
            df_sorted = df.sort_values(by=num, ascending=False)
            fig = px.bar(
                df_sorted, x=cat, y=num,
                title=f"{num} by {cat}",
                color=num,
                color_continuous_scale="blues",
                text=num,
                hover_data=cols
            )
            fig.update_traces(texttemplate='%{text:.2f}', textposition='outside')
            fig.update_layout(
                coloraxis_showscale=False,
                xaxis_tickangle=-30
            )
            return fig

        elif chart_type == "Horizontal Bar":
            df_sorted = df.sort_values(by=num, ascending=True)
            fig = px.bar(
                df_sorted, x=num, y=cat,
                orientation='h',
                title=f"{num} by {cat}",
                color=num,
                color_continuous_scale="blues",
                text=num,
                hover_data=cols
            )
            fig.update_traces(texttemplate='%{text:.2f}', textposition='outside')
            fig.update_layout(coloraxis_showscale=False)
            return fig

        elif chart_type == "Line":
            fig = px.line(
                df, x=date, y=num,
                title=f"{num} over time",
                markers=True,
                hover_data=cols
            )
            fig.update_layout(xaxis_title=date, yaxis_title=num)
            return fig

        elif chart_type == "Pie":
            fig = px.pie(
                df, names=cat, values=num,
                title=f"Distribution by {cat}",
                hole=0.3
            )
            fig.update_traces(textposition='inside', textinfo='percent+label')
            return fig

        elif chart_type == "Scatter":
            if len(num_cols) >= 2:
                fig = px.scatter(
                    df, x=num_cols[0], y=num_cols[1],
                    title=f"{num_cols[0]} vs {num_cols[1]}",
                    hover_data=cols
                )
            else:
                fig = px.scatter(
                    df, x=cat, y=num,
                    title=f"{num} by {cat}",
                    hover_data=cols
                )
            return fig

    except Exception:
        return None

    return None

# ─── ASK FUNCTION ────────────────────────────────────────────────────────────

def ask(question, conn, schemas, table_names):
    schema_text = "\n\n".join(schemas)

    system_prompt = f"""You are an expert SQLite query writer. Your job is to convert plain English questions into correct SQLite SQL queries.

The database has these tables:
{schema_text}

STRICT RULES:
- Write SELECT queries ONLY. Never use INSERT, UPDATE, DELETE, DROP, ALTER or TRUNCATE.
- Return ONLY the raw SQL query. No explanations, no markdown, no backticks, no comments.
- Always use exact table names: {', '.join(table_names)}
- Always use exact column names as shown in the schema above.
- When joining tables always use the common column between them.
- For TOP N queries always use ORDER BY with LIMIT.
- For percentages use ROUND(100.0 * x / total, 2).
- Always use table aliases when joining (e.g. o.column, c.column).
- If a column has spaces in the original name it has been replaced with underscores.
- SELECT means show rows — do NOT use COUNT unless the user asks to count.
- If the user says "show me" or "list" always return actual rows not a count.

EXAMPLES:
Q: How many rows are there?
A: SELECT COUNT(*) FROM {table_names[0]}

Q: What is the total revenue?
A: SELECT ROUND(SUM(total_amount), 2) as total_revenue FROM {table_names[0]}

Q: Show me the top 5 by total amount
A: SELECT * FROM {table_names[0]} ORDER BY total_amount DESC LIMIT 5

Q: Show me all orders from New York
A: SELECT * FROM {table_names[0]} WHERE city = 'New York'

Q: What is the average order value per group?
A: SELECT customer_segment, ROUND(AVG(total_amount), 2) as avg_order_value FROM {table_names[0]} GROUP BY customer_segment ORDER BY avg_order_value DESC

Q: Which ones have the most entries?
A: SELECT customer_name, COUNT(*) as total_orders FROM {table_names[0]} GROUP BY customer_name ORDER BY total_orders DESC LIMIT 10

Q: What percentage of orders are from each city?
A: SELECT city, ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM {table_names[0]}), 2) as percentage FROM {table_names[0]} GROUP BY city ORDER BY percentage DESC
"""

    # Step 1: question → SQL
    response = ollama.chat(
        model="llama3.1",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user",   "content": question}
        ]
    )
    sql = clean_sql(response["message"]["content"])

    # Step 2: safety check
    if not is_safe(sql):
        return "That query was blocked for safety reasons.", None, None, None

    # Step 3: run query with up to 3 retries
    last_error = None
    for attempt in range(3):
        try:
            cur = conn.cursor()
            cur.execute(sql)
            rows = cur.fetchall()
            columns = [d[0] for d in cur.description]

            if not rows:
                return "No results found for your question.", None, sql, None

            result_df = pd.DataFrame(rows, columns=columns)

            # Step 4: friendly answer
            data_str = result_df.head(10).to_string(index=False)
            friendly = ollama.chat(
                model="llama3.1",
                messages=[{
                    "role": "user",
                    "content": f"Question: {question}\nData:\n{data_str}\nGive a short friendly plain-English answer in 2-3 sentences. Be specific and mention the actual numbers from the data."
                }]
            )

            # Step 5: explain the SQL
            explanation = build_explanation(sql)

            return (
                friendly["message"]["content"].strip(),
                result_df,
                sql,
                explanation
            )

        except Exception as e:
            last_error = e
            response = ollama.chat(
                model="llama3.1",
                messages=[
                    {"role": "system",    "content": system_prompt},
                    {"role": "user",      "content": question},
                    {"role": "assistant", "content": sql},
                    {"role": "user",      "content": f"That failed with: {e}. Please fix the SQL. Remember table names are: {', '.join(table_names)}"}
                ]
            )
            sql = clean_sql(response["message"]["content"])

    return f"Sorry, I couldn't answer that after 3 attempts. Last error: {last_error}", None, None, None

# ─── DISPLAY ANSWER ──────────────────────────────────────────────────────────

def display_answer(msg, idx):
    """Display answer with chart switcher"""
    st.markdown(msg["content"])

    if msg.get("table") is not None:
        st.dataframe(msg["table"], use_container_width=True)

    if msg.get("table") is not None and len(msg["table"]) >= 2:
        chart_types = ["Bar", "Horizontal Bar", "Line", "Pie", "Scatter", "No chart"]

        default = msg.get("default_chart_type", "No chart")
        default_idx = chart_types.index(default) if default in chart_types else len(chart_types) - 1

        selected = st.selectbox(
            "Change chart type",
            options=chart_types,
            index=default_idx,
            key=f"chart_select_{idx}"
        )

        if selected != "No chart":
            chart = build_chart(msg["table"], selected)
            if chart:
                st.plotly_chart(chart, use_container_width=True)

    if msg.get("sql") is not None:
        with st.expander("See generated SQL"):
            st.code(msg["sql"], language="sql")

    if msg.get("explanation") is not None:
        with st.expander("What does this SQL do?"):
            st.markdown(msg["explanation"])

# ─── MAIN APP ────────────────────────────────────────────────────────────────

st.subheader("Load data")

col1, col2 = st.columns([1, 3])
with col1:
    if st.button("Try sample data", use_container_width=True, type="primary"):
        st.session_state.use_sample = True
        st.session_state.messages = []
        st.session_state.active_sheets = "sample"
with col2:
    st.caption("No file? Load our sample sales dataset instantly and start asking questions!")

st.divider()

uploaded_file = st.file_uploader(
    "Or upload your own Excel or CSV file",
    type=["csv", "xlsx"],
    help="Supports CSV and Excel files"
)

use_sample = st.session_state.get("use_sample", False) and uploaded_file is None

if uploaded_file:
    st.session_state.use_sample = False
    use_sample = False

if use_sample or uploaded_file:

    if use_sample:
        sample_path = os.path.join(os.path.dirname(__file__), "sample_data.csv")
        conn, dfs, schemas = load_sheets(sample_path, ["sample_data"])
        table_names = ["sample_data"]
        st.success("Sample sales data loaded! Ask any question below.")
    else:
        if uploaded_file.name.endswith(".xlsx"):
            xl = pd.ExcelFile(uploaded_file)
            sheet_names = xl.sheet_names
        else:
            sheet_names = ["data"]

        st.subheader("Select sheets to query")
        col1, col2 = st.columns(2)
        with col1:
            sheet1 = st.selectbox("Primary sheet", options=sheet_names, index=0)
        with col2:
            sheet2_options = ["None (single sheet only)"] + [s for s in sheet_names if s != sheet1]
            sheet2 = st.selectbox("Second sheet to combine (optional)", options=sheet2_options, index=0)

        selected_sheets = [sheet1]
        if sheet2 != "None (single sheet only)":
            selected_sheets.append(sheet2)

        if len(selected_sheets) == 2:
            st.success(f"Combining **{selected_sheets[0]}** and **{selected_sheets[1]}**!")
        else:
            st.info(f"Querying **{selected_sheets[0]}** only.")

        conn, dfs, schemas = load_sheets(uploaded_file, selected_sheets)
        table_names = [s.lower().replace(" ", "_") for s in selected_sheets]

    # ── Data preview ──
    st.subheader("Data preview")
    for table, df in dfs.items():
        st.caption(f"**{table}** — {len(df)} rows, {len(df.columns)} columns")
        st.dataframe(df.head(3), use_container_width=True)

    # ── Suggested questions ──
    st.subheader("Ask a question")
    suggestions = [
        "What is the total revenue by customer segment?",
        "Show me the top 5 customers by total spending",
        "What percentage of orders are from each city?",
        "Which product category generates the most revenue?",
    ]
    cols = st.columns(2)
    for i, suggestion in enumerate(suggestions):
        with cols[i % 2]:
            if st.button(suggestion, use_container_width=True):
                st.session_state.prefill = suggestion

    # ── Reset chat when data source changes ──
    source_key = "sample" if use_sample else "_".join(table_names)
    if "active_sheets" not in st.session_state or st.session_state.active_sheets != source_key:
        st.session_state.messages = []
        st.session_state.active_sheets = source_key

    # ── Display chat history ──
    for idx, msg in enumerate(st.session_state.messages):
        with st.chat_message(msg["role"]):
            if msg["role"] == "assistant":
                display_answer(msg, idx)
            else:
                st.markdown(msg["content"])

    # ── Chat input ──
    prefill = st.session_state.pop("prefill", "") if "prefill" in st.session_state else ""
    question = st.chat_input("Ask anything about your data...")

    if question or prefill:
        q = question or prefill
        with st.chat_message("user"):
            st.write(q)
        st.session_state.messages.append({"role": "user", "content": q})

        with st.chat_message("assistant"):
            with st.spinner("Thinking..."):
                answer, result_df, sql, explanation = ask(q, conn, schemas, table_names)
                default_chart_type = auto_chart(result_df, q) if result_df is not None else None

            msg = {
                "role": "assistant",
                "content": answer,
                "table": result_df,
                "default_chart_type": default_chart_type,
                "sql": sql,
                "explanation": explanation
            }
            st.session_state.messages.append(msg)
            display_answer(msg, len(st.session_state.messages) - 1)

else:
    st.info("Click 'Try sample data' above or upload your own file to get started!")
    st.markdown("""
    **What you can do:**
    - Try the sample dataset instantly with one click
    - Upload your own CSV or Excel file
    - Combine two sheets and ask questions across both
    - See interactive charts automatically
    - Switch chart types with one click
    - Hover over charts to see all details
    - See the exact SQL generated for every question
    - Understand what each part of the SQL means
    """)