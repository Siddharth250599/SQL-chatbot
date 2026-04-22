# evals.py
import sqlite3
import pandas as pd
import ollama
import os
import re

# ─── LOAD SAMPLE DATA ────────────────────────────────────────────────────────

def load_sample_data():
    df = pd.read_csv("ambiguous_test_data.csv")
    df.columns = df.columns.str.strip().str.replace(" ", "_").str.lower()
    conn = sqlite3.connect(":memory:")
    df.to_sql("data", conn, index=False, if_exists="replace")
    return conn, df

# ─── SCHEMA ──────────────────────────────────────────────────────────────────

def get_schema(df):
    columns = ", ".join([f"{col} ({str(df[col].dtype)})" for col in df.columns])
    sample = df.head(2).to_string(index=False)
    return f"""Table: data
Columns: {columns}
Sample:
{sample}"""

# ─── CLEAN SQL ───────────────────────────────────────────────────────────────

def clean_sql(text):
    text = text.strip()
    if "```sql" in text:
        text = text.split("```sql")[1].split("```")[0]
    elif "```" in text:
        text = text.split("```")[1].split("```")[0]
    text = text.strip().rstrip(";").strip()
    return text

# ─── SYSTEM PROMPT ───────────────────────────────────────────────────────────

def get_prompt(schema):
    return f"""You are an expert SQLite query writer. Your job is to convert plain English questions into correct SQLite SQL queries.

The database has these tables:
{schema}

STRICT RULES:
- Write SELECT queries ONLY. Never use INSERT, UPDATE, DELETE, DROP, ALTER or TRUNCATE.
- Return ONLY the raw SQL query. No explanations, no markdown, no backticks, no comments.
- Always use exact table names: data
- Always use exact column names as shown in the schema above.
- For TOP N queries always use ORDER BY with LIMIT.
- For percentages use ROUND(100.0 * x / total, 2).
- For date filtering use SQLite date functions like strftime.
- SELECT means show rows — do NOT use COUNT unless the user asks to count.
- If the user says "show me" or "list" always return actual rows not a count.

EXAMPLES:
Q: How many rows are there?
A: SELECT COUNT(*) FROM data

Q: What is the total revenue?
A: SELECT ROUND(SUM(total_amount), 2) as total_revenue FROM data

Q: Show me the top 5 by total amount
A: SELECT * FROM data ORDER BY total_amount DESC LIMIT 5

Q: Show me all completed orders
A: SELECT * FROM data WHERE status = 'completed'

Q: What is the average order value per segment?
A: SELECT customer_segment, ROUND(AVG(total_amount), 2) as avg_order_value FROM data GROUP BY customer_segment ORDER BY avg_order_value DESC

Q: Show me orders from January 2024
A: SELECT * FROM data WHERE strftime('%Y-%m', order_date) = '2024-01'

Q: What percentage of orders are from each city?
A: SELECT city, ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM data), 2) as percentage FROM data GROUP BY city ORDER BY percentage DESC
"""

# ─── TEST CASES ──────────────────────────────────────────────────────────────

TEST_CASES = [
    # ── Basic ──────────────────────────────────────────────────────────────
    {
        "category": "Basic",
        "description": "Total count",
        "question": "How many orders are there in total?",
        "expected_rows": 1,
        "check_col": None,
        "check_value": None
    },
    {
        "category": "Basic",
        "description": "Simple filter by status",
        "question": "Show me all completed orders",
        "expected_rows": None,
        "check_col": "status",
        "check_value": "completed"
    },
    {
        "category": "Basic",
        "description": "Simple filter by city",
        "question": "Show me all orders from New York",
        "expected_rows": None,
        "check_col": "city",
        "check_value": "New York"
    },
    {
        "category": "Basic",
        "description": "Sort and limit",
        "question": "Show me the top 3 orders by total amount",
        "expected_rows": 3,
        "check_col": None,
        "check_value": None
    },

    # ── Aggregation ────────────────────────────────────────────────────────
    {
        "category": "Aggregation",
        "description": "Sum by group",
        "question": "What is the total revenue by customer segment?",
        "expected_rows": 3,
        "check_col": "customer_segment",
        "check_value": None
    },
    {
        "category": "Aggregation",
        "description": "Average calculation",
        "question": "What is the average order value?",
        "expected_rows": 1,
        "check_col": None,
        "check_value": None
    },
    {
        "category": "Aggregation",
        "description": "Count by group",
        "question": "How many orders are there per product category?",
        "expected_rows": 2,
        "check_col": "category",
        "check_value": None
    },
    {
        "category": "Aggregation",
        "description": "Max value",
        "question": "What is the highest order amount?",
        "expected_rows": 1,
        "check_col": None,
        "check_value": None
    },

    # ── Multiple Conditions ────────────────────────────────────────────────
    {
        "category": "Multiple Conditions",
        "description": "AND condition",
        "question": "Show me completed orders from New York",
        "expected_rows": None,
        "check_col": "city",
        "check_value": "New York"
    },
    {
        "category": "Multiple Conditions",
        "description": "OR condition",
        "question": "Show me orders from Chicago or Houston",
        "expected_rows": None,
        "check_col": None,
        "check_value": None
    },
    {
        "category": "Multiple Conditions",
        "description": "NOT condition",
        "question": "Show me orders that are not completed",
        "expected_rows": None,
        "check_col": "status",
        "check_value": None
    },
    {
        "category": "Multiple Conditions",
        "description": "AND with segment and status",
        "question": "Show me pending orders from Enterprise customers",
        "expected_rows": None,
        "check_col": "customer_segment",
        "check_value": "Enterprise"
    },

    # ── Date Filtering ─────────────────────────────────────────────────────
    {
        "category": "Date Filtering",
        "description": "Filter by month",
        "question": "Show me all orders from January 2024",
        "expected_rows": None,
        "check_col": "order_date",
        "check_value": None
    },
    {
        "category": "Date Filtering",
        "description": "Filter by quarter",
        "question": "Show me all orders from the first quarter of 2024",
        "expected_rows": None,
        "check_col": "order_date",
        "check_value": None
    },
    {
        "category": "Date Filtering",
        "description": "Date calculation between two columns",
        "question": "Show me orders where delivery took more than 5 days",
        "expected_rows": None,
        "check_col": None,
        "check_value": None
    },

    # ── Percentages ────────────────────────────────────────────────────────
    {
        "category": "Percentages",
        "description": "Percentage by city",
        "question": "What percentage of orders are from each city?",
        "expected_rows": None,
        "check_col": "city",
        "check_value": None
    },
    {
        "category": "Percentages",
        "description": "Percentage by status",
        "question": "What percentage of orders are completed?",
        "expected_rows": None,
        "check_col": None,
        "check_value": None
    },

    # ── Advanced ───────────────────────────────────────────────────────────
    {
        "category": "Advanced",
        "description": "Subquery — above average",
        "question": "Show me customers who spent more than the average order amount",
        "expected_rows": None,
        "check_col": "customer_name",
        "check_value": None
    },
    {
        "category": "Advanced",
        "description": "HAVING clause",
        "question": "Which customers have placed more than 1 order?",
        "expected_rows": None,
        "check_col": "customer_name",
        "check_value": None
    },
    {
        "category": "Advanced",
        "description": "Sales rep performance",
        "question": "Which sales rep has the highest total sales?",
        "expected_rows": 1,
        "check_col": "sales_rep",
        "check_value": None
    },
    {
        "category": "Advanced",
        "description": "Revenue and count together",
        "question": "Show me total revenue and number of orders per sales rep",
        "expected_rows": None,
        "check_col": "sales_rep",
        "check_value": None
    },
]

# ─── RUN EVALS ───────────────────────────────────────────────────────────────

def run_evals():
    conn, df = load_sample_data()
    schema = get_schema(df)
    system_prompt = get_prompt(schema)

    results = []
    passed = 0
    failed = 0
    errors = 0

    # Track results by category
    category_results = {}

    print("\n" + "="*60)
    print("RUNNING HARDER EVALS ON AMBIGUOUS TEST DATA")
    print("="*60)

    for i, test in enumerate(TEST_CASES):
        print(f"\nTest {i+1}/{len(TEST_CASES)} [{test['category']}]: {test['description']}")
        print(f"Question: {test['question']}")

        try:
            # Get SQL from AI
            response = ollama.chat(
                model="llama3.1",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user",   "content": test["question"]}
                ]
            )
            sql = clean_sql(response["message"]["content"])
            print(f"Generated SQL: {sql}")

            # Run the SQL
            cur = conn.cursor()
            cur.execute(sql)
            rows = cur.fetchall()
            columns = [d[0] for d in cur.description]
            result_df = pd.DataFrame(rows, columns=columns)

            # Check results
            test_errors = []

            # Check row count if specified
            if test["expected_rows"] is not None:
                if len(result_df) != test["expected_rows"]:
                    test_errors.append(f"Expected {test['expected_rows']} rows but got {len(result_df)}")

            # Check column exists if specified
            if test["check_col"] is not None:
                if test["check_col"] not in result_df.columns:
                    test_errors.append(f"Expected column '{test['check_col']}' not in results")

            # Check specific value if specified
            if test["check_value"] is not None and test["check_col"] is not None:
                if test["check_col"] in result_df.columns:
                    unique_vals = result_df[test["check_col"]].unique().tolist()
                    if test["check_value"] not in unique_vals:
                        test_errors.append(f"Expected value '{test['check_value']}' not found in results")

            # Check it returned something
            if len(result_df) == 0:
                test_errors.append("Query returned no results")

            if test_errors:
                print(f"FAILED ✗ — {', '.join(test_errors)}")
                failed += 1
                status = "FAILED"
            else:
                print(f"PASSED ✓ — {len(result_df)} rows returned")
                passed += 1
                status = "PASSED"

            # Track by category
            cat = test["category"]
            if cat not in category_results:
                category_results[cat] = {"passed": 0, "failed": 0}
            if status == "PASSED":
                category_results[cat]["passed"] += 1
            else:
                category_results[cat]["failed"] += 1

            results.append({
                "category": test["category"],
                "test": test["description"],
                "question": test["question"],
                "generated_sql": sql,
                "status": status,
                "rows_returned": len(result_df),
                "errors": ", ".join(test_errors) if test_errors else "None"
            })

        except Exception as e:
            print(f"ERROR ✗ — {e}")
            errors += 1
            failed += 1
            cat = test["category"]
            if cat not in category_results:
                category_results[cat] = {"passed": 0, "failed": 0}
            category_results[cat]["failed"] += 1
            results.append({
                "category": test["category"],
                "test": test["description"],
                "question": test["question"],
                "generated_sql": "ERROR",
                "status": "ERROR",
                "rows_returned": 0,
                "errors": str(e)
            })

    # ── Print summary ──
    print("\n" + "="*60)
    print("EVAL SUMMARY")
    print("="*60)
    print(f"Total tests:  {len(TEST_CASES)}")
    print(f"Passed:       {passed} ✓")
    print(f"Failed:       {failed} ✗")
    print(f"Score:        {round(passed/len(TEST_CASES)*100)}%")
    print("\nResults by category:")
    print("-"*40)
    for cat, res in category_results.items():
        total = res["passed"] + res["failed"]
        score = round(res["passed"] / total * 100)
        bar = "✓" * res["passed"] + "✗" * res["failed"]
        print(f"{cat:<25} {res['passed']}/{total} ({score}%) {bar}")
    print("="*60)

    # ── Save results ──
    results_df = pd.DataFrame(results)
    results_df.to_csv("eval_results_hard.csv", index=False)
    print(f"\nDetailed results saved to: eval_results_hard.csv")
    print("Open this file to see exactly which tests failed and why!")

    # ── Print failed tests ──
    failed_tests = [r for r in results if r["status"] != "PASSED"]
    if failed_tests:
        print("\nFailed tests to fix:")
        print("-"*40)
        for t in failed_tests:
            print(f"• [{t['category']}] {t['test']}")
            print(f"  Q: {t['question']}")
            print(f"  SQL: {t['generated_sql']}")
            print(f"  Error: {t['errors']}")

if __name__ == "__main__":
    run_evals()