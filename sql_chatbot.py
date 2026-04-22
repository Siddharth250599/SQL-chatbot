# sql_chatbot.py
import sqlite3
import ollama

# ─── 1. CREATE & SEED THE DATABASE ───────────────────────────────────────────

def setup_database(db_path="shop.db"):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.executescript("""
        CREATE TABLE IF NOT EXISTS customers (
            id         INTEGER PRIMARY KEY,
            name       TEXT NOT NULL,
            email      TEXT UNIQUE NOT NULL,
            city       TEXT,
            created_at DATE DEFAULT CURRENT_DATE
        );
        CREATE TABLE IF NOT EXISTS products (
            id             INTEGER PRIMARY KEY,
            name           TEXT NOT NULL,
            category       TEXT,
            price          REAL NOT NULL,
            stock_quantity INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS orders (
            id           INTEGER PRIMARY KEY,
            customer_id  INTEGER REFERENCES customers(id),
            status       TEXT DEFAULT 'pending',
            order_date   DATE DEFAULT CURRENT_DATE,
            total_amount REAL
        );
        CREATE TABLE IF NOT EXISTS order_items (
            id         INTEGER PRIMARY KEY,
            order_id   INTEGER REFERENCES orders(id),
            product_id INTEGER REFERENCES products(id),
            quantity   INTEGER NOT NULL,
            unit_price REAL NOT NULL
        );
    """)

    if not cur.execute("SELECT 1 FROM customers LIMIT 1").fetchone():
        cur.executescript("""
            INSERT INTO customers (name, email, city) VALUES
              ('Alice Johnson', 'alice@email.com', 'New York'),
              ('Bob Smith',     'bob@email.com',   'Chicago'),
              ('Carol White',   'carol@email.com', 'Houston');

            INSERT INTO products (name, category, price, stock_quantity) VALUES
              ('Laptop Pro',     'Electronics', 1299.99, 15),
              ('Wireless Mouse', 'Electronics',   29.99, 80),
              ('Desk Chair',     'Furniture',    249.99, 20),
              ('USB-C Hub',      'Electronics',   49.99, 50);

            INSERT INTO orders (customer_id, status, order_date, total_amount) VALUES
              (1, 'completed', '2024-01-15', 1329.98),
              (2, 'pending',   '2024-02-10',  249.99),
              (1, 'completed', '2024-03-05',   49.99);

            INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES
              (1, 1, 1, 1299.99),
              (1, 2, 1,   29.99),
              (2, 3, 1,  249.99),
              (3, 4, 1,   49.99);
        """)

    conn.commit()
    return conn

# ─── 2. SCHEMA CONTEXT ───────────────────────────────────────────────────────

SYSTEM_PROMPT = """You are a SQL expert. Convert the user's question into a valid SQLite query.

Tables available:
customers(id, name, email, city, created_at)
products(id, name, category, price, stock_quantity)
orders(id, customer_id→customers.id, status, order_date, total_amount)
order_items(id, order_id→orders.id, product_id→products.id, quantity, unit_price)

Rules:
- Write SELECT queries ONLY. Never use INSERT, UPDATE, DELETE, DROP, or ALTER.
- Return ONLY the raw SQL query, no explanation, no markdown, no backticks.
"""

# ─── 3. SAFETY CHECK ─────────────────────────────────────────────────────────

BLOCKED = ["drop", "delete", "insert", "update", "alter", "truncate"]

def is_safe(sql):
    return not any(word in sql.lower() for word in BLOCKED)

# ─── 4. CORE FUNCTION ────────────────────────────────────────────────────────

def ask(question, conn):
    # Step 1: question → SQL
    response = ollama.chat(
        model="llama3.2",
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user",   "content": question}
        ]
    )
    sql = response["message"]["content"].strip()
    print(f"\n  Generated SQL: {sql}")

    # Step 2: safety check
    if not is_safe(sql):
        return "That query was blocked for safety reasons."

    # Step 3: run the query
    try:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        columns = [d[0] for d in cur.description]
    except Exception as e:
        # Step 4: retry once if there's an error
        response = ollama.chat(
            model="llama3.2",
            messages=[
                {"role": "system",    "content": SYSTEM_PROMPT},
                {"role": "user",      "content": question},
                {"role": "assistant", "content": sql},
                {"role": "user",      "content": f"That failed with: {e}. Please fix the SQL."}
            ]
        )
        sql = response["message"]["content"].strip()
        print(f"  Retried SQL:   {sql}")
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        columns = [d[0] for d in cur.description]

    # Step 5: friendly answer
    if not rows:
        return "No results found."

    data = ", ".join(columns) + "\n"
    for row in rows:
        data += "  " + " | ".join(str(v) for v in row) + "\n"

    friendly = ollama.chat(
        model="llama3.2",
        messages=[{
            "role": "user",
            "content": f"Question: {question}\nData:\n{data}\nGive a short friendly plain-English answer."
        }]
    )
    return friendly["message"]["content"].strip()

# ─── 5. CHAT LOOP ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    conn = setup_database()
    print("\nSQL Chatbot ready! Type 'quit' to exit.")
    print("Try asking:")
    print("  - Which customers are from New York?")
    print("  - What are the top products by price?")
    print("  - How much has Alice spent in total?\n")

    while True:
        question = input("You: ").strip()
        if question.lower() in ("quit", "exit"):
            break
        if not question:
            continue
        answer = ask(question, conn)
        print(f"\nBot: {answer}\n")