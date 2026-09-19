"""
Text-to-SQL evaluation harness.

Builds a compact schema description straight from the dbt-documented
descriptions in `dbt_project/models/marts/_marts.yml` (the same text dbt
docs renders), asks Claude to translate a set of business questions into
SQL against that schema, executes both the generated SQL and a
hand-written reference query against the DuckDB warehouse, and reports
how often they agree.

This also doubles as a smoke test of the documentation itself: if an LLM
given nothing but the column descriptions can't write correct SQL, a
human analyst reading the same docs probably can't either.

Usage:
    pip install anthropic pandas pyyaml
    export ANTHROPIC_API_KEY=sk-...
    python scripts/generate_mock_data.py
    python scripts/load_raw_to_duckdb.py
    cd dbt_project && dbt run --target ci --profiles-dir . && cd ..
    python scripts/text_to_sql_eval.py
"""

import os
import re
import sys

import duckdb
import pandas as pd
import yaml
import anthropic

BASE_DIR = os.path.join(os.path.dirname(__file__), "..")
DB_PATH = os.path.join(BASE_DIR, "warehouse.duckdb")
MARTS_YML = os.path.join(BASE_DIR, "dbt_project", "models", "marts", "_marts.yml")
REPORT_PATH = os.path.join(BASE_DIR, "text_to_sql_eval_results.md")

MODEL = "claude-sonnet-5"


def build_schema_context() -> str:
    """Render the dbt-documented marts schema as compact text for the LLM prompt."""
    with open(MARTS_YML) as f:
        doc = yaml.safe_load(f)

    lines = []
    for model in doc["models"]:
        lines.append(f"Table: main_marts.{model['name']}")
        if model.get("description"):
            lines.append(f"  Description: {' '.join(model['description'].split())}")
        for col in model.get("columns", []):
            desc = " ".join((col.get("description") or "").split())
            lines.append(f"  - {col['name']}: {desc}")
        lines.append("")
    return "\n".join(lines)


# Each case pairs a natural-language business question with a hand-written
# "ground truth" SQL query. If the LLM's generated SQL produces the same
# result set as the reference query, it's marked a match.
EVAL_CASES = [
    {
        "question": "What is the total net revenue?",
        "reference_sql": """
            select round(sum(net_revenue), 2) as total_net_revenue
            from main_marts.fct_orders
        """,
    },
    {
        "question": "How many orders were placed from each device type?",
        "reference_sql": """
            select device_type, count(*) as order_count
            from main_marts.fct_orders
            group by device_type
            order by device_type
        """,
    },
    {
        "question": "What is the average order value, meaning net revenue per order?",
        "reference_sql": """
            select round(sum(net_revenue) / count(*), 2) as avg_order_value
            from main_marts.fct_orders
        """,
    },
    {
        "question": "Which product category generated the most net revenue?",
        "reference_sql": """
            select category, round(sum(net_revenue), 2) as revenue
            from main_marts.fct_orders
            group by category
            order by revenue desc
            limit 1
        """,
    },
    {
        "question": "How many customers are there in each region?",
        "reference_sql": """
            select region, count(*) as customer_count
            from main_marts.dim_customers
            group by region
            order by region
        """,
    },
    {
        "question": "What percentage of orders were cancelled or refunded, rather than completed?",
        "reference_sql": """
            select round(
                100.0 * sum(case when status in ('cancelled', 'refunded') then 1 else 0 end)
                / count(*), 2
            ) as pct_not_completed
            from main_marts.fct_orders
        """,
    },
    {
        "question": "What is the total net revenue from members versus non-members?",
        "reference_sql": """
            select c.is_member, round(sum(o.net_revenue), 2) as net_revenue
            from main_marts.fct_orders o
            join main_marts.dim_customers c on o.customer_id = c.customer_id
            group by c.is_member
            order by c.is_member
        """,
    },
    {
        "question": "What are the top 3 products by net revenue, by product_id?",
        "reference_sql": """
            select product_id, round(sum(net_revenue), 2) as revenue
            from main_marts.fct_orders
            group by product_id
            order by revenue desc
            limit 3
        """,
    },
]


PROMPT_TEMPLATE = """You are a SQL analyst. You are given a DuckDB database schema and a business question. Write a single DuckDB SQL query that answers the question.

Schema:
{schema}

Question: {question}

Respond with ONLY the SQL query. No explanation, no markdown code fences, no commentary."""


def generate_sql(client: anthropic.Anthropic, schema: str, question: str) -> str:
    response = client.messages.create(
        model=MODEL,
        max_tokens=500,
        messages=[
            {"role": "user", "content": PROMPT_TEMPLATE.format(schema=schema, question=question)}
        ],
    )
    text = "".join(block.text for block in response.content if block.type == "text")
    text = re.sub(r"^```(sql)?|```$", "", text.strip(), flags=re.MULTILINE).strip()
    return text


def results_match(df_a: pd.DataFrame, df_b: pd.DataFrame) -> bool:
    """Order-insensitive, tolerant comparison of two result sets."""
    if df_a.shape != df_b.shape:
        return False
    try:
        a_sorted = df_a.sort_values(by=list(df_a.columns)).reset_index(drop=True)
        b_sorted = df_b.sort_values(by=list(df_b.columns)).reset_index(drop=True)
        pd.testing.assert_frame_equal(
            a_sorted, b_sorted, check_dtype=False, check_exact=False, rtol=1e-2
        )
        return True
    except Exception:
        return False


def run():
    if not os.environ.get("ANTHROPIC_API_KEY"):
        print("Set ANTHROPIC_API_KEY before running this script.")
        sys.exit(1)
    if not os.path.exists(DB_PATH):
        print(f"{DB_PATH} not found — run generate_mock_data.py, load_raw_to_duckdb.py, and dbt run first.")
        sys.exit(1)

    client = anthropic.Anthropic()
    con = duckdb.connect(DB_PATH, read_only=True)
    schema = build_schema_context()

    results = []
    for case in EVAL_CASES:
        question = case["question"]
        reference_sql = case["reference_sql"].strip()

        try:
            generated_sql = generate_sql(client, schema, question)
        except Exception as e:
            results.append(
                {"question": question, "reference_sql": reference_sql,
                 "generated_sql": "", "match": False, "error": f"LLM call failed: {e}"}
            )
            print(f"[ERROR] {question}")
            continue

        try:
            generated_df = con.execute(generated_sql).fetchdf()
            reference_df = con.execute(reference_sql).fetchdf()
            match = results_match(generated_df, reference_df)
            error = None
        except Exception as e:
            match = False
            error = str(e)

        results.append(
            {
                "question": question,
                "reference_sql": reference_sql,
                "generated_sql": generated_sql,
                "match": match,
                "error": error,
            }
        )
        print(f"[{'MATCH' if match else 'MISMATCH'}] {question}")

    con.close()

    correct = sum(r["match"] for r in results)
    accuracy = correct / len(results)
    print(f"\nAccuracy: {accuracy:.0%} ({correct}/{len(results)})")

    write_report(results, accuracy, correct)


def write_report(results, accuracy, correct):
    lines = [
        "# Text-to-SQL Evaluation Results\n",
        f"**Accuracy: {accuracy:.0%}** ({correct}/{len(results)} questions matched the hand-written reference query)\n",
        f"Model: `{MODEL}`. Schema context was generated directly from `dbt_project/models/marts/_marts.yml`",
        "so this also doubles as a check that the dbt documentation is complete enough for an LLM",
        "(not just a human) to write correct SQL against it.\n",
        "---\n",
    ]
    for i, r in enumerate(results, 1):
        status = "✅ MATCH" if r["match"] else "❌ MISMATCH"
        lines.append(f"## {i}. {r['question']} — {status}\n")
        lines.append("**Reference SQL:**")
        lines.append(f"```sql\n{r['reference_sql']}\n```")
        lines.append("**Generated SQL:**")
        lines.append(f"```sql\n{r['generated_sql']}\n```")
        if r["error"]:
            lines.append(f"**Error:** `{r['error']}`")
        lines.append("")

    with open(REPORT_PATH, "w") as f:
        f.write("\n".join(lines))
    print(f"Wrote report: {REPORT_PATH}")


if __name__ == "__main__":
    run()
