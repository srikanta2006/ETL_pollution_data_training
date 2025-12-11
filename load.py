import os
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv


def get_supabase_client():
    load_dotenv()
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")

    if not url or not key:
        raise ValueError("Missing SUPABASE_URL or SUPABASE_KEY")

    return create_client(url, key)


def map_dtype(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return "INTEGER"
    if pd.api.types.is_float_dtype(dtype):
        return "DOUBLE PRECISION"
    return "TEXT"


def exec_sql(supabase, query):
    """Execute SQL using RPC wrapper."""
    try:
        res = supabase.postgrest.rpc("sql", {"query": query}).execute()
        return res
    except Exception as e:
        print("⚠️ SQL Exec Error:", e)
        print("Query:", query)
        return None


def ensure_table_exists(table_name, df, supabase):
    try:
        supabase.table(table_name).select("*").limit(1).execute()
        print(f"ℹ️ Table '{table_name}' already exists.")
        return
    except Exception:
        print(f"🆕 Creating table '{table_name}'...")

    cols_sql = []
    for col in df.columns:
        coltype = map_dtype(df[col].dtype)
        cols_sql.append(f'"{col}" {coltype}')

    create_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id BIGSERIAL PRIMARY KEY,
            {", ".join(cols_sql)}
        );
    """

    exec_sql(supabase, create_sql)
    print("✅ Table created.")


def sync_missing_columns(table_name, df, supabase):
    q = f"""
        SELECT column_name FROM information_schema.columns
        WHERE table_name = '{table_name}';
    """

    res = exec_sql(supabase, q)
    if not res or not res.data:
        return

    existing = {r["column_name"] for r in res.data}

    for col in df.columns:
        if col not in existing:
            coltype = map_dtype(df[col].dtype)
            alter = f'ALTER TABLE {table_name} ADD COLUMN "{col}" {coltype};'
            exec_sql(supabase, alter)
            print(f"➕ Added column: {col}")


def load_to_supabase(csv_path, table_name="air_quality_data"):
    supabase = get_supabase_client()

    if not os.path.exists(csv_path):
        print(f"❌ CSV not found: {csv_path}")
        return

    df = pd.read_csv(csv_path)
    df = df.where(pd.notnull(df), None)

    ensure_table_exists(table_name, df, supabase)
    sync_missing_columns(table_name, df, supabase)

    batch_size = 200
    total = len(df)

    print(f"📥 Inserting {total} rows into '{table_name}'...")

    for i in range(0, total, batch_size):
        batch = df.iloc[i:i+batch_size].to_dict(orient="records")
        try:
            supabase.table(table_name).insert(batch).execute()
            print(f"✅ Inserted rows {i+1} → {min(i+batch_size, total)}")
        except Exception as e:
            print(f"⚠️ Error inserting batch {i}: {e}")

    print("🎯 Load complete.")


if __name__ == "__main__":
    csv_path = os.path.join("data", "staged", "air_quality_transformed.csv")
    load_to_supabase(csv_path)
