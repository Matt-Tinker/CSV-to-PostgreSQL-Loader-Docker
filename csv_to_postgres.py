
from pathlib import Path
import os, re, sys
import pandas as pd
from sqlalchemy import create_engine, text

# Load .env next to this file (safe no-op if missing)
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).with_name(".env"))
except Exception:
    pass

# ---- Config from .env ----
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "")
DB_NAME = os.getenv("POSTGRES_DB", "postgres")
DB_HOST = os.getenv("POSTGRES_HOST", "postgres")   # 'postgres' inside compose; 'localhost' if running on host
DB_PORT = os.getenv("POSTGRES_PORT", "5432")

CSV_FILE = os.getenv("CSV_FILE")
CSV_DIR  = os.getenv("CSV_DIR", "./data")
CSV_GLOB = os.getenv("CSV_GLOB", "*.csv")

SCHEMA    = os.getenv("SCHEMA_NAME", "public")
IF_EXISTS = os.getenv("IF_EXISTS", "replace")      # fail | replace | append
AUTO_PK   = (os.getenv("AUTO_PK") or "id").strip() or None
CHUNKSIZE = (os.getenv("CHUNKSIZE") or "").strip()
CHUNKSIZE = int(CHUNKSIZE) if CHUNKSIZE else None

DB_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def sanitize(name: str, table=False) -> str:
    s = re.sub(r"[^\w]+", "_", name.strip().lower())
    s = re.sub(r"_+", "_", s).strip("_") or ("t" if table else "col")
    if s[0].isdigit():
        s = ("t_" if table else "c_") + s
    return s

def unique_cols(cols):
    seen, out = {}, []
    for c in cols:
        base = sanitize(str(c))
        n = seen.get(base, 0)
        seen[base] = n + 1
        out.append(base if n == 0 else f"{base}_{n}")
    return out

def auto_dates(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.select_dtypes(include=["object"]).columns:
        parsed = pd.to_datetime(df[col], errors="coerce", infer_datetime_format=True)
        if parsed.notna().mean() >= 0.80:
            df[col] = parsed
    return df

def maybe_set_pk(engine, schema: str, table: str, df: pd.DataFrame, pk: str):
    if not pk or pk not in df.columns: return
    if df[pk].isna().any() or df[pk].duplicated().any(): return
    tbl = f'"{schema}"."{table}"' if schema else f'"{table}"'
    try:
        with engine.begin() as conn:
            conn.execute(text(f'ALTER TABLE {tbl} ADD PRIMARY KEY ("{pk}");'))
        print(f"  • PRIMARY KEY set on {schema}.{table}({pk})")
    except Exception as e:
        print(f"  • Could not set PRIMARY KEY on {schema}.{table}({pk}): {e}")

def load_one(path: Path, engine):
    table = sanitize(path.stem, table=True)
    print(f"→ Loading {path} → {SCHEMA}.{table}")
    df = pd.read_csv(path, low_memory=False)
    df.columns = unique_cols(df.columns)
    df = auto_dates(df)

    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA}";'))

    df.to_sql(
        name=table,
        con=engine,
        schema=SCHEMA,
        if_exists=IF_EXISTS,
        index=False,
        method="multi",
        chunksize=CHUNKSIZE,
    )
    print(f"✓ {len(df):,} rows → {SCHEMA}.{table}")
    maybe_set_pk(engine, SCHEMA, table, df, AUTO_PK)

def main():
    base = Path(__file__).resolve().parent

    def resolve(p: str) -> Path:
        pp = Path(p) if p else None
        if not pp: return None
        return pp if pp.is_absolute() else (base / pp)

    # Build list of CSVs to load
    files = []
    single = resolve(CSV_FILE)
    if single and single.exists():
        files = [single]
    else:
        d = resolve(CSV_DIR) or (base / "data")
        if not d.exists():
            sys.exit(f"CSV_DIR not found: {d}")
        files = sorted(d.glob(CSV_GLOB))

    if not files:
        sys.exit("No CSVs found to import. Check CSV_FILE or CSV_DIR/CSV_GLOB in .env.")

    try:
        engine = create_engine(DB_URL, future=True)
    except Exception as e:
        sys.exit(f"Failed to create DB engine: {e}")

    for f in files:
        if not f.exists():
            sys.exit(f"CSV file not found: {f}")
        load_one(f, engine)

if __name__ == "__main__":
    main()
