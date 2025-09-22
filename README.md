
# CSV → PostgreSQL Loader (Docker + pandas)

![Docker](https://img.shields.io/badge/Docker-ready-2496ED?logo=docker&logoColor=white)
![Python 3.12](https://img.shields.io/badge/Python-3.12-3776AB?logo=python&logoColor=white)
![PostgreSQL 16](https://img.shields.io/badge/PostgreSQL-16-336791?logo=postgresql&logoColor=white)

A one-command data loader that turns **CSV files** into a **PostgreSQL** database you can browse from **VS Code** or **pgAdmin**. Designed to be dead‑simple for demos, take‑home challenges, and local analytics: drop files in `data/`, run `docker compose up --build`, and the tables appear.

---

## What it does

- Spins up **PostgreSQL 16** in Docker.
- Runs a one‑shot **Python 3.12** loader (pandas + SQLAlchemy + psycopg2) that:
  - Scans a single CSV (`CSV_FILE`) or a folder (`CSV_DIR` + `CSV_GLOB`).
  - **Creates the schema** if needed.
  - **Derives table names** from filenames.
  - **Sanitizes column names** (lowercase, alphanumeric + underscore).
  - **Auto‑parses dates** when ≥80% of values parse.
  - **Optionally sets a PRIMARY KEY** (e.g., `AUTO_PK=id`) if the column is clean (no nulls/dupes).
  - Streams with optional **CHUNKSIZE** for large files.
- Persists data in a named Docker **volume** (`pgdata`), so restarts don’t lose data.

---

## Tech stack

- **PostgreSQL 16** (official image)  
- **Python 3.12**: `pandas`, `sqlalchemy`, `psycopg2-binary`, `python-dotenv`  
- **Docker Compose** for orchestration  
- **VS Code** + SQLTools (or **pgAdmin**)

---

## Project layout

```
.
├─ .env                      # the only file you normally edit
├─ docker-compose.yml        # postgres + one-shot csv loader
├─ Dockerfile.loader         # tiny Python image with deps
├─ csv_to_postgres.py        # env-only loader script (no CLI flags)
├─ data/
│  └─ example.csv            # sample CSV to test
└─ .vscode/
   └─ tasks.json             # helpful docker tasks (optional)
```

---

## Configuration (.env)

```dotenv
# PostgreSQL
POSTGRES_USER=user
POSTGRES_PASSWORD=password
POSTGRES_DB=name_of_db
POSTGRES_PORT=5432
POSTGRES_HOST=postgres   # inside compose; use localhost when connecting from tools

# What to load
# CSV_FILE=./data/example.csv  # single file (optional)
CSV_DIR=./data                 # or load a folder
CSV_GLOB=*.csv

# Loader behavior
SCHEMA_NAME=public
IF_EXISTS=replace              # replace | append | fail
AUTO_PK=id                     # set PK if this column exists & is unique
CHUNKSIZE=                     # e.g., 50000 for very large files
```

---

## Quick start

1) Put your CSVs in `./data/` (or set `CSV_FILE=./data/example.csv` in `.env`).  
2) In VS Code terminal:
   ```bash
   docker compose up --build
   ```
   - Postgres starts and persists data in the `pgdata` volume.
   - The `csv_loader` waits for Postgres, imports your CSV(s), then exits.
3) Connect in VS Code PostgreSQL extension (or pgAdmin):
   - Host: `localhost`
   - Port: `${POSTGRES_PORT}` (default 5432)
   - DB: `${POSTGRES_DB}`
   - User: `${POSTGRES_USER}`
   - Password: `${POSTGRES_PASSWORD}`

### Replace vs Append
Use `IF_EXISTS=replace` to overwrite tables, or `append` to add rows.

### Primary key
If a clean `id` column exists (no nulls or duplicates), it’s set as PRIMARY KEY.  
Change via `AUTO_PK` in `.env`.

### Reset database
If you change DB name/user/password after first run:
```bash
docker compose down -v && docker compose up --build
```

---

## How it works (under the hood)

```
CSV files ──► pandas.read_csv ──► tidy columns ──► infer dates ──► to_sql(method="multi")
                        │                            │
                        └── table name from filename └── optional PRIMARY KEY via ALTER TABLE
```

- The loader reads `.env` (no CLI flags).
- If `CSV_FILE` is set, it loads that file; otherwise it globs `CSV_DIR/CSV_GLOB`.
- Table name = sanitized `Path(csv).stem`.
- Date inference: object columns with ≥80% parse rate become `timestamp`.
- Primary Key: if `AUTO_PK` exists, has no nulls or duplicates, it adds `ALTER TABLE … ADD PRIMARY KEY`.

---

## Connect & verify

**VS Code (SQLTools)**  
Create a connection with:
- Server: `127.0.0.1 / localhost`
- Port: `5432`
- Database: `name_of_db`
- Username: `user`
- Password: `password`
- SSL: Disabled

**Sample queries**
```sql
-- list tables
SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema = 'public'
ORDER BY 1,2;

-- peek
SELECT * FROM public.example LIMIT 20;

-- row count
SELECT COUNT(*) FROM public.example;
```

---

## Troubleshooting

**“password authentication failed” in tools**  
The Postgres image only reads `POSTGRES_*` at **first** init. If you changed `.env` later, the volume still holds old creds.  
Fix:  
```bash
docker compose down -v
docker compose up --build
```
Or alter the role inside the container:
```bash
docker exec -it pg_csv psql -U user -d name_of_db -c "ALTER USER user WITH PASSWORD 'password';"
```

**No tables created**  
- Ensure your CSV(s) are in `./data` (or set `CSV_FILE`), and the loader logs show files found.
- If the CSV is huge, try `CHUNKSIZE=50000`.

**Windows paths**  
Prefer forward slashes or quote the path in `.env`, e.g.  
`CSV_FILE=D:/data/myfile.csv`

---

## Extending this repo (ideas)

- Add **dbt** or **DuckDB** for transformation steps.  
- Expose a **FastAPI** / GraphQL API over the tables.  
- Add **pytest** smoke tests (row counts, PK/unique checks).  
- Add **GitHub Actions** to lint Python and validate compose builds.

---

## License

MIT — use it however you like.
