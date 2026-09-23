"""
database.py
------------
Formål: centralisere alt der har med databaseforbindelse at gøre.

Hvorfor SQLAlchemy Core (ikke ORM) her?
- Vi bruger SQLAlchemy som en tynd, database-agnostisk adgangslinje
  (engine + raw SQL via `text()`), ikke den fulde ORM med model-klasser.
- Fordel: koden ligner ren SQL, hvilket er godt til lærings-formålet
  (du vil selv kunne læse/skrive SQL'en).
- Trade-off: vi mister nogle ORM-bekvemmeligheder (automatisk objekt-mapping,
  relationship-navigation). For et projekt med 6 tabeller er det fint at undvære.
- Alternativ: rent sqlite3-modul. SQLAlchemy vælges fordi engine-abstraktionen
  gør det trivielt at skifte til SQL Server senere (bare skift connection string).
"""

from pathlib import Path
from sqlalchemy import create_engine, text, event
from sqlalchemy.engine import Engine

# Central placering af databasefilen.
DB_PATH = Path(__file__).resolve().parent.parent / "data" / "warehouse.db"
SCHEMA_PATH = Path(__file__).resolve().parent.parent / "sql" / "schema.sql"


def _enable_sqlite_foreign_keys(dbapi_connection, connection_record) -> None:
    """
    SQLite håndhæver IKKE foreign keys som standard - det er en per-connection
    indstilling (PRAGMA foreign_keys = ON), ikke noget der er slået til globalt
    for databasefilen. Uden dette ville FOREIGN KEY-definitionerne i schema.sql
    kun være dokumentation, ikke en reel constraint.

    Vi bruger SQLAlchemy's connect-event til at sætte PRAGMA'en på HVER ny
    DBAPI-forbindelse i poolen, så det er trygt uanset connection pooling.
    """
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()


def get_engine(db_path: Path = DB_PATH) -> Engine:
    """
    Opretter en SQLAlchemy engine mod SQLite.

    For at flytte til SQL Server ville man kun ændre denne connection-string,
    fx: "mssql+pyodbc://user:pass@server/db?driver=ODBC+Driver+17+for+SQL+Server"
    Resten af koden (SQL-forespørgslerne) forbliver stort set uændret,
    da vi undgår SQLite-specifik syntaks i vores forretningslogik.
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)
    engine = create_engine(f"sqlite:///{db_path}")
    event.listen(engine, "connect", _enable_sqlite_foreign_keys)
    return engine


def init_db(engine: Engine, schema_path: Path = SCHEMA_PATH) -> None:
    """
    Kører schema.sql mod databasen. Bruger 'IF NOT EXISTS' i schemaet,
    så dette er trygt at køre gentagne gange (idempotent DDL).
    """
    schema_sql = schema_path.read_text()
    with engine.begin() as conn:
        for statement in schema_sql.split(";"):
            statement = statement.strip()
            if statement:
                conn.execute(text(statement))


if __name__ == "__main__":
    # Simpel manuel test: opret databasen og bekræft det virker.
    engine = get_engine()
    init_db(engine)
    print(f"Database initialiseret på: {DB_PATH}")
