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
  relationship-navigation). For et projekt med 6-7 tabeller er det fint at
  undvære.
- Alternativ: rent sqlite3-modul. SQLAlchemy vælges fordi engine-abstraktionen
  gør SQL-laget nemmere at genbruge - men se README-sektionen
  "SQLite -> SQL Server mapping" for hvorfor det IKKE kun er et
  connection-string-skift.

MIGRATIONS (nyt i denne review-runde):
`CREATE TABLE IF NOT EXISTS` opgraderer IKKE en eksisterende tabel - hvis vi
tilføjer en kolonne til schema.sql, sker der ingenting for en database der
allerede findes. Vi bruger derfor en lille, forståelig migrationsmekanisme:
- migrations/NNN_navn.sql, anvendt i filnavns-rækkefølge
- schema_migrations-tabellen sporer hvilke versioner der er kørt
- hver migration køres i sin egen transaktion; den registreres KUN som
  anvendt hvis hele migrationen lykkedes

BESLUTNING: migrations/ er nu den AUTORITATIVE kilde til sandhed for
skemaet. sql/schema.sql bevares som et menneskelæsbart "sådan ser det
fulde skema ud lige nu"-referencedokument, men bliver ikke længere kørt af
koden - det undgår at have to modstridende kilder til sandhed (se
docs/architecture-decisions.md, ADR om migrations).
"""

from dataclasses import dataclass
from pathlib import Path
from typing import List

from sqlalchemy import create_engine, text, event
from sqlalchemy.engine import Engine

# Central placering af databasefilen.
DB_PATH = Path(__file__).resolve().parent.parent / "data" / "warehouse.db"
SCHEMA_PATH = Path(__file__).resolve().parent.parent / "sql" / "schema.sql"  # reference only
MIGRATIONS_DIR = Path(__file__).resolve().parent.parent / "migrations"


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

    OBS: Dette er IKKE "skift kun connection-string for at bruge SQL Server".
    Se README -> "SQLite -> SQL Server mapping" for hvilke dele af koden
    (datatyper, upsert-syntaks, migrationer, connection-håndtering) der
    reelt skal tilpasses.
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)
    engine = create_engine(f"sqlite:///{db_path}")
    event.listen(engine, "connect", _enable_sqlite_foreign_keys)
    return engine


@dataclass
class Migration:
    version: int
    name: str
    path: Path


def _discover_migrations(migrations_dir: Path) -> List[Migration]:
    """Finder alle NNN_navn.sql-filer, sorteret efter det numeriske præfiks."""
    migrations = []
    for path in sorted(migrations_dir.glob("*.sql")):
        prefix = path.stem.split("_", 1)
        version = int(prefix[0])
        name = prefix[1] if len(prefix) > 1 else path.stem
        migrations.append(Migration(version=version, name=name, path=path))
    return sorted(migrations, key=lambda m: m.version)


def _ensure_migrations_table(engine: Engine) -> None:
    """
    Bootstrap af selve sporings-tabellen. Denne er bevidst IKKE en
    migrationsfil selv - den skal altid kunne oprettes, uanset hvilke
    "rigtige" migrationer der findes, så vi kan spore dem.
    """
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    version     INTEGER PRIMARY KEY,
                    name        TEXT NOT NULL,
                    applied_at  TEXT NOT NULL
                )
                """
            )
        )


def _applied_versions(engine: Engine) -> set:
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT version FROM schema_migrations")).fetchall()
    return {row[0] for row in rows}


def _looks_like_unversioned_legacy_database(engine: Engine) -> bool:
    """
    Lille, bevidst simpel vagt (IKKE et fuldt schema-introspektions-system):
    hvis `schema_migrations` er tom (ingen migration er nogensinde
    registreret som anvendt), men en af kernetabellerne fra migration 001
    allerede findes, er databasen sandsynligvis en UNVERSIONERET database
    fra FØR migrationssystemet blev indført.

    Migration 001 bruger `CREATE TABLE IF NOT EXISTS`, så den vil IKKE fejle
    i den situation - men den vil heller ikke opgradere en tabel med et
    andet/ældre skema. Vi vælger at gøre dette eksplicit og synligt her, i
    stedet for at bygge en generel legacy-migrationsmotor (se
    docs/architecture-decisions.md).
    """
    with engine.connect() as conn:
        legacy_table_exists = conn.execute(
            text(
                "SELECT name FROM sqlite_master "
                "WHERE type='table' AND name='ingested_files'"
            )
        ).fetchone()
    return legacy_table_exists is not None


def _warn_if_unversioned_legacy_database(engine: Engine) -> None:
    if not _looks_like_unversioned_legacy_database(engine):
        return
    if _applied_versions(engine):
        return
    import logging

    logging.getLogger(__name__).warning(
        "Databasen indeholder allerede tabeller (fx 'ingested_files'), men "
        "'schema_migrations' er tom. Dette ligner en UNVERSIONERET database "
        "fra FØR migrationssystemet blev indført - migration 001 bruger "
        "CREATE TABLE IF NOT EXISTS og vil derfor IKKE opgradere en "
        "eksisterende, afvigende tabelstruktur. Migrationshistorikken er "
        "kun autoritativ fra v001 og frem. Anbefaling: slet/genskab "
        "databasefilen, eller baseline den manuelt ved at indsætte de "
        "rigtige rækker i schema_migrations. Se docs/architecture-decisions.md."
    )


def apply_migrations(engine: Engine, migrations_dir: Path = MIGRATIONS_DIR) -> List[int]:
    """
    Anvender alle migrationer der endnu ikke er registreret i
    schema_migrations, i filnavns-rækkefølge.

    Hver migration køres i SIN EGEN transaktion: enten committer BÅDE
    DDL-statements OG registreringen i schema_migrations sammen, eller
    ingen af dem gør (rollback). Det betyder en migration aldrig kan stå
    registreret som "anvendt" uden reelt at være gennemført, og en fejlet
    migration ikke efterlader databasen i en halvvejs-tilstand for netop
    DEN migration.

    Returnerer listen af versionsnumre der blev anvendt i dette kald
    (tom liste hvis databasen allerede var opdateret).
    """
    from datetime import datetime, timezone

    _ensure_migrations_table(engine)
    _warn_if_unversioned_legacy_database(engine)
    already_applied = _applied_versions(engine)

    newly_applied = []
    for migration in _discover_migrations(migrations_dir):
        if migration.version in already_applied:
            continue

        sql = migration.path.read_text()
        with engine.begin() as conn:
            for statement in sql.split(";"):
                statement = statement.strip()
                if statement:
                    conn.execute(text(statement))
            conn.execute(
                text(
                    "INSERT INTO schema_migrations (version, name, applied_at) "
                    "VALUES (:version, :name, :applied_at)"
                ),
                {
                    "version": migration.version,
                    "name": migration.name,
                    "applied_at": datetime.now(timezone.utc).isoformat(),
                },
            )
        newly_applied.append(migration.version)

    return newly_applied


def init_db(engine: Engine, migrations_dir: Path = MIGRATIONS_DIR) -> None:
    """
    Bringer databasen op på den nyeste skema-version ved at anvende alle
    endnu-ikke-anvendte migrationer. Trygt at kalde gentagne gange
    (idempotent: en allerede opdateret database ændres ikke).
    """
    apply_migrations(engine, migrations_dir)


if __name__ == "__main__":
    # Simpel manuel test: opret/opgrader databasen og bekræft det virker.
    engine = get_engine()
    init_db(engine)
    print(f"Database initialiseret på: {DB_PATH}")
