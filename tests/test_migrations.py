"""
test_migrations.py
-------------------
Tester den lille migrationsmekanisme i src/database.py.
"""

from pathlib import Path

from sqlalchemy import text

from src.database import get_engine, apply_migrations, MIGRATIONS_DIR


def test_empty_database_upgrades_to_latest_version():
    engine = get_engine(db_path=Path(":memory:"))
    applied = apply_migrations(engine)

    assert applied == [1, 2, 3]
    with engine.connect() as conn:
        versions = [r[0] for r in conn.execute(
            text("SELECT version FROM schema_migrations ORDER BY version")
        ).fetchall()]
    assert versions == [1, 2, 3]


def test_already_current_database_performs_no_migration():
    engine = get_engine(db_path=Path(":memory:"))
    apply_migrations(engine)

    second_call = apply_migrations(engine)

    assert second_call == []


def test_migrations_execute_in_order(tmp_path):
    """Bruger en midlertidig migrations-mappe for at bevise rækkefølgen -
    uden at afhænge af den rigtige projekt-historik."""
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    (migrations_dir / "001_create_a.sql").write_text(
        "CREATE TABLE a (id INTEGER PRIMARY KEY);"
    )
    (migrations_dir / "002_create_b.sql").write_text(
        "CREATE TABLE b (id INTEGER PRIMARY KEY, a_id INTEGER REFERENCES a(id));"
    )

    engine = get_engine(db_path=Path(":memory:"))
    applied = apply_migrations(engine, migrations_dir=migrations_dir)

    assert applied == [1, 2]
    with engine.connect() as conn:
        # Hvis 002 var kørt før 001, ville FOREIGN KEY-referencen til "a"
        # stadig virke i SQLite (ingen validering ved CREATE), men denne
        # test beviser i stedet at ORDER BY version i schema_migrations
        # matcher den forventede rækkefølge.
        rows = conn.execute(text("SELECT name FROM schema_migrations ORDER BY version")).fetchall()
    assert [r[0] for r in rows] == ["create_a", "create_b"]


def test_failed_migration_is_not_recorded_as_successful(tmp_path):
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    (migrations_dir / "001_broken.sql").write_text("CREATE TBLE not_valid_sql (id INTEGER);")

    engine = get_engine(db_path=Path(":memory:"))

    try:
        apply_migrations(engine, migrations_dir=migrations_dir)
        assert False, "Forventede at den ugyldige migration kastede en fejl"
    except Exception:
        pass

    with engine.connect() as conn:
        row = conn.execute(text("SELECT COUNT(*) FROM schema_migrations")).fetchone()
    count = row[0] if row is not None else 0
    assert count == 0


def test_real_project_migrations_apply_cleanly():
    """Sanity check: de faktiske migrations/-filer i projektet anvendes uden fejl."""
    engine = get_engine(db_path=Path(":memory:"))
    applied = apply_migrations(engine, migrations_dir=MIGRATIONS_DIR)
    assert applied == [1, 2, 3]
