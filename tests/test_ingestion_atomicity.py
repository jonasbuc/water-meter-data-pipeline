"""
test_ingestion_atomicity.py
----------------------------
Tester at fil-ingestion er atomisk på tværs af "claim fil" + "indsæt raw" +
"markér PROCESSED", og at pipelinen kan komme sig efter et simuleret crash
uden at duplikere raw-rækker eller efterlade filer i en uklar tilstand.

Tester også den eksplicitte kilde-kontrakt (schema drift-håndtering).
"""

from pathlib import Path

import pytest
from sqlalchemy import text

from src.ingestion import ingest_file, ingest_directory
import src.ingestion as ingestion_module


def _write_csv(path: Path, rows: str = "M-001,2026-09-20T08:00:00,50.0,14.0,OK\n") -> None:
    path.write_text(
        "meter_id,timestamp,consumption_liters,temperature,status\n" + rows
    )


def test_successful_file_ingestion(engine, tmp_path):
    file_path = tmp_path / "a.csv"
    _write_csv(file_path)

    rows = ingest_file(engine, file_path)

    assert rows == 1
    with engine.connect() as conn:
        status = conn.execute(
            text("SELECT status, row_count FROM ingested_files")
        ).fetchone()
    assert status.status == "PROCESSED"
    assert status.row_count == 1


def test_same_file_ingested_twice_is_skipped(engine, tmp_path):
    file_path = tmp_path / "a.csv"
    _write_csv(file_path)

    first = ingest_file(engine, file_path)
    second = ingest_file(engine, file_path)

    assert first == 1
    assert second == 0
    with engine.connect() as conn:
        raw_count = conn.execute(text("SELECT COUNT(*) FROM raw_meter_readings")).fetchone()[0]
    assert raw_count == 1


def test_simulated_database_failure_while_inserting_raw_rows_leaves_no_orphaned_data(
    engine, tmp_path, monkeypatch
):
    """
    Simulerer at selve raw-indsættelsen fejler midt i transaktionen (fx en
    forbindelsesfejl). Beviser at:
      - INGEN raw-rækker er committet
      - filen er markeret FAILED (ikke hængende som PENDING)
    """
    file_path = tmp_path / "a.csv"
    _write_csv(file_path)

    original_execute = None

    class _BoomOnRawInsert:
        def __init__(self, conn):
            self._conn = conn

        def execute(self, statement, *args, **kwargs):
            sql_text = str(statement)
            if "INSERT INTO raw_meter_readings" in sql_text:
                raise RuntimeError("Simuleret database-fejl under raw-insert")
            return self._conn.execute(statement, *args, **kwargs)

    # Vi patcher ikke selve SQLAlchemy - i stedet injicerer vi fejlen via en
    # lille wrapper omkring engine.begin(), for at holde testen simpel og
    # undgå at mocke SQLAlchemy internals for dybt.
    import contextlib

    real_begin = engine.begin

    @contextlib.contextmanager
    def _patched_begin():
        with real_begin() as conn:
            yield _BoomOnRawInsert(conn)

    monkeypatch.setattr(engine, "begin", _patched_begin)

    with pytest.raises(RuntimeError):
        ingest_file(engine, file_path)

    monkeypatch.undo()

    with engine.connect() as conn:
        raw_count = conn.execute(text("SELECT COUNT(*) FROM raw_meter_readings")).fetchone()[0]
        file_status = conn.execute(text("SELECT status FROM ingested_files")).fetchone()

    assert raw_count == 0
    assert file_status.status == "FAILED"


def test_simulated_failure_before_marking_processed_leaves_no_orphaned_raw_rows(
    engine, tmp_path, monkeypatch
):
    """
    Simulerer at UPDATE ... SET status='PROCESSED' fejler. Fordi hele
    operationen er ÉN transaktion, skal dette rulle raw-indsættelsen
    tilbage også - IKKE efterlade raw-rækker uden en færdig fil-markering.
    """
    file_path = tmp_path / "a.csv"
    _write_csv(file_path)

    import contextlib

    real_begin = engine.begin

    class _BoomOnMarkProcessed:
        def __init__(self, conn):
            self._conn = conn

        def execute(self, statement, *args, **kwargs):
            sql_text = str(statement)
            if "SET status = 'PROCESSED'" in sql_text:
                raise RuntimeError("Simuleret fejl før markering som PROCESSED")
            return self._conn.execute(statement, *args, **kwargs)

    @contextlib.contextmanager
    def _patched_begin():
        with real_begin() as conn:
            yield _BoomOnMarkProcessed(conn)

    monkeypatch.setattr(engine, "begin", _patched_begin)

    with pytest.raises(RuntimeError):
        ingest_file(engine, file_path)

    monkeypatch.undo()

    with engine.connect() as conn:
        raw_count = conn.execute(text("SELECT COUNT(*) FROM raw_meter_readings")).fetchone()[0]
        file_status = conn.execute(text("SELECT status FROM ingested_files")).fetchone()

    assert raw_count == 0  # ATOMICITET: raw-indsættelsen blev rullet tilbage
    assert file_status.status == "FAILED"


def test_retry_after_failure_succeeds_without_duplicating_raw_rows(engine, tmp_path, monkeypatch):
    """
    Integrationstest for hele "crash -> retry"-løftet:
    1. Første forsøg fejler kunstigt -> FAILED, ingen raw-rækker.
    2. Andet (normale) forsøg lykkes -> PROCESSED, raw-rækker indsat ÉN gang.
    """
    file_path = tmp_path / "a.csv"
    _write_csv(file_path)

    import contextlib

    real_begin = engine.begin

    class _BoomOnce:
        def __init__(self, conn):
            self._conn = conn

        def execute(self, statement, *args, **kwargs):
            if "INSERT INTO raw_meter_readings" in str(statement):
                raise RuntimeError("Simuleret engangsfejl")
            return self._conn.execute(statement, *args, **kwargs)

    @contextlib.contextmanager
    def _patched_begin():
        with real_begin() as conn:
            yield _BoomOnce(conn)

    monkeypatch.setattr(engine, "begin", _patched_begin)
    with pytest.raises(RuntimeError):
        ingest_file(engine, file_path)
    monkeypatch.undo()

    # Retry - normal kørsel, samme fil
    rows = ingest_file(engine, file_path)

    assert rows == 1
    with engine.connect() as conn:
        raw_count = conn.execute(text("SELECT COUNT(*) FROM raw_meter_readings")).fetchone()[0]
        status = conn.execute(text("SELECT status FROM ingested_files")).fetchone()
    assert raw_count == 1
    assert status.status == "PROCESSED"


def test_missing_required_column_fails_the_file(engine, tmp_path):
    """Schema-drift: manglende PÅKRÆVET kolonne skal fejle filen synligt."""
    file_path = tmp_path / "bad.csv"
    file_path.write_text("timestamp,consumption_liters\n2026-09-20T08:00:00,50.0\n")

    with pytest.raises(ValueError):
        ingest_file(engine, file_path)

    with engine.connect() as conn:
        status = conn.execute(text("SELECT status FROM ingested_files")).fetchone()
    assert status.status == "FAILED"


def test_extra_unexpected_column_is_accepted(engine, tmp_path):
    """Schema-drift: ekstra/ukendte kolonner accepteres (ikke en fejl)."""
    file_path = tmp_path / "extra.csv"
    file_path.write_text(
        "meter_id,timestamp,consumption_liters,temperature,status,new_field\n"
        "M-001,2026-09-20T08:00:00,50.0,14.0,OK,hello\n"
    )

    rows = ingest_file(engine, file_path)
    assert rows == 1


def test_empty_but_valid_csv_is_success_with_zero_rows(engine, tmp_path):
    """En tom, men i øvrigt gyldig fil (kun header) er SUCCESS med 0 rækker."""
    file_path = tmp_path / "empty.csv"
    file_path.write_text("meter_id,timestamp,consumption_liters,temperature,status\n")

    rows = ingest_file(engine, file_path)

    assert rows == 0
    with engine.connect() as conn:
        status = conn.execute(text("SELECT status FROM ingested_files")).fetchone()
    assert status.status == "PROCESSED"  # succesfuldt behandlet, bare 0 rækker


def test_json_object_instead_of_list_fails_explicitly(engine, tmp_path):
    """Schema-drift: JSON-objekt i stedet for liste skal fejle med en klar besked."""
    file_path = tmp_path / "bad.json"
    file_path.write_text('{"meter_id": "M-001", "timestamp": "2026-09-20T08:00:00"}')

    with pytest.raises(ValueError):
        ingest_file(engine, file_path)
