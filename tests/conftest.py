"""
conftest.py
-----------
Delt pytest-fixture: en frisk, in-memory SQLite-database pr. test.

Hvorfor in-memory (':memory:') og ikke fil-baseret databasen?
- Tests skal være hurtige og isolerede fra hinanden og fra din rigtige
  udviklings-database (data/warehouse.db).
- Hver test får sin egen tomme database med schemaet kørt ind, så tests
  ikke kan påvirke hinanden (ingen delt state).
"""

import pytest
from sqlalchemy import create_engine

from src.database import init_db, SCHEMA_PATH


@pytest.fixture
def engine():
    test_engine = create_engine("sqlite:///:memory:")
    init_db(test_engine, schema_path=SCHEMA_PATH)
    return test_engine
