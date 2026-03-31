"""
Compensation module — Agent commission tracking for Nadlan Agency.
Registers a Flask Blueprint at /comp and initialises the SQLite database.

Import order matters:
  1. db and compensation_bp are created here
  2. models.py imports db from here
  3. calculator.py imports models
  4. routes.py imports compensation_bp and decorates it
  5. This file imports routes at the bottom so decorators run before
     the blueprint is registered with the Flask app
"""
import os
from flask import Blueprint
from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()
compensation_bp = Blueprint("compensation", __name__, url_prefix="/comp")

ADMIN_KEY = os.environ.get("COMP_ADMIN_KEY", "")

# Import routes here so all @compensation_bp decorators are applied
# before the blueprint is registered with the Flask app in app.py.
from . import routes  # noqa: F401, E402


def init_db(app):
    db_uri = os.environ.get("COMP_DB_URI", "sqlite:///compensation.db")
    app.config.setdefault("SQLALCHEMY_DATABASE_URI", db_uri)
    app.config.setdefault("SQLALCHEMY_TRACK_MODIFICATIONS", False)
    db.init_app(app)
    with app.app_context():
        from . import models  # noqa: F401 — ensures all models are known to SQLAlchemy
        db.create_all()
        _migrate(db)
        from .seed import seed_tiers_if_empty
        seed_tiers_if_empty()


def _migrate(database):
    """Add new columns to existing tables if they don't exist yet."""
    engine = database.engine
    with engine.connect() as conn:
        existing = {row[1] for row in conn.execute(
            database.text("PRAGMA table_info(agents)")
        )}
        new_cols = {
            "target_annual":      "ALTER TABLE agents ADD COLUMN target_annual REAL",
            "target_quarterly":   "ALTER TABLE agents ADD COLUMN target_quarterly REAL",
            "office_tab":         "ALTER TABLE agents ADD COLUMN office_tab TEXT",
            "override_threshold": "ALTER TABLE agents ADD COLUMN override_threshold REAL",
            "override_agent_pct": "ALTER TABLE agents ADD COLUMN override_agent_pct REAL",
        }
        for col, sql in new_cols.items():
            if col not in existing:
                conn.execute(database.text(sql))
        conn.commit()
