import os
from sqlalchemy import create_engine, text

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("Missing DATABASE_URL")

engine = create_engine(DATABASE_URL, pool_pre_ping=True)

def init_db():
    with engine.begin() as conn:
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS orders (
            order_id TEXT PRIMARY KEY,
            status TEXT,
            create_time TIMESTAMP,
            date DATE,
            hour INT,
            sap_contract_type TEXT,
            sap_profile_id TEXT,
            pickup_city TEXT
        );
        """))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_orders_date ON orders(date);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_orders_date_hour ON orders(date, hour);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_orders_city ON orders(pickup_city);"))
