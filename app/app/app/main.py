from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from sqlalchemy import text
from datetime import datetime, timedelta

from .db import engine, init_db
from .ingest import ingest_from_admin, INGEST_KEY

app = FastAPI(title="Xanh Dashboard API")

# tạo bảng + index khi service start
init_db()


@app.get("/")
def home():
    return HTMLResponse("""
    <h2>✅ Xanh Dashboard API đang chạy</h2>
    <p>Vào <a href="/docs">/docs</a> để test API</p>
    <ul>
      <li><b>Ingest:</b> /ingest?from_ts=...&to_ts=...&key=...</li>
      <li><b>Hourly:</b> /api/hourly?date=YYYY-MM-DD&city=Thành%20phố%20Hà%20Nội&type=bike</li>
      <li><b>KPI:</b> /api/kpi?date=YYYY-MM-DD&city=...&type=bike</li>
    </ul>
    """)


@app.get("/ingest")
def ingest(from_ts: int, to_ts: int, key: str = "", vehicle_type: str = "Motorcycle"):
    # bảo vệ ingest
    if INGEST_KEY and key != INGEST_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")
    return ingest_from_admin(from_ts, to_ts, vehicle_type=vehicle_type)


@app.get("/api/hourly")
def hourly(date: str, city: str | None = None, type: str | None = None):
    """
    Trả về tổng đơn theo giờ (00-23) cho 1 ngày
    type = sap_contract_type (bike / bike_platform)
    """
    where = ["date = :date"]
    params = {"date": date}

    if city:
        where.append("pickup_city = :city")
        params["city"] = city
    if type:
        where.append("sap_contract_type = :type")
        params["type"] = type

    where_sql = " AND ".join(where)

    with engine.begin() as conn:
        rows = conn.execute(text(f"""
            SELECT hour,
                   COUNT(*) AS total,
                   SUM(CASE WHEN status='COMPLETED' THEN 1 ELSE 0 END) AS completed
            FROM orders
            WHERE {where_sql}
            GROUP BY hour
            ORDER BY hour
        """), params).fetchall()

    return [{"hour": r[0], "total": int(r[1]), "completed": int(r[2] or 0)} for r in rows]


@app.get("/api/kpi")
def kpi(date: str, city: str | None = None, type: str | None = None):
    """
    KPI ngày: total, completed, fr + DoD/WoW cho total & completed
    (TX active sẽ bổ sung sau nếu có driver_id)
    """

    def count_for(d: str):
        where = ["date = :date"]
        params = {"date": d}
        if city:
            where.append("pickup_city = :city")
            params["city"] = city
        if type:
            where.append("sap_contract_type = :type")
            params["type"] = type

        where_sql = " AND ".join(where)

        with engine.begin() as conn:
            total = conn.execute(text(f"SELECT COUNT(*) FROM orders WHERE {where_sql}"), params).scalar() or 0
            completed = conn.execute(
                text(f"SELECT COUNT(*) FROM orders WHERE {where_sql} AND status='COMPLETED'"),
                params
            ).scalar() or 0

        fr = (completed / total) if total else 0.0
        return int(total), int(completed), fr

    d = datetime.strptime(date, "%Y-%m-%d").date()
    d1 = (d - timedelta(days=1)).strftime("%Y-%m-%d")
    d7 = (d - timedelta(days=7)).strftime("%Y-%m-%d")

    total, completed, fr = count_for(date)
    total_d1, completed_d1, fr_d1 = count_for(d1)
    total_d7, completed_d7, fr_d7 = count_for(d7)

    def pct(now, prev):
        if prev == 0:
            return None
        return (now - prev) / prev

    return {
        "total": total,
        "completed": completed,
        "fr": fr,

        # chưa có nguồn TX active thì để None
        "tx_active": None,
        "tb_request_tx": None,

        "dod_total_pct": pct(total, total_d1),
        "wow_total_pct": pct(total, total_d7),

        "dod_completed_pct": pct(completed, completed_d1),
        "wow_completed_pct": pct(completed, completed_d7),

        # so sánh fr theo tỷ lệ thay đổi tương đối (tuỳ bạn có muốn)
        "dod_fr_pct": pct(fr, fr_d1) if fr_d1 else None,
        "wow_fr_pct": pct(fr, fr_d7) if fr_d7 else None,

        "dod_tx_pct": None,
        "wow_tx_pct": None,
        "dod_tb_pct": None,
        "wow_tb_pct": None,
    }
