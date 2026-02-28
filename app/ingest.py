import os
import requests
from datetime import datetime
from dateutil import parser
from sqlalchemy import text
from .db import engine

ADMIN_API_JSON = os.environ.get("ADMIN_API_JSON", "")
ADMIN_AUTH = os.environ.get("ADMIN_AUTH", "")  # Bearer <token>
INGEST_KEY = os.environ.get("INGEST_KEY", "")


def _parse_time(x) -> datetime:
    if isinstance(x, (int, float)):
        return datetime.fromtimestamp(int(x))
    return parser.parse(str(x))


def _extract_items(payload: dict):
    """
    Admin API có thể trả về nhiều dạng:
    - {"data": [ ... ]}
    - {"data": {"items": [ ... ]}}
    - {"items": [ ... ]}
    - {"orders": [ ... ]}
    - {"data": {"orders": [ ... ]}}
    """
    if not isinstance(payload, dict):
        return []

    # thử các đường dẫn phổ biến
    if isinstance(payload.get("data"), list):
        return payload["data"]

    data = payload.get("data")
    if isinstance(data, dict):
        for k in ["items", "orders", "list", "rows", "data"]:
            v = data.get(k)
            if isinstance(v, list):
                return v

    for k in ["items", "orders", "list", "rows"]:
        v = payload.get(k)
        if isinstance(v, list):
            return v

    return []


def ingest_from_admin(from_ts: int, to_ts: int, vehicle_type: str = "Motorcycle"):
    if not ADMIN_API_JSON:
        raise RuntimeError("Missing ADMIN_API_JSON")
    if not ADMIN_AUTH:
        raise RuntimeError("Missing ADMIN_AUTH")

    headers = {
        "Authorization": ADMIN_AUTH,
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

    body = {
        "from_date": int(from_ts),
        "to_date": int(to_ts),
        "vehicle_type": vehicle_type,
    }

    # ✅ Endpoint bạn chụp là POST => dùng requests.post + json=body
    r = requests.post(ADMIN_API_JSON, headers=headers, json=body, timeout=60)

    # Nếu token hết hạn / bị chặn, log text sẽ giúp debug
    if r.status_code >= 400:
        raise RuntimeError(f"Admin API error {r.status_code}: {r.text[:500]}")

    # Đảm bảo là JSON
    try:
        payload = r.json()
    except Exception:
        raise RuntimeError(f"Admin API returned non-JSON: {r.text[:500]}")

    items = _extract_items(payload)
    if not isinstance(items, list):
        raise RuntimeError("Unexpected admin response format (items is not list)")

    inserted_attempts = 0

    with engine.begin() as conn:
        for it in items:
            # map field linh hoạt
            order_id = it.get("id") or it.get("order_id") or it.get("Order ID")
            status = it.get("status") or it.get("Status")
            ct = it.get("create_time") or it.get("created_at") or it.get("Create Time")
            sap_contract_type = it.get("sap_contract_type") or it.get("Sap Contract Type")
            sap_profile_id = it.get("sap_profile_id") or it.get("Sap Profile Id")
            pickup_city = it.get("pickup_city") or it.get("Pickup City")

            if not order_id or not ct:
                continue

            dt = _parse_time(ct)
            d = dt.date()
            h = dt.hour

            conn.execute(text("""
                INSERT INTO orders(
                    order_id, status, create_time, date, hour,
                    sap_contract_type, sap_profile_id, pickup_city
                )
                VALUES(
                    :order_id, :status, :create_time, :date, :hour,
                    :sap_contract_type, :sap_profile_id, :pickup_city
                )
                ON CONFLICT (order_id) DO NOTHING
            """), {
                "order_id": str(order_id),
                "status": status,
                "create_time": dt,
                "date": d,
                "hour": h,
                "sap_contract_type": sap_contract_type,
                "sap_profile_id": sap_profile_id,
                "pickup_city": pickup_city
            })

            inserted_attempts += 1

    return {"fetched": len(items), "inserted_attempts": inserted_attempts}
