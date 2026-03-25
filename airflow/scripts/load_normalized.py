import gc
import glob
import os
from typing import Dict, List

import numpy as np
import re
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from sqlalchemy.engine import make_url


DB_URI = os.getenv(
    "PROJECT_DB_URI",
    "postgresql+psycopg2://project_user:project_password@postgres:5432/project_db",
)

DATA_PATH = os.getenv("RAW_DATA_PATH", "/opt/airflow/data/raw/*.parquet")


def get_parquet_files() -> List[str]:
    files = sorted(glob.glob(DATA_PATH))
    if not files:
        raise FileNotFoundError(f"No parquet files found by path: {DATA_PATH}")
    return files


def get_db_params():
    url = make_url(DB_URI)
    return {
        "host": url.host,
        "port": url.port,
        "dbname": url.database,
        "user": url.username,
        "password": url.password,
    }


def get_pg_connection():
    return psycopg2.connect(**get_db_params())


def build_normalized_data():
    parquet_files = get_parquet_files()

    users_dict: Dict[int, str] = {}
    stores_dict: Dict[int, str] = {}
    drivers_dict: Dict[int, str] = {}
    items_dict: Dict[int, tuple] = {}

    all_orders = []
    all_order_items = []
    all_delivery_assignments = []

    for file_path in parquet_files:
        df = pd.read_parquet(file_path)

        for _, row in (
            df[["user_id", "user_phone"]].drop_duplicates("user_id").iterrows()
        ):
            users_dict[row["user_id"]] = normalize_phone(row["user_phone"])

        for _, row in (
            df[["store_id", "store_address"]].drop_duplicates("store_id").iterrows()
        ):
            stores_dict[row["store_id"]] = row["store_address"]

        driver_subset = (
            df[["driver_id", "driver_phone"]]
            .dropna(subset=["driver_id"])
            .drop_duplicates("driver_id")
        )
        for _, row in driver_subset.iterrows():
            drivers_dict[row["driver_id"]] = normalize_phone(row["driver_phone"])

        for _, row in (
            df[["item_id", "item_title", "item_category"]]
            .drop_duplicates("item_id")
            .iterrows()
        ):
            items_dict[row["item_id"]] = (row["item_title"], row["item_category"])

        orders_current = df.groupby("order_id", as_index=False).agg(
            {
                "user_id": "first",
                "store_id": "first",
                "address_text": "first",
                "created_at": "first",
                "paid_at": "first",
                "delivery_started_at": "first",
                "delivered_at": "first",
                "canceled_at": "first",
                "payment_type": "first",
                "order_discount": "first",
                "order_cancellation_reason": "first",
                "delivery_cost": "first",
            }
        )
        all_orders.append(orders_current)

        temp = df.copy()
        temp["item_replaced_id"] = temp["item_replaced_id"].fillna(-1)

        order_items_current = temp.groupby(
            ["order_id", "item_id", "item_price", "item_discount", "item_replaced_id"],
            as_index=False,
        ).agg(
            {
                "item_quantity": "sum",
                "item_canceled_quantity": "sum",
            }
        )

        order_items_current["replaced_by_item_id"] = order_items_current[
            "item_replaced_id"
        ].replace(-1, np.nan)
        order_items_current = order_items_current.drop(columns=["item_replaced_id"])
        all_order_items.append(order_items_current)

        orders_info = orders_current.set_index("order_id")[
            ["delivered_at", "canceled_at"]
        ].to_dict("index")

        driver_history = (
            df[["order_id", "driver_id", "delivery_started_at"]]
            .dropna(subset=["driver_id", "delivery_started_at"])
            .drop_duplicates()
        )

        delivery_rows = []
        for _, row in driver_history.iterrows():
            order_id = row["order_id"]
            assigned_at = row["delivery_started_at"]
            order_info = orders_info.get(order_id, {})
            delivered_at = order_info.get("delivered_at")
            canceled_at = order_info.get("canceled_at")

            if pd.notna(delivered_at):
                unassigned_at = delivered_at
            elif pd.notna(canceled_at):
                unassigned_at = canceled_at
            else:
                unassigned_at = None

            if pd.notna(unassigned_at) and unassigned_at < assigned_at:
                continue

            delivery_rows.append(
                {
                    "order_id": order_id,
                    "driver_id": row["driver_id"],
                    "assigned_at": assigned_at,
                    "unassigned_at": unassigned_at,
                }
            )

        if delivery_rows:
            all_delivery_assignments.append(pd.DataFrame(delivery_rows))

        del df, temp, orders_current, order_items_current, driver_history, delivery_rows
        gc.collect()

    users = (
        pd.DataFrame(
            [(k, v) for k, v in users_dict.items()], columns=["user_id", "user_phone"]
        )
        .sort_values("user_id")
        .reset_index(drop=True)
    )

    stores = (
        pd.DataFrame(
            [(k, v) for k, v in stores_dict.items()],
            columns=["store_id", "store_address"],
        )
        .sort_values("store_id")
        .reset_index(drop=True)
    )

    drivers = (
        pd.DataFrame(
            [(k, v) for k, v in drivers_dict.items()],
            columns=["driver_id", "driver_phone"],
        )
        .sort_values("driver_id")
        .reset_index(drop=True)
    )

    items = (
        pd.DataFrame(
            [(k, v[0], v[1]) for k, v in items_dict.items()],
            columns=["item_id", "item_title", "item_category"],
        )
        .sort_values("item_id")
        .reset_index(drop=True)
    )

    orders = pd.concat(all_orders, ignore_index=True)
    orders = (
        orders.drop_duplicates(subset=["order_id"])
        .sort_values("order_id")
        .reset_index(drop=True)
    )

    order_items = pd.concat(all_order_items, ignore_index=True)
    order_items = (
        order_items.groupby(
            [
                "order_id",
                "item_id",
                "item_price",
                "item_discount",
                "replaced_by_item_id",
            ],
            dropna=False,
            as_index=False,
        )
        .agg(
            {
                "item_quantity": "sum",
                "item_canceled_quantity": "sum",
            }
        )
        .sort_values(["order_id", "item_id"])
        .reset_index(drop=True)
    )

    if all_delivery_assignments:
        delivery_assignments = pd.concat(all_delivery_assignments, ignore_index=True)
        delivery_assignments = (
            delivery_assignments.drop_duplicates()
            .sort_values(["order_id", "driver_id", "assigned_at"])
            .reset_index(drop=True)
        )
    else:
        delivery_assignments = pd.DataFrame(
            columns=["order_id", "driver_id", "assigned_at", "unassigned_at"]
        )

    del all_orders, all_order_items, all_delivery_assignments
    gc.collect()

    return {
        "users": users,
        "stores": stores,
        "drivers": drivers,
        "items": items,
        "orders": orders,
        "order_items": order_items,
        "delivery_assignments": delivery_assignments,
    }


def truncate_target_tables(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            TRUNCATE TABLE
                delivery_assignments,
                order_items,
                orders,
                items,
                drivers,
                stores,
                users
            RESTART IDENTITY CASCADE;
        """
        )
    conn.commit()


def normalize_value(value):
    if pd.isna(value):
        return None
    if isinstance(value, np.generic):
        return value.item()
    return value


def normalize_phone(phone):
    if pd.isna(phone):
        return None

    digits = re.sub(r"\D", "", str(phone))

    if not digits:
        return None

    if len(digits) == 11 and digits.startswith("8"):
        digits = "7" + digits[1:]

    return digits


def load_table(df: pd.DataFrame, table_name: str, conn, page_size: int = 10000):
    if df.empty:
        return

    columns = list(df.columns)
    values = [
        tuple(normalize_value(v) for v in row)
        for row in df[columns].itertuples(index=False, name=None)
    ]

    insert_sql = f"""
        INSERT INTO {table_name} ({', '.join(columns)})
        VALUES %s
    """

    with conn.cursor() as cur:
        execute_values(cur, insert_sql, values, page_size=page_size)

    conn.commit()


def run_load():
    data = build_normalized_data()

    conn = get_pg_connection()
    try:
        truncate_target_tables(conn)

        load_table(data["users"], "users", conn)
        load_table(data["stores"], "stores", conn)
        load_table(data["drivers"], "drivers", conn)
        load_table(data["items"], "items", conn)
        load_table(data["orders"], "orders", conn)
        load_table(data["order_items"], "order_items", conn)
        load_table(data["delivery_assignments"], "delivery_assignments", conn)

        return {name: len(df) for name, df in data.items()}
    finally:
        conn.close()
