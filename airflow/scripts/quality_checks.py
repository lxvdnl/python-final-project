import os

import psycopg2
from sqlalchemy.engine import make_url


DB_URI = os.getenv(
    "PROJECT_DB_URI",
    "postgresql+psycopg2://project_user:project_password@postgres:5432/project_db",
)


def get_db_params():
    url = make_url(DB_URI)
    return {
        "host": url.host,
        "port": url.port,
        "dbname": url.database,
        "user": url.username,
        "password": url.password,
    }


def fetch_one(cur, query: str) -> int:
    cur.execute(query)
    result = cur.fetchone()[0]
    return int(result)


def run_quality_checks():
    conn = psycopg2.connect(**get_db_params())

    try:
        with conn.cursor() as cur:
            checks = {
                "users_count": "SELECT COUNT(*) FROM users",
                "stores_count": "SELECT COUNT(*) FROM stores",
                "drivers_count": "SELECT COUNT(*) FROM drivers",
                "items_count": "SELECT COUNT(*) FROM items",
                "orders_count": "SELECT COUNT(*) FROM orders",
                "order_items_count": "SELECT COUNT(*) FROM order_items",
                "delivery_assignments_count": "SELECT COUNT(*) FROM delivery_assignments",
                "orphan_order_items": """
                    SELECT COUNT(*)
                    FROM order_items oi
                    LEFT JOIN orders o ON oi.order_id = o.order_id
                    WHERE o.order_id IS NULL
                """,
                "orders_without_users": """
                    SELECT COUNT(*)
                    FROM orders o
                    LEFT JOIN users u ON o.user_id = u.user_id
                    WHERE u.user_id IS NULL
                """,
                "orders_without_stores": """
                    SELECT COUNT(*)
                    FROM orders o
                    LEFT JOIN stores s ON o.store_id = s.store_id
                    WHERE s.store_id IS NULL
                """,
                "invalid_replaced_items": """
                    SELECT COUNT(*)
                    FROM order_items oi
                    LEFT JOIN items i ON oi.replaced_by_item_id = i.item_id
                    WHERE oi.replaced_by_item_id IS NOT NULL
                      AND i.item_id IS NULL
                """,
                "duplicate_order_item_groups": """
                    SELECT COUNT(*)
                    FROM (
                        SELECT
                            order_id,
                            item_id,
                            item_price,
                            item_discount,
                            COALESCE(replaced_by_item_id, -1) AS replaced_id
                        FROM order_items
                        GROUP BY
                            order_id,
                            item_id,
                            item_price,
                            item_discount,
                            replaced_id
                        HAVING COUNT(*) > 1
                    ) t
                """,
                "invalid_assignment_dates": """
                    SELECT COUNT(*)
                    FROM delivery_assignments
                    WHERE unassigned_at IS NOT NULL
                      AND unassigned_at < assigned_at
                """,
                "users_non_digit_phones": """
                    SELECT COUNT(*)
                    FROM users
                    WHERE user_phone ~ '[^0-9]'
                """,
                "drivers_non_digit_phones": """
                    SELECT COUNT(*)
                    FROM drivers
                    WHERE driver_phone ~ '[^0-9]'
                """,
            }

            results = {name: fetch_one(cur, query) for name, query in checks.items()}

        empty_tables = [
            name
            for name in [
                "users_count",
                "stores_count",
                "drivers_count",
                "items_count",
                "orders_count",
                "order_items_count",
                "delivery_assignments_count",
            ]
            if results[name] == 0
        ]

        failed_checks = {
            "orphan_order_items": results["orphan_order_items"],
            "orders_without_users": results["orders_without_users"],
            "orders_without_stores": results["orders_without_stores"],
            "invalid_replaced_items": results["invalid_replaced_items"],
            "duplicate_order_item_groups": results["duplicate_order_item_groups"],
            "invalid_assignment_dates": results["invalid_assignment_dates"],
            "users_non_digit_phones": results["users_non_digit_phones"],
            "drivers_non_digit_phones": results["drivers_non_digit_phones"],
        }

        failed_checks = {k: v for k, v in failed_checks.items() if v != 0}

        if empty_tables or failed_checks:
            raise ValueError(
                f"Quality checks failed. Empty tables: {empty_tables}. "
                f"Failed checks: {failed_checks}"
            )

        return results

    finally:
        conn.close()
