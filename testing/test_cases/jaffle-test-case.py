import duckcypher as dc

config = """
tables: 
    - name: payments
    path: ~/code/datas/jaffle-shop/raw_payments.csv
"""
raw_payments_path = ""
raw_orders_path = "~/code/datas/jaffle-shop/raw_orders.csv"
raw_customers_path = "~/code/datas/jaffle-shop/raw_customers.csv"

PAYMENTS = "payments"
ORDERS = "orders"
CUSTOMERS = "customers"
dc.add_table_from_csv(PAYMENTS, raw_payments_path)
dc.add_table_from_csv(ORDERS, raw_orders_path)
dc.add_table_from_csv(CUSTOMERS, raw_customers_path)


dc.add_model("Payment", 
             {
    "columns": [
        {"name": "id", "type": "int", "primary": True},
             }
             )
dc.add_model(
    "Payments",
    PAYMENTS,
    {
        "columns": [
            {"name": "id", "field": "id", "type": "int", "primary": True},
            {"name": "order_id", "field": "order_id", "type": "int"},
            {"name": "amount", "field": "amount", "type": "float"},
            {"name": "payment_method", "field": "payment_method", "type": "string"},
        ]
    },
)

dc.add_model(
    "Orders",
    ORDERS,
    {
        "columns": [
            {"name": "id", "field": "id", "type": "int", "primary": True},
            {"name": "user_id", "field": "user_id", "type": "int"},
            {"name": "order_date", "field": "order_date", "type": "date"},
            {"name": "status", "field": "status", "type": "string"},
        ]
    },
)

dc.add_model(
    "Customers",
    CUSTOMERS,
    {
        "columns": [
            {"name": "id", "field": "id", "type": "int", "primary": True},
            {"name": "first_name", "field": "first_name", "type": "string"},
            {"name": "last_name", "field": "last_name", "type": "string"},
            {"name": "company", "field": "company", "type": "string"},
        ]
    },
)
