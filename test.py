import grandcypher as dc
import duckdb

dc.add_csv_table("customer", "data/raw_customers.csv")
dc.add_model(
    "Customer",
    "customer",
    {
        "columns": [
            {
                "name": "first_name",
                "field": "first_name",
                "type": "string",
            },
            {
                "name": "id",
                "field": "id",
                "type": "int",
                "primary": True,
            },
        ]
    },
)

dc.add_model(
    "Company",
    "customer",
    {
        "columns": [
            {"name": "company", "field": "company", "type": "string", "primary": True},
        ]
    },
)


dc.show_models()
dc.show_models("Customer")

print(
    dc.run_cypher(
        """
match (c: Customer) return c limit 10
"""
    ).show()
)
