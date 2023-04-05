import duckcypher as dc
import duckdb

dc.add_table_from_csv("customer", "data/raw_customers.csv")
# dc.head_table("customer").show()
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
                "name": "last_name",
                "field": "last_name",
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


dc.add_table_from_csv("infos", "data/customer_infos.csv")
dc.add_model(
    "CustomerInfo",
    "infos",
    {
        "columns": [
            {"name": "id", "field": "id", "type": "int", "primary": True},
            {"name": "age", "field": "age", "type": "int"},
            {"name": "state", "field": "state", "type": "string"},
        ]
    },
)
oldest_google_customer = dc.run_cypher(
    """
    MATCH (info: CustomerInfo") -- (c:Customer)--(g:Company {company: "Google"})
    order by info.age desc
    limit 1 
    return c 
    """


# dc.add_table_from_variable("google_customers", google_customers)

# dc.add_model(
#     "GoogleCustomer",
#     "google_customers",
#     {
#         "columns": [
#             {"name": "first_name", "field": "first_name", "type": "string"},
#             {"name": "id", "field": "id", "type": "int", "primary": True},
#         ]
#     },
# )
