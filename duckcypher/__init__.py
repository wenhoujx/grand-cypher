__version__ = "0.2.0"


from duckcypher.constants import MODELS, TABLES
from duckcypher.parser import _DuckCypherGrammar, _DuckCypherTransformer
import duckcypher.schema as schema
from .schema import show_tables
import duckdb

local_schema = {TABLES: [], MODELS: []}


def show_models(*model_types):
    return schema.show_models(local_schema, *model_types)


def add_model(model_type, table, mappings):
    schema.add_model(local_schema, model_type, table, mappings)


def add_table_from_csv(table_name, csv_path):
    schema.add_csv_table(local_schema, table_name, csv_path)


def add_table_from_variable(table_name, table):
    schema.add_table_from_variable(local_schema, table_name, table)


def head_table(table_name, n=10):
    return duckdb.sql(f"select * from {table_name} limit {n};")


def run_cypher(cypher_query):
    t = _DuckCypherTransformer(local_schema)
    t.transform(_DuckCypherGrammar.parse(cypher_query))
    return t.run()
