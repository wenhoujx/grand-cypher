"""
GrandCypher is a Cypher interpreter for the Grand graph library.

You can use this tool to search Python graph data-structures by
data/attribute or by structure, using the same language you'd use
to search in a much larger graph database.

"""


__version__ = "0.2.0"


from grandcypher.constants import MODELS, TABLES
from grandcypher.parser import _GrandCypherTransformer, _GrandCypherGrammar
import grandcypher.schema as schema
from .schema import show_tables

local_schema = {TABLES: [], MODELS: []}


def show_models(*model_types):
    return schema.show_models(local_schema, *model_types)


def add_model(model_type, table, mappings):
    schema.add_model(local_schema, model_type, table, mappings)


def add_csv_table(table_name, csv_path):
    schema.add_csv_table(local_schema, table_name, csv_path)


def run_cypher(cypher_query):
    t = _GrandCypherTransformer(local_schema)
    t.transform(_GrandCypherGrammar.parse(cypher_query))
    return t.run()
