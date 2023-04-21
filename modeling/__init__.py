from gettext import find
import tempfile
from typing import Dict
import kuzu
import duckdb
import random
import string
import toolz as tz
from duckcypher.constants import (
    DATA,
    DUCKDB,
    EDGES,
    FIELDS,
    FROM,
    MAPPINGS,
    NAME,
    NODES,
    PRIMARY,
    PROPERTIES,
    TABLES,
    TO,
    TYPE,
)
import logging
import pyarrow.parquet as pq

from duckcypher.schema import primary_field


logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


def _random_id(k=8):
    return "".join(random.choices(string.ascii_letters, k=k))


def load_from_schema(schema: Dict):
    # returns a kuzu connection
    nodes, edges, data = (
        schema[NODES],
        schema.get(EDGES, []),
        schema.get(DATA, []),
    )
    # todo: reuse table?
    db = kuzu.Database(f"./db-{_random_id()}")
    conn = kuzu.Connection(db)
    _create_nodes(conn, nodes)
    _create_edges(conn, edges)
    _copy_data(conn, data)
    return conn


def _copy_data(conn, data_mappings):
    for mapping in data_mappings:
        node_or_edge, duckdb_commands = mapping[TYPE], mapping[DUCKDB]
        with tempfile.NamedTemporaryFile(suffix='.parquet') as temp_file:
            for command in duckdb_commands[:-1]:
                duckdb.execute(command)
            final_result = duckdb.execute(duckdb_commands[-1]).arrow()
            pq.write_table(final_result, temp_file.name)
            conn.execute(f"COPY {node_or_edge} FROM '{temp_file.name}'")


def _create_edges(conn, edges):
    for edge in edges:
        # TODO(whou): add edge properties
        create_string = (
            f"CREATE REL TABLE {edge[NAME]} (FROM {edge[FROM]}  TO {edge[TO]})"
        )
        log.info(f"creating edge: {create_string}")
        conn.execute(create_string)


def _create_nodes(conn, nodes):
    for node in nodes:
        primary_field = _find_primary_field(node)
        property_string = ", ".join(
            tz.thread_last(
                node[PROPERTIES],
                (map, lambda property: f"{property[NAME]} {property[TYPE].upper()}"),
            )
        )
        create_string = f"CREATE NODE TABLE {node[NAME]} ({property_string}, PRIMARY KEY ({primary_field[NAME]}))"
        log.info(f"creating node table: {create_string}")
        conn.execute(create_string)


def _find_primary_field(node):
    primary_field = list(filter(lambda f: f.get(PRIMARY, False), node[PROPERTIES]))
    if not primary_field:
        raise Exception(f"Node {node[NAME]} must have a primary field")
    if len(primary_field) > 1:
        raise Exception(f"Node {node[NAME]} must have only one primary field")
    return primary_field[0]
