from errno import ENFILE
from html import entities
import random
import re
from select import select
import string
from sys import getfilesystemencoding
from tkinter import CURRENT
from typing import List
import toolz as tz
import duckdb
from grandcypher.constants import (
    ALIAS,
    ALIASES,
    AND,
    COLUMN,
    ENTITY,
    ENTITY_ID,
    ENTITY_TYPE,
    ENTITY_TYPES,
    FIELD,
    FILTERS,
    MATCH,
    NAME,
    OP,
    OR,
    QUERY,
    RESULT,
    RETURN,
    SELECTS,
    SOURCE,
    SQL,
    TABLE,
    TABLE_NAME,
    TYPE,
    WHERE,
)
from pypika import Field, Table, Query, functions as fn

from grandcypher.schema import find_join_fields, get_field, primary_field, table_name


def _aggregate_op_to_fn(op):
    if op == "sum":
        return fn.Sum
    elif op == "count":
        return fn.Count
    elif op == "max":
        return fn.Max
    elif op == "min":
        return fn.Min
    elif op == "avg":
        return fn.Avg
    elif op == None:
        return lambda x: x
    else:
        raise ValueError(f"Unknown op {op}")


def _split_query(query_list):
    queries = []
    for q in query_list:
        if q[TYPE] == MATCH:
            queries.append({})
        queries[-1].update({q[TYPE]: q[q[TYPE]]})
    return queries


def _process_single_query(schema, query, previous_result):
    previous_table = None
    if previous_result:
        table_alias = shortuuid()
        duckdb.register(table_alias, previous_result[RESULT].fetch_arrow_table())
        table = Table(table_alias)
        previous_table = {
            TABLE: table,
            ENTITY_TYPES: previous_result[ENTITY_TYPES],
            CURRENT: False,
        }

    sql = _process_match_query(
        schema, query[MATCH], query.get(WHERE), query[RETURN], previous_table
    )

    return {
        SQL: sql,  # this is for debugging purposes
        RESULT: duckdb.sql(sql),
        ENTITY_TYPES: {
            **(previous_result[ENTITY_TYPES] if previous_result else {}),
            **{entity[ALIAS]: entity[TYPE] for entity in query[MATCH]},
        },
    }


def process_query(schema, query_list):
    queries = _split_query(query_list)
    query_results = []
    for query in queries:
        query_results.append(
            {
                **_process_single_query(
                    schema, query, query_results[-1] if query_results else None
                ),
                QUERY: query,
            }
        )

    return query_results[-1][RESULT]


def _split_entity_id(entity_id):
    if "." not in entity_id:
        return entity_id, "*"
    else:
        return tuple(entity_id.split("."))


def shortuuid(k=4) -> str:
    return "".join(random.choices(string.ascii_lowercase, k=k))


def _add_previous_result_table(join_tables, previous_result):
    return join_tables


def _process_match_query(schema, match, where, return_clause, previous_table):
    join_tables = _find_join_tables(schema, match)
    if previous_table:
        join_tables.append(previous_table)

    q = Query.from_(join_tables[0][TABLE])
    for i, join_table in enumerate(join_tables[1:], start=1):
        if not join_table[CURRENT]:
            continue
        left, right = find_join_fields(
            schema,
            list(join_tables[i - 1][ENTITY_TYPES].values()),
            list(join_table[ENTITY_TYPES].values()),
        )

        q = q.join(join_table[TABLE]).on(
            Field(left, table=join_tables[i - 1][TABLE])
            == Field(right, table=join_table[TABLE])
        )
    # handle node match exact match where conditions.
    for entity in match:
        entity_alias = entity[ALIAS]
        if entity[FILTERS]:
            for col, val in entity[FILTERS].items():
                target_join_table = _find_target_join_table(join_tables, entity_alias)
                _ignored, field = get_field(
                    schema, target_join_table[ENTITY_TYPES][entity_alias], col
                )
                q = q.where(Field(field, table=target_join_table[TABLE]) == val)
    if where:
        # handle explicit where clause
        q = q.where(_process_where(schema, join_tables, where))
    # handle return clause
    select_terms = []
    for ret in return_clause:
        entity_alias, col = _split_entity_id(ret[ENTITY_ID])
        target_table = _find_target_join_table(join_tables, entity_alias)
        op = ret.get(OP)
        if col == "*" and op == "count":
            select_terms.append(_aggregate_op_to_fn(op)("*"))
        elif col == "*":
            select_terms.append(_aggregate_op_to_fn(op)(target_table[TABLE].star))
        else:
            _ignored, field = get_field(
                schema, target_table[ENTITY_TYPES][entity_alias], col
            )
            select_terms.append(
                _aggregate_op_to_fn(op)(Field(field, table=target_table[TABLE]))
            )

    q = q.select(*select_terms)
    return q.get_sql()


def _find_target_join_table(join_tables, entity_alias):
    return tz.first(filter(lambda jt: entity_alias in jt[ENTITY_TYPES], join_tables))


def get_primary_field(schema, entity_type, table):
    id_field = primary_field(schema, entity_type)
    return Field(id_field, table=table)


def _find_join_tables(schema, entities):
    # return a list [{table, aliases}]
    split_entities = [[entities[0]]]

    # try merge adjacent sources backed by the same table.
    for entity in entities[1:]:
        if all(
            table_name(schema, entity[TYPE]) == table_name(schema, src[TYPE])
            and entity[TYPE] != src[TYPE]
            for src in split_entities[-1]
        ):
            # backed by the same table, but represents different entities, this case, we don't have to do any special joins.
            split_entities[-1].append(entity)
        else:
            split_entities.append([entity])
    join_tables = []
    for split in split_entities:
        # each split entity group is backed by the same table.
        first_alias = split[0][ALIAS]
        first_entity_type = split[0][TYPE]
        table = Table(table_name(schema, first_entity_type)).as_(first_alias)
        join_tables.append(
            {
                CURRENT: True,
                TABLE: table,
                ENTITY_TYPES: dict(
                    map(lambda entity: (entity[ALIAS], entity[TYPE]), split)
                ),
            }
        )
    return join_tables


def _process_where(schema, join_tables, where):
    if where is None:
        raise ValueError("where clause cannot be None")

    if where[0] == AND:
        return _process_where(schema, join_tables, where[1]) & _process_where(
            schema, join_tables, where[2]
        )
    elif where[0] == OR:
        return _process_where(schema, join_tables, where[1]) | _process_where(
            schema, join_tables, where[2]
        )
    else:
        entity_id, op, entity_id_or_value = where
        entity, col = entity_id.split(".")
        target = tz.first(filter(lambda t: entity in t[ENTITY_TYPES], join_tables))
        left_field = Field(
            get_field(schema, target[ENTITY_TYPES][entity], col)[1], table=target[TABLE]
        )
        if isinstance(entity_id_or_value, str) and "." in entity_id_or_value:
            entity, col = entity_id_or_value.split(".")
            target = tz.first(filter(lambda t: entity in t[ENTITY_TYPES], join_tables))
            field = get_field(schema, target[ENTITY_TYPES][entity], col)[1]
            table = None
            if not target[CURRENT]:
                right_field = Query.from_(target[TABLE]).select(
                    Field(field, table=target[TABLE])
                )
            else:
                right_field = Field(field, table=target[TABLE])

        else:
            right_field = entity_id_or_value
        return _condition_op_to_fn(op)(left_field, right_field)


def _condition_op_to_fn(op):
    if op == "eq":
        return lambda x, y: x == y
    elif op == "neq":
        return lambda x, y: x != y
    elif op == "gt":
        return lambda x, y: x > y
    elif op == "gte":
        return lambda x, y: x >= y
    elif op == "lt":
        return lambda x, y: x < y
    elif op == "lte":
        return lambda x, y: x <= y
    else:
        raise ValueError(f"unknown op {op}")
