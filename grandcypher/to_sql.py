from html import entities
import re
from select import select
from sys import getfilesystemencoding
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
    ENTITY_TYPES,
    FIELD,
    FILTERS,
    NAME,
    OP,
    OR,
    SELECTS,
    SOURCE,
    SQL,
    TABLE,
    TABLE_NAME,
    TYPE,
)
from pypika import Field, Table, Query, functions as fn

from grandcypher.schema import find_join_fields, get_field, primary_field, table_name


def _join_entities(schema, entities):
    for i, entity in enumerate(entities):
        entity.update(
            {
                TABLE: Table(table_name(schema, entity[ENTITY][TYPE])).as_(
                    tz.first(entity[ENTITY_TYPES].keys())
                ),
            }
        )
        entity.update(
            {
                # the source of the first table is the table itself.
                SOURCE: entity[TABLE]
                if i == 0
                else _later_sql_source(schema, entity),
            }
        )

    q = _first_sql_source(schema, entities[0])

    select_terms = []
    for i, entity in enumerate(entities):
        select_terms += _compute_fields(entity[SOURCE], entity[SELECTS])

    q = q.select(*select_terms)
    # add possible joins.
    for i in range(1, len(entities)):
        entity = entities[i]
        prev_source = entities[i - 1]
        field_a, field_b = find_join_fields(
            schema,
            list(prev_source[ENTITY_TYPES].values()),
            list(entity[ENTITY_TYPES].values()),
        )
        q = q.join(entity[SOURCE]).on(
            # join on the outer table if prev is the first source, otherwise join on the prev source.
            Field(
                field_a,
                table=(prev_source[SOURCE]),
            )
            == Field(field_b, table=entity[SOURCE])
        )
    return q


def _execute_query(query):
    res = duckdb.sql(query.get_sql())
    return res


def _build_final_query(schema, match_results, where, return_clause):
    ...


def _no_aggregate_return(return_clause):
    return not any([ret.get(OP) for ret in return_clause])


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


def process_query(schema, query):
    matches = []
    for match in query["matches"]:
        matches.append(
            _process_match_query(schema, match, query.get("where"), query["return"])
        )
    match_results = [
        {"result": _execute_query(match[SQL]), ENTITY_TYPES: match[ENTITY_TYPES]}
        for match in matches
    ]

    if (
        len(match_results) == 1
        and not query.get("where")
        and _no_aggregate_return(query["return"])
    ):
        return match_results[0]["result"].fetchall()
    elif len(match_results) == 1:
        # single match with aggregate return
        table_name = tz.first(match_results[0][ENTITY_TYPES].keys())
        # we are telling duckdb this variable is a result table, since duckdb doesn't support array variables.
        exec(f"{table_name} = match_results[0]['result']")
        table = Table(table_name)
        q = Query.from_(table)
        select_terms = []
        for ret in query["return"]:
            op = ret.get(OP, None)
            entity_id = ret[ENTITY_ID]
            if "." not in entity_id:
                select_terms.append(_aggregate_op_to_fn(op)(table.star))
            else:
                entity_alias, col = entity_id.split(".")
                _ignored, field = get_field(
                    schema, match_results[0][ENTITY_TYPES][entity_alias], col
                )
                select_terms.append(_aggregate_op_to_fn(op)(Field(field, table=table)))
        q = q.select(*select_terms)
        if query.get("where"):
            q = q.where(
                _process_where(
                    schema, table, match_results[0][ENTITY_TYPES], query.get("where")
                )
            )

        sql = q.get_sql()

        return duckdb.sql(sql).fetchall()

    # need to do another query that joins the results of the match queries.
    where = _gather_entity_id_from_where(
        [match[ALIASES] for match in matches], query.get("where")
    )
    if not where:
        raise ValueError("No where clause found for join query.")

    return_clause = query["return"]
    q = _build_final_query(schema, match_results, where, return_clause)
    res = _execute_query(q)
    return res


def _entity_id_belongs(entity_id, entity_alias):
    return entity_id.split(".")[0] == entity_alias


def _split_entity_id(entity_id):
    if "." not in entity_id:
        return entity_id, "*"
    else:
        return tuple(entity_id.split("."))


def _find_selects(aliases, return_clause, where):
    # find selects , return [(entity, column)...]
    selects = []

    for ret in return_clause:
        entity_alias = ret[ENTITY_ID].split(".")[0]
        if entity_alias in aliases:
            selects.append(_split_entity_id(ret[ENTITY_ID]))

    if where:
        selects += _gather_entity_id_from_where(aliases, where)
    return selects


def _gather_entity_id_from_where(aliases: List[str], where):
    # recursively find the entity_id needs to be selected in where clause.
    if where[0] == AND or where[0] == OR:
        return _gather_entity_id_from_where(
            aliases, where[1]
        ) + _gather_entity_id_from_where(aliases, where[2])
    else:
        # if where[2] is an entity_id , include too
        entity_id_a = where[0]
        entity_id_b = where[2]
        selects = []
        if entity_id_a.split(".")[0] in aliases:
            selects.append(_split_entity_id(entity_id_a))
        if (
            isinstance(entity_id_b, str)
            and "." in entity_id_b
            and entity_id_b.split(".")[0] in aliases
        ):
            selects.append(_split_entity_id(entity_id_b))
        return selects


def _process_match_query(schema, match, where, return_clause):
    all_aliases = set(tz.thread_last(match, (map, lambda entity: entity[ALIAS])))

    entities = _merge_entities(
        schema, match, _find_selects(all_aliases, return_clause, where)
    )
    # join the entities together.
    q = _join_entities(schema, entities)
    # add where
    # q = _add_where(q, entities, where)

    return {
        SQL: q,
        ENTITY_TYPES: dict(tz.merge([source[ENTITY_TYPES] for source in entities])),
    }


def _update_entity(entity, selects):
    entity_type = entity[TYPE]
    entity_alias = entity[ALIAS]
    filters = []
    for col, val in entity[FILTERS].items():
        # these filters are of the form match ... {col: val}
        filters.append((entity_type, col, val))

    return {
        ENTITY: entity,
        # contains the list of alias and their entity types.
        ENTITY_TYPES: {entity_alias: entity_type},
        FILTERS: filters,
        SELECTS: list(filter(lambda sel: sel[0] == entity_alias, selects)),
    }


def _merge_sources(sources):
    return {
        **sources[0],
        ENTITY_TYPES: dict(tz.merge(*[src[ENTITY_TYPES] for src in sources])),
        FILTERS: list(tz.concat([src[FILTERS] for src in sources])),
        SELECTS: list(tz.concat([src[SELECTS] for src in sources])),
    }


def _later_sql_source(schema, entity):
    alias = tz.first(entity[ENTITY_TYPES].keys())
    if not entity[FILTERS]:
        # return as plain table.
        q = entity[TABLE]
    else:
        # if filters present, we need to use a subquery with select, where, ...
        # TODO pretty weird that the inner and out share the same alias.
        table = entity[TABLE]
        q = Query.from_(table)
        for entity_type, col, val in entity[FILTERS]:
            _ignored, field = get_field(schema, entity_type, col)
            q = q.where(Field(field, table=table) == val)
        # wrap the subquery in the same alias as the inner table.
        q = q.as_(alias)
        # if filters, we need to select the fields we want to return
        select_terms = _compute_fields(q, entity[SELECTS])
        # include join field which is the primary field.
        select_terms += [
            get_primary_field(schema, entity_type, table)
            for entity_type in entity[ENTITY_TYPES].values()
        ]
        q = q.select(*select_terms)
    return q


def _first_sql_source(schema, entity):
    table = entity[TABLE]
    q = Query.from_(table)
    if entity[FILTERS]:
        for entity_type, col, val in entity[FILTERS]:
            _ignored, field = get_field(schema, entity_type, col)
            q = q.where(Field(field, table=table) == val)
    return q


def get_primary_field(schema, entity_type, table):
    id_field = primary_field(schema, entity_type)
    return Field(id_field, table=table)


def _compute_fields(entity, selects):
    select_terms = []
    for select in selects:
        entity_alias, column = select
        if column == "*":
            select_terms.append(entity.star)
        else:
            select_terms.append(Field(column, table=entity))
    return select_terms


def _update_entity_from_returns(entity, return_clause):
    selects = []
    # direct select from return clause
    selects += list(
        tz.thread_last(
            return_clause,
            (filter, lambda ret: ret[ENTITY_ID].split(".")[0] in entity[ENTITY_TYPES]),
            (
                map,
                lambda ret: {
                    **ret,
                    ALIAS: ret[ENTITY_ID].split(".")[0],
                    COLUMN: ret[ENTITY_ID].split(".")[1]
                    if "." in ret[ENTITY_ID]
                    else "*",
                },
            ),
        )
    )


def _merge_entities_backed_by_same_table(schema, entities):
    split_sources = [[entities[0]]]
    # try merge adjacent sources backed by the same table.
    for source in entities[1:]:
        if all(
            table_name(schema, source[ENTITY][TYPE])
            == table_name(schema, src[ENTITY][TYPE])
            and source[ENTITY][TYPE] != src[ENTITY][TYPE]
            for src in split_sources[-1]
        ):
            # backed by the same table, but represents different entities, this case, we don't have to do any special joins.
            split_sources[-1].append(source)
        else:
            split_sources.append([source])
    entities = [_merge_sources(split) for split in split_sources]
    return entities


def _merge_entities(schema, entities, selects):
    # some adjacent entities may be backed by the same table, we should merge them into one entity instead of doing joins.
    entities = [_update_entity(entity, selects) for entity in entities]
    entities = _merge_entities_backed_by_same_table(schema, entities)

    return entities


def _process_where(schema, table, entity_types, where):
    if where is None:
        raise ValueError("where clause cannot be None")

    if where[0] == AND:
        return _process_where(schema, table, entity_types, where[1]) & _process_where(
            schema, table, entity_types, where[2]
        )
    elif where[0] == OR:
        return _process_where(schema, table, entity_types, where[1]) | _process_where(
            schema, table, entity_types, table
        )
    else:
        entity_id, op, entity_id_or_value = where
        entity, col = entity_id.split(".")
        left_field = Field(get_field(schema, entity_types[entity], col)[1], table=table)
        if isinstance(entity_id_or_value, str) and "." in entity_id_or_value:
            right_field = Field(
                get_field(schema, *entity_id_or_value.split("."))[1], table=table
            )
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
