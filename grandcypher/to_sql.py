from html import entities
from typing import List
import toolz as tz
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
    OR,
    SELECTS,
    SOURCE,
    SQL,
    TABLE,
    TABLE_NAME,
    TYPE,
)
from pypika import Field, Table


def _join_entities(schema, entities):
    q = _first_sql_source(entities[0])

    select_terms = []
    for i, source in enumerate(entities):
        select_terms += _compute_fields(source[SOURCE], source[SELECTS])

    q = q.select(*select_terms)
    # add possible joins.
    for i in range(1, len(entities)):
        source = entities[i]
        prev_source = entities[i - 1]
        field_a, field_b = find_join_fields(
            self.schema,
            list(prev_source[ENTITY_TYPES].values()),
            list(source[ENTITY_TYPES].values()),
        )
        q = q.join(source[SOURCE]).on(
            # join on the outer table if prev is the first source, otherwise join on the prev source.
            Field(
                field_a,
                table=(prev_source[SOURCE]),
            )
            == Field(field_b, table=source[SOURCE])
        )


def _add_where(q, entities, where):
    if where:
        q = q.where(_process_where(entities, where))


def _execute_query(duckdb, query):
    ...


def _find_relevant_where(where, aliases):
    # find the where clause that join different match clauses together
    ...


def _build_final_query(schema, match_results, where, return_clause):
    ...


def process_query(schema, query, duckdb):
    matches = []
    for match in query["matches"]:
        matches.append(
            _process_match_query(schema, match, query.get("where"), query["return"])
        )
    match_results = [_execute_query(duckdb, match[SQL]) for match in matches]
    if len(match_results) == 0:
        return match_results[0]["result"]

    # need to do another query that joins the results of the match queries.
    where = _find_relevant_where(
        query.get("where"), [match[ALIASES] for match in matches]
    )
    if not where:
        raise ValueError("No where clause found for join query.")

    return_clause = query["return"]
    q = _build_final_query(schema, match_results, where, return_clause)
    res = _execute_query(duckdb, q)
    return res


def _entity_id_belongs(entity_id, entity_alias):
    return entity_id.split(".")[0] == entity_alias


def _split_entity_id(entity_id):
    if "." not in entity_id:
        return entity_id, "*"
    else:
        return entity_id.split(".")


def _find_selects(aliases, return_clause, where):
    # find selects , return [(entity, column)...]
    selects = []

    for ret in return_clause:
        entity_alias = ret[ENTITY_ID].split(".")[0]
        if entity_alias in aliases:
            selects.append(_split_entity_id(ret[ENTITY_ID]))

    selects += _find_relevant_where(aliases, where)
    return selects


def _find_relevant_where(aliases: List[str], where):
    # recursively find the entity_id needs to be selected in where clause.
    if where[0] == AND or where[0] == OR:
        return _find_relevant_where(aliases, where[1]) + _find_relevant_where(
            aliases, where[2]
        )
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

    entities = _merge_entities(match, _find_selects(all_aliases, return_clause, where))
    # join the entities together.
    q = _join_entities(schema, entities)
    # add where
    q = _add_where(q, entities, where)

    return {
        SQL: q,
        ALIASES: set(tz.concat([source[ENTITY_TYPES].keys() for source in entities])),
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


def _later_sql_source(self, source):
    alias = tz.first(source[ENTITY_TYPES].keys())
    if not source[FILTERS]:
        # return as plain table.
        q = source[TABLE]
    else:
        # if filters present, we need to use a subquery with select, where, ...
        # TODO pretty weird that the inner and out share the same alias.
        table = source[TABLE]
        q = Query.from_(table)
        for entity_type, col, val in source[FILTERS]:
            _ignored, field = get_field(self.schema, entity_type, col)
            q = q.where(Field(field, table=table) == val)
        # wrap the subquery in the same alias as the inner table.
        q = q.as_(alias)
        # if filters, we need to select the fields we want to return
        select_terms = self._compute_fields(q, source[SELECTS])
        # include join field which is the primary field.
        select_terms += [
            self.get_primary_field(entity_type, table)
            for entity_type in source[ENTITY_TYPES].values()
        ]
        # add selects from where clause.
        for where_select in self._where_selects:
            entity_alias, col = where_select.split(".")
            if entity_alias in source[ENTITY_TYPES]:
                entity_type = source[ENTITY_TYPES][entity_alias]
                _ignored, field = get_field(self.schema, entity_type, col)
                select_terms.append(Field(field, table=table))

        q = q.select(*select_terms)
    return q


def _first_sql_source(self, source):
    table = source[TABLE]
    q = Query.from_(table)
    if source[FILTERS]:
        for entity_type, col, val in source[FILTERS]:
            _ignored, field = get_field(self.schema, entity_type, col)
            q = q.where(Field(field, table=table) == val)
    return q


def get_primary_field(self, entity_type, table):
    id_field = primary_field(self.schema, entity_type)
    return Field(id_field, table=table)


def _compute_fields(self, source, selects):
    select_terms = []
    for select in selects:
        op = select.get(OP, None)
        if op == "count" and select[FIELD] == "*":
            select_terms.append(fn.Count("*"))
        elif select[FIELD] == "*":
            select_terms.append(self._sql_op(op, source.star))
        else:
            select_terms.append(self._sql_op(op, Field(select[FIELD], table=source)))
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


def _first_alias(entity):
    ...


def _update_entity_from_where(entity, where):
    # add filter from where clause.
    # add selects from where clause?
    ...


def _merge_entities_backed_by_same_table(entities):
    split_sources = [[entities[0]]]
    # try merge adjacent sources backed by the same table.
    for source in entities[1:]:
        if all(
            source[TABLE_NAME] == src[TABLE_NAME]
            and source[ENTITY][TYPE] != src[ENTITY][TYPE]
            for src in split_sources[-1]
        ):
            # backed by the same table, but represents different entities, this case, we don't have to do any special joins.
            split_sources[-1].append(source)
        else:
            split_sources.append([source])
    entities = [_merge_sources(split) for split in split_sources]
    return entities


def _merge_entities(entities, selects):
    # some adjacent entities may be backed by the same table, we should merge them into one entity instead of doing joins.
    entities = [_update_entity(entity, selects) for entity in entities]
    entities = _merge_entities_backed_by_same_table(entities)

    # TODO two updates, pretty horrible, refactor.
    for source in entities:
        source.update(
            {
                TABLE: Table(source[TABLE_NAME]).as_(
                    tz.first(source[ENTITY_TYPES].keys())
                ),
            }
        )
    for i, source in enumerate(entities):
        source.update(
            {
                # the source of the first table is the table itself.
                SOURCE: source[TABLE]
                if i == 0
                else _later_sql_source(source),
            }
        )
    return entities


def _process_where(sources, where_condition):
    if where_condition[0] == AND:
        return _process_where(sources, where_condition[1]) & _process_where(
            sources, where_condition[2]
        )
    elif where_condition[0] == OR:
        return _process_where(sources, where_condition[1]) | _process_where(
            sources, where_condition[2]
        )
    else:
        entity_id, op, entity_id_or_value = where_condition
        entity, col = entity_id.split(".")
        left_field = self._get_field(sources, entity, col)
        if isinstance(entity_id_or_value, str) and "." in entity_id_or_value:
            right_field = self._get_field(sources, *entity_id_or_value.split("."))
        else:
            right_field = entity_id_or_value
        return op(left_field, right_field)


def _get_field(self, sources, entity_alias, col):
    for i, source in enumerate(sources):
        if entity_alias in source[ENTITY_TYPES]:
            _ignored, field = get_field(
                self.schema, source[ENTITY_TYPES][entity_alias], col
            )
            return Field(field, table=source[SOURCE])

    raise Exception(f"Entity {entity_alias} not found in sources")


def _sql_op(self, op, field):
    if op:
        return {
            "sum": fn.Sum,
            "count": fn.Count,
            "avg": fn.Avg,
            "min": fn.Min,
            "max": fn.Max,
        }[op](field)
    else:
        return field
