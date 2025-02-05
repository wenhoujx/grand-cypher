import re
from duckcypher.constants import COLUMNS, FIELD, MODELS, NAME, TABLE, TABLES, TYPE
import toolz as tz
import duckdb


def show_models(schema, *model_types):
    model_types = set(model_types)
    return list(
        tz.thread_last(
            schema.get(MODELS, []), (filter, lambda m: m[NAME] in model_types)
        )
    )


def show_tables():
    return duckdb.sql("show tables;").fetchall()


def add_model(schema, model_type, table, mappings):
    if table not in set((t[NAME] for t in schema.get(TABLES, []))):
        raise ValueError(f"table {table} is not defined in schema")
    schema[MODELS] = list(
        tz.concatv(
            [
                {
                    **mappings,
                    NAME: model_type,
                    TABLE: table,
                }
            ],
            filter(lambda m: m[NAME] != model_type, schema.get(MODELS, [])),
        )
    )


def add_csv_table(schema, table_name, csv_path):
    try:
        duckdb.sql(
            f"""
        create view {table_name} as select * from read_csv_auto("{csv_path}");
        """
        )
        schema.setdefault(TABLES, []).append(
            {
                NAME: table_name,
                TYPE: "csv",
            }
        )
    except:
        raise ValueError(f"could not add table {table_name} from {csv_path}")


def add_table_from_variable(schema, table_name, var):
    if not isinstance(var, duckdb.DuckDBPyRelation):
        raise ValueError(f"var must be a duckdb.DuckDBPyRelation, not {type(var)}")
    duckdb.register(table_name, var.fetch_arrow_table())
    schema.setdefault(TABLES, []).append(
        {
            NAME: table_name,
            TYPE: "duckdb_variable",
        }
    )


def table_name(schema, entity_type):
    return next((s[TABLE] for s in schema[MODELS] if s[NAME] == entity_type), None)


def get_field(schema, entity_type, column):
    # returns tuple of (raw table name, raw field)
    for mod in schema[MODELS]:
        if mod[NAME] != entity_type:
            continue
        for col in mod[COLUMNS]:
            if col[NAME] != column:
                continue
            else:
                return (mod[TABLE], col.get(FIELD) or col[NAME])
    raise ValueError(f"could not find field {column} in entity {entity_type}")


def get_all_fields(schema, entity_type):
    # returns all fields of an entity.
    for mod in schema[MODELS]:
        if mod[NAME] != entity_type:
            continue
        return [col.get(FIELD) or col[NAME] for col in mod[COLUMNS]]


def find_join_fields(schema, left_entity_types, right_entity_types):
    if not left_entity_types:
        raise ValueError("left_entity_types is empty")
    if not right_entity_types:
        raise ValueError("right_entity_types is empty")
    if (
        len(left_entity_types) == 1
        and len(right_entity_types) == 1
        and left_entity_types[0] == right_entity_types[0]
    ):
        raise ValueError(
            f"should not be the same, left: {left_entity_types}, right: {right_entity_types}"
        )
    # entity_types are grouped together only b/c they are from the same source table.
    if table_name(schema, left_entity_types[-1]) == table_name(
        schema, right_entity_types[0]
    ):
        return (
            primary_field(schema, left_entity_types[-1]),
            primary_field(schema, left_entity_types[-1]),
        )
    else:
        return (
            primary_field(schema, left_entity_types[-1]),
            primary_field(schema, right_entity_types[0]),
        )


def get_field_by_table_and_col(schema, table, col):
    for mod in schema[MODELS]:
        if mod[TABLE] != table:
            continue
        for column in mod[COLUMNS]:
            if column[NAME] == col:
                return column.get(FIELD) or column[NAME]


def primary_field(schema, entity_type):
    for model in schema[MODELS]:
        if model[NAME] != entity_type:
            continue
        col = tz.first(filter(lambda col: col.get("primary", False), model[COLUMNS]))
        return col.get(FIELD, col[NAME]) or col[NAME]
