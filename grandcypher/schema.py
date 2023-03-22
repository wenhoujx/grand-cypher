from grandcypher.constants import  COLUMNS, FIELD, MODELS, NAME, TABLE
import toolz as tz


def table_name(schema, entity_type): 
    return next((s[TABLE] for s in  schema[MODELS] if s[NAME] == entity_type), None)


def get_field(schema, entity, column): 
    # returns tuple of (raw table name, raw field)
    for mod in schema[MODELS]: 
        if mod[NAME] != entity: 
            continue 
        for col in mod[COLUMNS]: 
            if col[NAME] != column: 
                continue 
            else : 
                return (mod[TABLE], col.get(FIELD) or col[NAME])
    
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
    if len(left_entity_types) == 1 and len(right_entity_types) == 1 and left_entity_types[0] == right_entity_types[0]:
        raise ValueError(f"should not be the same, left: {left_entity_types}, right: {right_entity_types}")
    # entity_types are grouped together only b/c they are from the same source table.
    if table_name(schema,  left_entity_types[-1]) == table_name(schema, right_entity_types[0]):
        return (primary_field(schema, left_entity_types[-1]), primary_field(schema, left_entity_types[-1]))
    else: 
        return (primary_field(schema, left_entity_types[-1]), primary_field(schema, right_entity_types[0]))
    

def get_field_by_table_and_col(schema, table, col): 
    for mod in schema[MODELS]: 
        if mod[TABLE] != table: 
            continue
        for column in mod[COLUMNS]: 
            if column[NAME] == col : 
                return column.get(FIELD) or column[NAME]


def primary_field(schema, entity_type): 
    for model in schema[MODELS]: 
        if model[NAME]!= entity_type:
            continue
        col = tz.first(filter(lambda col: col.get('primary', False), model[COLUMNS]))
        return col.get(FIELD, col[NAME]) or col[NAME]
