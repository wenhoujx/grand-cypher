from ipaddress import collapse_addresses
from turtle import end_fill
from grandcypher.constants import A, B, COLUMNS, FIELD, LINKS, MODELS, NAME, TABLE, TYPE
import toolz


def table_name(schema, entity_type): 
    return next((s[TABLE] for s in  schema[MODELS] if s[NAME] == entity_type), None)


def try_find_links(schema, entities):
    sources = set(toolz.thread_last(entities, 
                                (map, lambda ent: table_name(schema, ent[TYPE]))))
    
    if len(sources) == 1:
        # same source, no link 
        return None 
    links = set()
    entity_types = list(map(lambda ent: ent[TYPE], entities))
    for entity_a in entity_types:
        for entity_b in entity_types: 
            if entity_a == entity_b: continue
            link = next(filter(lambda link: _list_equals_disregard_ordering([entity_a, entity_b], [_split_then_first(link[A]), _split_then_first(link[B])]), 
                        schema[LINKS]), None)
            if not link: 
                raise ValueError(f"Could not find link between {entity_a} and {entity_b} in schema")
            else: 
                links.add(_find_source_field(schema, link[A]))
                links.add(_find_source_field(schema, link[B]))
    return list(links)
    
def _find_source_field(schema, entity_column): 
    entity_type, col = entity_column.split('.')
    return get_field(schema, entity_type, col)

def _split_then_first(s, split_on='.'): 
    return s.split(split_on)[0]

def _list_equals_disregard_ordering(lst1, lst2):
    return sorted(lst1) == sorted(lst2)


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
    for link in schema[LINKS]: 
        if (entity_type_a, entity_type_b) == (_split_then_first(link[A]), _split_then_first(link[B])): 
            return (get_field(schema, *link[A].split('.'))[1], get_field(schema, *link[B].split('.'))[1])
        elif (entity_type_b, entity_type_a) == (_split_then_first(link[A]), _split_then_first(link[B])): 
            return (get_field(schema, *link[B].split('.'))[1], get_field(schema, *link[A].split('.'))[1])
        else: 
            continue
    raise ValueError(f"fail to find link btw {entity_type_a} and {entity_type_b}")

def get_field_by_table_and_col(schema, table, col): 
    for mod in schema[MODELS]: 
        if mod[TABLE] != table: 
            continue
        for column in mod[COLUMNS]: 
            if column[NAME] == col : 
                return column.get(FIELD) or column[NAME]


def get_all_join_fields(schema, entity_type): 
    join_fields = []
    for link in schema[LINKS]: 
        if _split_then_first(link[A]) == entity_type: 
            join_fields.append( get_field(schema, entity_type, link[A].split('.')[1])[1])
        elif _split_then_first(link[B]) == entity_type: 
            join_fields.append( get_field(schema, entity_type, link[B].split('.')[1])[1])
    return join_fields
