"""
GrandCypher is a Cypher interpreter for the Grand graph library.

You can use this tool to search Python graph data-structures by
data/attribute or by structure, using the same language you'd use
to search in a much larger graph database.

"""
from asyncio import selector_events
from distutils.command.build_scripts import first_line_re
import toolz as tz
from typing import Dict, List, Callable, Tuple
from pypika import Query, Table, Field, functions as fn

import random
import string
from functools import lru_cache
import networkx as nx

import grandiso

from lark import Lark, Transformer, v_args, Token

from grandcypher.constants import (
    ALIASES,
    AND,
    COLUMN,
    COLUMNS,
    ENTITY,
    ENTITY_TYPES,
    FIELD,
    FILTERS,
    NAME,
    OP,
    OR,
    SELECTS,
    SOURCE,
    TABLE,
    TABLE,
    TYPE,
)
from grandcypher.schema import (
    find_join_fields,
    get_all_fields,
    primary_field,
    get_field,
    table_name,
)


_OPERATORS = {
    "=": lambda x, y: x == y,
    "==": lambda x, y: x == y,
    ">=": lambda x, y: x >= y,
    "<=": lambda x, y: x <= y,
    "<": lambda x, y: x < y,
    ">": lambda x, y: x > y,
    "!=": lambda x, y: x != y,
    "<>": lambda x, y: x != y,
    "in": lambda x, y: x in y,
    "contains": lambda x, y: y in x,
    "is": lambda x, y: x is y,
}


_GrandCypherGrammar = Lark(
    """
start               : query

query               : many_match_clause where_clause return_clause
                    | many_match_clause return_clause


many_match_clause   : (match_clause)+


match_clause        : "match"i node_match (edge_match node_match)*

where_clause        : "where"i compound_condition

compound_condition  : condition
                    | "(" compound_condition boolean_arithmetic compound_condition ")"
                    | compound_condition boolean_arithmetic compound_condition

condition           : entity_id op entity_id_or_value

?entity_id_or_value : entity_id
                    | value
                    | "NULL"i -> null
                    | "TRUE"i -> true
                    | "FALSE"i -> false

op                  : "==" -> op_eq
                    | "=" -> op_eq
                    | "<>" -> op_neq
                    | ">" -> op_gt
                    | "<" -> op_lt
                    | ">="-> op_gte
                    | "<="-> op_lte

return_clause       : "return"i (aggregate | entity_id) ("," (aggregate | entity_id))* limit_clause? skip_clause?
aggregate           : count_aggregate |count_star | sum_aggregate | avg_aggregate | min_aggregate | max_aggregate
count_star          : "count"i "(" "*" ")"
count_aggregate     : "count"i "(" entity_id ")"
sum_aggregate       : "sum"i "(" entity_id ")"
avg_aggregate       : "avg"i "(" entity_id ")"
min_aggregate       : "min"i "(" entity_id ")"
max_aggregate       : "max"i "(" entity_id ")"

limit_clause        : "limit"i NUMBER
skip_clause         : "skip"i NUMBER


?entity_id          : CNAME
                    | CNAME "." CNAME

node_match          : "(" (CNAME)? (json_dict)? ")"
                    | "(" (CNAME)? ":" TYPE (json_dict)? ")"

edge_match          : LEFT_ANGLE? "--" RIGHT_ANGLE?
                    | LEFT_ANGLE? "-[]-" RIGHT_ANGLE?
                    | LEFT_ANGLE? "-[" CNAME "]-" RIGHT_ANGLE? 
                    | LEFT_ANGLE? "-[" CNAME ":" TYPE "]-" RIGHT_ANGLE? 
                    | LEFT_ANGLE? "-[" ":" TYPE "]-" RIGHT_ANGLE? 
                    | LEFT_ANGLE? "-[" "*" MIN_HOP "]-" RIGHT_ANGLE?
                    | LEFT_ANGLE? "-[" "*" MIN_HOP  ".." MAX_HOP "]-" RIGHT_ANGLE?
                    | LEFT_ANGLE? "-[" CNAME "*" MIN_HOP "]-" RIGHT_ANGLE?
                    | LEFT_ANGLE? "-[" CNAME "*" MIN_HOP  ".." MAX_HOP "]-" RIGHT_ANGLE?
                    | LEFT_ANGLE? "-[" ":" TYPE "*" MIN_HOP "]-" RIGHT_ANGLE?
                    | LEFT_ANGLE? "-[" ":" TYPE "*" MIN_HOP  ".." MAX_HOP "]-" RIGHT_ANGLE?
                    | LEFT_ANGLE? "-[" CNAME ":" TYPE "*" MIN_HOP "]-" RIGHT_ANGLE?
                    | LEFT_ANGLE? "-[" CNAME ":" TYPE "*" MIN_HOP  ".." MAX_HOP "]-" RIGHT_ANGLE?



LEFT_ANGLE          : "<"
RIGHT_ANGLE         : ">"
MIN_HOP             : INT
MAX_HOP             : INT
TYPE                : CNAME

json_dict           : "{" json_rule ("," json_rule)* "}"
?json_rule          : CNAME ":" value

boolean_arithmetic  : "and"i -> where_and
                    | "OR"i -> where_or

key                 : CNAME
?value              : ESTRING
                    | NUMBER
                    | "NULL"i -> null
                    | "TRUE"i -> true
                    | "FALSE"i -> false


%import common.CNAME            -> CNAME
%import common.ESCAPED_STRING   -> ESTRING
%import common.SIGNED_NUMBER    -> NUMBER
%import common.INT              -> INT

%import common.WS
%ignore WS

""",
    start="start",
)

__version__ = "0.2.0"


_ALPHABET = string.ascii_lowercase + string.digits


def shortuuid(k=4) -> str:
    return "".join(random.choices(_ALPHABET, k=k))



def _get_entity_from_host(host: nx.DiGraph, entity_name, entity_attribute=None):
    if entity_name in host.nodes():
        # We are looking for a node mapping in the target graph:
        if entity_attribute:
            # Get the correct entity from the target host graph,
            # and then return the attribute:
            return host.nodes[entity_name].get(entity_attribute, None)
        else:
            # Otherwise, just return the node from the host graph
            return entity_name
    else:
        # looking for an edge:
        edge_data = host.get_edge_data(*entity_name)
        if not edge_data:
            return None  # print(f"Nothing found for {entity_name} {entity_attribute}")
        if entity_attribute:
            # looking for edge attribute:
            return edge_data.get(entity_attribute, None)
        else:
            return host.get_edge_data(*entity_name)


def _get_edge(host: nx.DiGraph, mapping, match_path, u, v):
    edge_path = match_path[(u, v)]
    return [
        host.get_edge_data(mapping[u], mapping[v])
        for u, v in zip(edge_path[:-1], edge_path[1:])
    ]


CONDITION = Callable[[dict, nx.DiGraph, list], bool]

def cond_(should_be, entity_id, operator, value) -> CONDITION:
    def inner(match: dict, host: nx.DiGraph, return_endges: list) -> bool:
        host_entity_id = entity_id.split(".")
        if host_entity_id[0] in match:
            host_entity_id[0] = match[host_entity_id[0]]
        elif host_entity_id[0] in return_endges:
            # looking for edge...
            edge_mapping = return_endges[host_entity_id[0]]
            host_entity_id[0] = (match[edge_mapping[0]], match[edge_mapping[1]])
        else:
            raise IndexError(f"Entity {host_entity_id} not in graph.")
        try:
            val = operator(_get_entity_from_host(host, *host_entity_id), value)
        except:
            val = False
        if val != should_be:
            return False
        return True

    return inner

class _GrandCypherTransformer(Transformer):
    def __init__(self, schema):
        self._target_graph = None
        self.schema = schema
        self._where_condition: CONDITION = None
        self._matches = None
        self._matche_paths = None
        self._return_requests = []
        self._return_edges = {}
        self._limit = None
        self._skip = 0
        self._max_hop = 100
        self.entities = []


    def count_star(self, count):
        return {
            OP: "count",
        }

    def count_aggregate(self, count):
        return {
            OP: "count",
            COLUMN: count[0],
        }

    def sum_aggregate(self, sum):
        return {
            OP: "sum",
            COLUMN: sum[0], 
        }

    def avg_aggregate(self, avg):
        return {
            OP: "avg",
            COLUMN: avg[0], 
        }

    def min_aggregate(self, min):
        return {
            OP: "min",
            COLUMN: min[0], 
        }

    def max_aggregate(self, max):
        return {
            OP: "max",
            COLUMN: max[0],
        }

    def aggregate(self, clause):
        return clause[0]

    def return_clause(self, clause):
        for item in clause:
            if isinstance(item, dict):
                self._return_requests.append(item)
            else:
                self._return_requests.append({COLUMN: item})

    def limit_clause(self, limit):
        limit = int(limit[-1])
        self._limit = limit

    def skip_clause(self, skip):
        skip = int(skip[-1])
        self._skip = skip

    def returns(self, ignore_limit=False):
        if self._limit and ignore_limit is False:
            return {
                r: self._lookup(r)[self._skip : self._skip + self._limit]
                for r in self._return_requests
            }
        return {r: self._lookup(r)[self._skip :] for r in self._return_requests}

    def _get_entity_source(self, entity):
        entity_type = entity[TYPE]
        entity_alias = entity[NAME]
        table = Table(table_name(self.schema, entity_type))
        filters = []
        for col, val in entity[FILTERS].items():
            filters.append((entity_type, col, val))
        selects = []
        for ret in self._return_requests:
            column = ret[COLUMN]
            op = ret.get(OP, None)
            if entity_alias == column.split(".")[0]:
                if "." in column:
                    ent, col = column.split(".")
                    _ignored, field = get_field(self.schema, entity_type, col)
                    selects.append({**ret, FIELD: field})
                else:
                    selects.append({**ret, FIELD: "*"})

        return {
            ENTITY: entity,
            ENTITY_TYPES: [entity_type],
            ALIASES: [entity_alias],
            TABLE: table,
            FILTERS: filters,
            SELECTS: selects,
        }

    def _merge_sources(self, sources):
        return {
            **sources[0],
            ENTITY_TYPES: list(tz.concat([src[ENTITY_TYPES] for src in sources])),
            ALIASES: list(set(tz.concat([src[ALIASES] for src in sources]))),
            FILTERS: list(tz.concat([src[FILTERS] for src in sources])),
            SELECTS: list(tz.concat([src[SELECTS] for src in sources])),
        }

    def _later_sql_source(self, source, joins=False):
        alias = source[ALIASES][0]

        if not source[FILTERS]:
            q = source[TABLE].as_(alias)
        else:
            # if filters present, we need to use a subquery with select, where, ...
            # TODO pretty weird that the inner and out share the same alias.
            table = source[TABLE].as_(alias)
            q = Query.from_(table)
            for entity_type, col, val in source[FILTERS]:
                _ignored, field = get_field(self.schema, entity_type, col)
                q = q.where(Field(field, table=table) == val)
            q = q.as_(alias)
            # if filters, we need to select the fields we want to return
            select_terms = self._compute_fields(q, source[SELECTS])
            if joins: 
                # include join field which is the primary field.
                select_terms += [
                    self.get_primary_field(entity_type, table)
                    for entity_type in source[ENTITY_TYPES]
                ]
            q = q.select(*select_terms)
        return q

    def _first_sql_source(self, source):
        alias = source[ALIASES][0]
        # first source is special.
        table = source[TABLE].as_(alias)
        #TODO this is pretty horrible. 
        source.update({
            TABLE: table,
        })
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
                select_terms.append(
                    self._sql_op(op, Field(select[FIELD], table=source))
                )
        return select_terms

    def sql(self):
        raw_sources = [self._get_entity_source(entity) for entity in self.entities]
        split_sources = [[raw_sources[0]]]
        # try merge adjacent sources backed by the same table.
        for source in raw_sources[1:]:
            if all(
                source[TABLE] == src[TABLE]
                and source[ENTITY][TYPE] != src[ENTITY][TYPE]
                for src in split_sources[-1]
            ):
                # backed by the same table, but represents different entities, this case, we don't have to do any special joins.
                split_sources[-1].append(source)
            else:
                split_sources.append([source])
        sources = [self._merge_sources(split) for split in split_sources]
        for i, source in enumerate(sources):
            source.update(
                {
                    SOURCE: self._first_sql_source(source)
                    if i == 0
                    else self._later_sql_source(
                        source, joins=True
                    )
                }
            )

        q = sources[0][SOURCE]
        select_terms = []
        for i ,  source in enumerate(sources):
            select_terms += self._compute_fields(source[TABLE] if i == 0 else source[SOURCE], source[SELECTS])

        q = q.select(*select_terms)

        for i in range(1, len(sources)):
            source = sources[i]
            prev_source = sources[i - 1]
            field_a, field_b = find_join_fields(
                self.schema, prev_source[ENTITY_TYPES], source[ENTITY_TYPES]
            )
            q = q.join(source[SOURCE]).on(
                # join on the outer table if prev is the first source, otherwise join on the prev source.
                Field(field_a, table=(prev_source[TABLE] if i == 1 else prev_source[SOURCE]))
                == Field(field_b, table=source[SOURCE])
            )

        print(q)
        return str(q)

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

    def entity_id(self, entity_id):
        if len(entity_id) == 2:
            return ".".join(entity_id)
        return entity_id.value

    def edge_match(self, edge_name):
        direction = cname = min_hop = max_hop = edge_type = None

        for token in edge_name:
            if token.type == "MIN_HOP":
                min_hop = int(token.value)
            elif token.type == "MAX_HOP":
                max_hop = int(token.value) + 1
            elif token.type == "LEFT_ANGLE":
                direction = "l"
            elif token.type == "RIGHT_ANGLE" and direction == "l":
                direction = "b"
            elif token.type == "RIGHT_ANGLE":
                direction = "r"
            elif token.type == "TYPE":
                edge_type = token.value
            else:
                cname = token

        direction = direction if direction is not None else "b"
        if (min_hop is not None or max_hop is not None) and (direction == "b"):
            raise TypeError("not support edge hopping for bidirectional edge")

        return (cname, edge_type, direction, min_hop, max_hop)

    def node_match(self, node_name):
        cname = node_type = json_data = None
        for token in node_name:
            if not isinstance(token, Token):
                json_data = token
            elif token.type == "CNAME":
                cname = token
            elif token.type == "TYPE":
                node_type = token.value
        cname = cname or Token("CNAME", shortuuid())
        json_data = json_data or {}
        self.entities.append({NAME: cname, TYPE: node_type, FILTERS: json_data})
        return (cname, node_type, json_data)

    def match_clause(self, match_clause: Tuple):
        ... 


    def where_clause(self, where_clause: tuple):
        self._where_condition = where_clause[0]

    def compound_condition(self, val):
        if len(val) == 1:
            # single condition
            return val[0]
        else:  # len == 3
            compound_a, operator, compound_b = val
            return (operator, compound_a, compound_b)

    def where_and(self, val):
        return AND
        
    def where_or(self, val):
        return OR

    def condition(self, condition):
        if len(condition) == 3:
            (entity_id, operator, value) = condition
            return (True, entity_id, operator, value)
        

    null = lambda self, _: None
    true = lambda self, _: True
    false = lambda self, _: False
    ESTRING = v_args(inline=True)(eval)
    NUMBER = v_args(inline=True)(eval)

    def op(self, operator):
        return operator

    def op_eq(self, _):
        return '='

    def op_neq(self, _):
        return '<>'

    def op_gt(self, _):
        return '>'

    def op_lt(self, _):
        return '<'

    def op_gte(self, _):
        return '>='

    def op_lte(self, _):
        return '<='

    def json_dict(self, tup):
        constraints = {}
        for key, value in tup:
            constraints[key] = value
        return constraints

    def json_rule(self, rule):
        return (rule[0].value, rule[1])


def cypher_to_duck(schema, cypher_query):
    t = _GrandCypherTransformer(schema)
    t.transform(_GrandCypherGrammar.parse(cypher_query))
    return t.sql()


class GrandCypher:
    """
    The user-facing interface for GrandCypher.

    Create a GrandCypher object in order to wrap your NetworkX-flavored graph
    with a Cypher-queryable interface.

    """

    def __init__(self, host_graph: nx.Graph) -> None:
        """
        Create a new GrandCypher object to query graphs with Cypher.

        Arguments:
            host_graph (nx.Graph): The host graph to use as a "graph database"

        Returns:
            None

        """

        self._transformer = _GrandCypherTransformer(host_graph)
        self._host_graph = host_graph

    def run(self, cypher: str) -> Dict[str, List]:
        """
        Run a cypher query on the host graph.

        Arguments:
            cypher (str): The cypher query to run

        Returns:
            Dict[str, List]: A dictionary mapping of results, where keys are
                the items the user requested in the RETURN statement, and the
                values are all possible matches of that structure in the graph.

        """
        self._transformer.transform(_GrandCypherGrammar.parse(cypher))
        return self._transformer.returns()
