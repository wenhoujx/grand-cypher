"""
GrandCypher is a Cypher interpreter for the Grand graph library.

You can use this tool to search Python graph data-structures by
data/attribute or by structure, using the same language you'd use
to search in a much larger graph database.

"""
import duckdb
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
    TABLE,
    TABLE_NAME,
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


class _GrandCypherTransformer(Transformer):
    def __init__(self, schema):
        self.schema = schema
        self._where_condition = None
        self._matches = []
        self._return_requests = []
        self._limit = None
        self._skip = 0
        self.entities = []
        # a list of selects for where clause
        self._where_selects = []

    def count_star(self, count):
        return {
            OP: "count",
        }

    def count_aggregate(self, count):
        return {
            OP: "count",
            ENTITY_ID: count[0],
        }

    def sum_aggregate(self, sum):
        return {
            OP: "sum",
            ENTITY_ID: sum[0],
        }

    def avg_aggregate(self, avg):
        return {
            OP: "avg",
            ENTITY_ID: avg[0],
        }

    def min_aggregate(self, min):
        return {
            OP: "min",
            ENTITY_ID: min[0],
        }

    def max_aggregate(self, max):
        return {
            OP: "max",
            ENTITY_ID: max[0],
        }

    def aggregate(self, clause):
        return clause[0]

    def return_clause(self, clause):
        for item in clause:
            if isinstance(item, dict):
                self._return_requests.append(item)
            else:
                self._return_requests.append({ENTITY_ID: item})

    def limit_clause(self, limit):
        limit = int(limit[-1])
        self._limit = limit

    def skip_clause(self, skip):
        skip = int(skip[-1])
        self._skip = skip

    def _get_entity_source(self, entity):
        entity_type = entity[TYPE]
        entity_alias = entity[NAME]
        filters = []
        for col, val in entity[FILTERS].items():
            filters.append((entity_type, col, val))
        selects = []
        for ret in self._return_requests:
            column = ret[ENTITY_ID]
            # op field is intentionally ignored
            if entity_alias == column.split(".")[0]:
                if "." in column:
                    ent, col = column.split(".")
                    _ignored, field = get_field(self.schema, entity_type, col)
                    selects.append({**ret, FIELD: field})
                else:
                    selects.append({**ret, FIELD: "*"})

        return {
            ENTITY: entity,
            ENTITY_TYPES: {entity_alias: entity_type},
            TABLE_NAME: table_name(self.schema, entity_type),
            FILTERS: filters,
            SELECTS: selects,
        }

    @staticmethod
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
                select_terms.append(
                    self._sql_op(op, Field(select[FIELD], table=source))
                )
        return select_terms

    def _merge_entities(self, sources):
        raw_sources = [self._get_entity_source(entity) for entity in self.entities]
        split_sources = [[raw_sources[0]]]
        # try merge adjacent sources backed by the same table.
        for source in raw_sources[1:]:
            if all(
                source[TABLE_NAME] == src[TABLE_NAME]
                and source[ENTITY][TYPE] != src[ENTITY][TYPE]
                for src in split_sources[-1]
            ):
                # backed by the same table, but represents different entities, this case, we don't have to do any special joins.
                split_sources[-1].append(source)
            else:
                split_sources.append([source])
        sources = [
            _GrandCypherTransformer._merge_sources(split) for split in split_sources
        ]
        # TODO two updates, pretty horrible, refactor.
        for source in sources:
            source.update(
                {
                    TABLE: Table(source[TABLE_NAME]).as_(
                        tz.first(source[ENTITY_TYPES].keys())
                    ),
                }
            )
        for i, source in enumerate(sources):
            source.update(
                {
                    # the source of the first table is the table itself.
                    SOURCE: source[TABLE]
                    if i == 0
                    else self._later_sql_source(source),
                }
            )
        return sources

    def _match_source(self):
        ...

    def _process_match_clause(self):
        match_sources = []
        for match in self.match:
            match_sources.append(self._process_single_match_clause(match))
        return match_sources

    def _process_single_match_clause(self, match):
        sources = self._merge_entities(match)
        q = self._first_sql_source(sources[0])

        # add possible joins.
        for i in range(1, len(sources)):
            source = sources[i]
            prev_source = sources[i - 1]
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
        # add where
        if self._where_condition:
            q = q.where(self._process_where(sources, self._where_condition))
        return {
            SQL: q,
            ALIASES: set(tz.concat([source[ENTITY_TYPES].keys() for source in sources]))
        }

    def sql(self):
        match_sources = self._process_match_clause()
        for ret in self._return_requests:
            
             
            
            

    def _process_where(self, sources, where_condition):
        if where_condition[0] == AND:
            return self._process_where(
                sources, where_condition[1]
            ) & self._process_where(sources, where_condition[2])
        elif where_condition[0] == OR:
            return self._process_where(
                sources, where_condition[1]
            ) | self._process_where(sources, where_condition[2])
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

    def entity_id(self, entity_id):
        if len(entity_id) == 2:
            return ".".join(entity_id)
        return entity_id.value

    def edge_match(self, edge_name):
        return None

    def node_match(self, node_name):
        cname = node_type = json_data = None
        for token in node_name:
            if not isinstance(token, Token):
                json_data = token
            elif token.type == "CNAME":
                cname = token.value
            elif token.type == "TYPE":
                node_type = token.value
        return {NAME: cname, TYPE: node_type, FILTERS: json_data or {}}

    def match_clause(self, match_clause: Tuple):
        self._matches.append(list(filter(lambda x: x is not None, match_clause)))

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
            # these fields needs to be selected in subqueries
            self._where_selects.append(entity_id)
            if isinstance(value, str) and "." in value:
                self._where_selects.append(value)
            return (entity_id, operator, value)

    null = lambda self, _: None
    true = lambda self, _: True
    false = lambda self, _: False
    ESTRING = v_args(inline=True)(eval)
    NUMBER = v_args(inline=True)(eval)

    def op(self, operator):
        return operator

    def op_eq(self, _):
        return lambda x, y: x == y

    def op_neq(self, _):
        return lambda x, y: x != y

    def op_gt(self, _):
        return lambda x, y: x > y

    def op_lt(self, _):
        return lambda x, y: x < y

    def op_gte(self, _):
        return lambda x, y: x >= y

    def op_lte(self, _):
        return lambda x, y: x <= y

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


def run_cypher(schema, cypher_query):
    sql = cypher_to_duck(schema, cypher_query)
    res = duckdb.sql(sql).fetchall()
    return res


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
