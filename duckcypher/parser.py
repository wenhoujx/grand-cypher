import toolz as tz
from typing import Tuple


from lark import Lark, Transformer, v_args, Token

from duckcypher.constants import (
    ALIAS,
    AND,
    ENTITY_ID,
    FILTERS,
    LIMIT,
    MATCH,
    OP,
    OR,
    ORDER_BY,
    RETURN,
    TYPE,
    WHERE,
)
from duckcypher.to_sql import process_query


_DuckCypherGrammar = Lark(
    """
start               : query

query               : (match_clause (where_clause)? return_clause order_by_clause? limit_clause?)+

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

return_clause       : ("return"i | "with"i )(return_atom) ("," (return_atom))*  skip_clause?
return_atom         : aggregate | entity_id ("as"i CNAME)?
aggregate           : count_aggregate |count_star | sum_aggregate | avg_aggregate | min_aggregate | max_aggregate
count_star          : "count"i "(" "*" ")"
count_aggregate     : "count"i "(" entity_id ")"
sum_aggregate       : "sum"i "(" entity_id ")"
avg_aggregate       : "avg"i "(" entity_id ")"
min_aggregate       : "min"i "(" entity_id ")"
max_aggregate       : "max"i "(" entity_id ")"

limit_clause        : "limit"i NUMBER
order_by_clause     : "order"i "by"i (entity_id) 
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


class _DuckCypherTransformer(Transformer):
    def __init__(self, schema):
        self.schema = schema
        self._query = None

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

    def return_atom(self, clause):
        if len(clause) == 0:
            raise ValueError(f"Invalid return clause: {clause}")

        ret = {}
        ret.update(
            clause[0] if isinstance(clause[0], dict) else {ENTITY_ID: clause[0]}
        )
        if len(clause) == 2:
            # has alias
            ret.update({ALIAS: clause[1].value})
        return ret

    def return_clause(self, clause):
        return {TYPE: RETURN, RETURN: clause}

    def order_by_clause(self, order_by):
        return {TYPE: ORDER_BY, ORDER_BY: order_by}

    def limit_clause(self, limit):
        limit = int(limit[-1])
        return {TYPE: LIMIT, LIMIT: limit}

    def skip_clause(self, skip):
        skip = int(skip[-1])
        return skip

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
        return {ALIAS: cname, TYPE: node_type, FILTERS: json_data or {}}

    def match_clause(self, match_clause: Tuple):
        return {
            TYPE: MATCH,
            MATCH: list(
                tz.thread_last(
                    match_clause,
                    (filter, lambda x: x is not None),
                )
            ),
        }

    def where_clause(self, where_clause: tuple):
        return {TYPE: WHERE, WHERE: where_clause[0]}

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
        return condition

    null = lambda self, _: None
    true = lambda self, _: True
    false = lambda self, _: False
    ESTRING = v_args(inline=True)(eval)
    NUMBER = v_args(inline=True)(eval)

    def op(self, operator):
        return operator

    def op_eq(self, _):
        return "eq"

    def op_neq(self, _):
        return "neq"

    def op_gt(self, _):
        return "gt"

    def op_lt(self, _):
        return "lt"

    def op_gte(self, _):
        return "gte"

    def op_lte(self, _):
        return "lte"

    def json_dict(self, tup):
        constraints = {}
        for key, value in tup:
            constraints[key] = value
        return constraints

    def json_rule(self, rule):
        return (rule[0].value, rule[1])

    def query(self, clause):
        self._query = clause

    def run(self):
        if not self.query:
            raise ValueError("No query to run")
        res = process_query(self.schema, self._query)
        return res


def run_cypher(schema, cypher_query):
    t = _DuckCypherTransformer(schema)
    t.transform(_DuckCypherGrammar.parse(cypher_query))
    return t.run()
