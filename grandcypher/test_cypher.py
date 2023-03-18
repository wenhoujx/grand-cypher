from grandcypher.constants import COLUMNS, MODELS, NAME, PATH, SOURCE, SOURCES, TYPE
from . import _GrandCypherTransformer, _GrandCypherGrammar
import yaml 
import os 
import duckdb

def load_yaml(filename):
    with open(filename, 'r') as file:
        data = yaml.safe_load(file)
    return data 


def setup_duckdb(csv_file, schema): 
    for source in schema[SOURCES]: 
        if source[TYPE] =='csv': 
            duckdb.sql(f"""
            create table {source[NAME]} as select * from read_csv_auto("{source[PATH]}");""")

class TestSimple: 
    def setup_method(self, method): 
        self.schema=load_yaml(os.path.join(os.getcwd(), 'testing/test1/schema.yml'))
        self.db = setup_duckdb(os.path.join(os.getcwd(), 'testing/test1/raw_customers.csv'), self.schema)

    def test_simple(self):
        t = _GrandCypherTransformer(self.schema)
        cypher_q = """
        match (c: Customer)
        return c.first_name
        """
        t.transform(_GrandCypherGrammar.parse(cypher_q))
        sql = t.sql()
        print(sql)
        res = duckdb.sql(sql).fetchall()
        assert len(res) == 100
