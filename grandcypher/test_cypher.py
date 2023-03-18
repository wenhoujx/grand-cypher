from grandcypher.constants import COLUMNS, MODELS, NAME, PATH, SOURCE, SOURCES, TYPE
from . import _GrandCypherTransformer, _GrandCypherGrammar, cypher_to_duck
import yaml 
import os 
import duckdb

def load_yaml(filename):
    with open(filename, 'r') as file:
        data = yaml.safe_load(file)
    return data 


def setup_duckdb(schema): 
    for source in schema[SOURCES]: 
        if source[TYPE] =='csv': 
            duckdb.sql(f"""
            create table {source[NAME]} as select * from read_csv_auto("{os.path.join(os.getcwd(), source[PATH].removeprefix('./'))}");""")

class TestSimple: 
    def setup_class(cls): 
        cls.schema=load_yaml(os.path.join(os.getcwd(), 'testing/test1/schema.yml'))
        cls.db = setup_duckdb(cls.schema)

    def test_simple_customer(self):
        cypher_q = """
        match (c: Customer)
        return c.first_name
        """
        sql = cypher_to_duck(TestSimple.schema, cypher_q)
        res = duckdb.sql(sql).fetchall()
        assert len(res) == 100

    def test_simple_company(self):
        cypher_q = """
        match (c: Company)
        return c.company
        """
        sql = cypher_to_duck(TestSimple.schema, cypher_q)
        res = duckdb.sql(sql).fetchall()
        assert len(res) == 100
    
    def test_company_and_customer_sample_source_table(self): 
        cypher_q = """
        match (cu: Customer) -- (co: Company)
        return co.company, cu.first_name
        """
        sql = cypher_to_duck(TestSimple.schema, cypher_q)
        res = duckdb.sql(sql).fetchall()
        assert len(res) == 100
        
    def test_simple_join(self): 
        cypher_q = """
        match (cu: Customer) -- (ci: CustomerInfo)
        return cu.first_name, ci.age, ci,state
        """
        sql = cypher_to_duck(TestSimple.schema, cypher_q)
        res = duckdb.sql(sql).fetchall()
        assert len(res) == 100

