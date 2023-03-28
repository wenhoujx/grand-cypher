from grandcypher.constants import COLUMNS, MODELS, NAME, PATH, TABLE, TABLES, TYPE
from . import _GrandCypherTransformer, _GrandCypherGrammar, cypher_to_duck
import yaml 
import os 
import duckdb

def load_yaml(filename):
    with open(filename, 'r') as file:
        data = yaml.safe_load(file)
    return data 


def setup_duckdb(schema): 
    for source in schema[TABLES]: 
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
        return cu.first_name, ci.age, ci.state
        """
        sql = cypher_to_duck(TestSimple.schema, cypher_q)
        res = duckdb.sql(sql).fetchall()
        assert len(res) == 100

    def test_join_same_table(self):
        cypher_q = """MATCH (michael:Person {name: 'Michael'})
MATCH (other:Person)
WHERE other.state = michael.state AND other.name <> 'Michael'
RETURN other.name
"""
    def test_filter_one_table(self): 
        # find all rows with state 'tx' 
        cypher_q = """MATCH (ci:CustomerInfo{state: "TX"})
        RETURN ci
        """
        sql = cypher_to_duck(TestSimple.schema, cypher_q)
        res = duckdb.sql(sql).fetchall() 
        assert len(res) == 12

    def test_filter_join_table(self): 
        # find the person who lives in TX, works for google.
        cypher_q = """MATCH (ci:CustomerInfo{state: "TX"}) -- (cu: Customer) -- (co: Company {company:"google"})
        RETURN ci, cu, co
        """
        sql = cypher_to_duck(TestSimple.schema, cypher_q)
        res = duckdb.sql(sql).fetchall()
        print(res)
        assert len(res) == 2, 'two persons lives in tx and works for google'

    def test_self_join_filter(self): 
        cypher_q = """MATCH (michael:Customer {first_name: "michael"}) -- (company: Company) -- (person: Customer)
        RETURN person
        """
        sql = cypher_to_duck(TestSimple.schema, cypher_q)
        res = duckdb.sql(sql).fetchall()
        print(res)
        assert len(res) == 18, '18 ppl including michael works for the same company'

    def test_self_join_filter_no_michael(self): 
        cypher_q = """MATCH (michael:Customer {first_name: "michael"}) -- (company: Company) -- (person: Customer)

        RETURN person
        """
        sql = cypher_to_duck(TestSimple.schema, cypher_q)
        res = duckdb.sql(sql).fetchall()
        print(res)
        assert len(res) == 18, '18 ppl including michael works for the same company'

    def test_count(self): 
        cypher_q = """MATCH (company:Company {company: "google"}) -- (customer: Customer)
        RETURN count(customer)
        """
        sql = cypher_to_duck(TestSimple.schema, cypher_q)
        res = duckdb.sql(sql).fetchall()
        print(res)
        assert len(res) == 1 , 'single row'
        assert res[0][0] == 18, 'single value == 18'

    def test_max_age(self): 
        cypher_q = """MATCH (customer: Customer) -- (customer_info: CustomerInfo)
        RETURN max(customer_info.age)
        """
        sql = cypher_to_duck(TestSimple.schema, cypher_q)
        res = duckdb.sql(sql).fetchall()
        assert res[0][0] == 79, 'max age is 79'

    def test_find_by_two_filters(self): 
        cypher_q = """MATCH (customer: Customer) -- (customer_info: CustomerInfo)
        where customer_info.age = 32 and customer_info.state = "TX" 
        RETURN customer.first_name
        """
        sql = cypher_to_duck(TestSimple.schema, cypher_q)
        res = duckdb.sql(sql).fetchall()
        assert(res[0][0]) == 'michael', 'michael is the only person with age 32 and lives in TX'
        assert(len(res)) == 1 , 'only one person'

    def test_find_lt(self): 
        cypher_q = """MATCH (customer: Customer) -- (customer_info: CustomerInfo)
        where customer_info.age <=22  and customer_info.state = "TX" 
        RETURN customer.first_name
        """
        sql = cypher_to_duck(TestSimple.schema, cypher_q)
        res = duckdb.sql(sql).fetchall()
        assert(res[0][0]) == 'Nicholas', 'Nicholas is the only person with age 22 and lives in TX'
        assert(len(res)) == 1 , 'only one person'

    def test_find_younger_than_another_customer(self): 
        cypher_q = """MATCH (c1: Customer {first_name: "Lisa"}) -- (c1_info: CustomerInfo {state: "TX"})
        with c1 
        match (c2: Customer) -- (c2_info: CustomerInfo {state: "FL"})
        where c1_info.age > c2_info.age and c2.first_name <> "Lisa"
        RETURN c2.first_name, c1.first_name
        """
        sql = cypher_to_duck(TestSimple.schema, cypher_q)
        res = duckdb.sql(sql).fetchall()
        print(res)
        # assert(res[0][0]) == 'Nicholas', 'Nicholas is the only person with age 22 and lives in TX'
        # assert(len(res)) == 1 , 'only one person'
