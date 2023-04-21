from duckcypher.constants import (
    ALIAS,
    ENTITY_ID,
    FIELD,
    FROM,
    MAPPINGS,
    NAME,
    NODE,
    NODE_TYPE,
    NODES,
    PATH,
    PROPERTIES,
    TABLES,
    TO,
    TYPE,
)
from duckcypher.parser import run_cypher
from duckcypher.to_sql import _process_single_match
import yaml
import os
import duckdb


def load_yaml(filename):
    with open(filename, "r") as file:
        data = yaml.safe_load(file)
    return data


def setup_duckdb(schema):
    for source in schema[TABLES]:
        if source[TYPE] == "csv":
            duckdb.sql(
                f"""
            create table {source[NAME]} as select * from read_csv_auto("{os.path.join(os.getcwd(), source[PATH].removeprefix('./'))}");"""
            )


class TestSimple:
    def setup_class(cls):
        cls.schema = load_yaml(os.path.join(os.getcwd(), "testing/test1/schema.yml"))
        cls.db = setup_duckdb(cls.schema)

    def test_simple_customer(self):
        cypher_q = """
        match (c: Customer)
        with c.first_name
        """
        res = run_cypher(TestSimple.schema, cypher_q)
        assert len(res) == 100

    def test_simple_company(self):
        cypher_q = """
        match (c: Company)
        return c.company
        """
        res = run_cypher(TestSimple.schema, cypher_q)
        assert len(res) == 100

    def test_company_and_customer_sample_source_table(self):
        cypher_q = """
        match (cu: Customer) -- (co: Company)
        return co.company, cu.first_name
        """
        res = run_cypher(TestSimple.schema, cypher_q)
        assert len(res) == 100

    def test_simple_join(self):
        cypher_q = """
        match (cu: Customer) -- (ci: CustomerInfo)
        return cu.first_name, ci.age, ci.state
        """
        res = run_cypher(TestSimple.schema, cypher_q)
        assert len(res) == 100

    def test_filter_one_table(self):
        # find all rows with state 'tx'
        cypher_q = """MATCH (ci:CustomerInfo{state: "TX"})
        RETURN ci
        """
        res = run_cypher(TestSimple.schema, cypher_q)
        assert len(res) == 12

    def test_filter_join_table(self):
        # find the person who lives in TX, works for google.
        cypher_q = """MATCH (ci:CustomerInfo{state: "TX"}) -- (cu: Customer) -- (co: Company {company:"google"})
        RETURN ci, cu, co
        """
        res = run_cypher(TestSimple.schema, cypher_q)

        assert len(res) == 2, "two persons lives in tx and works for google"

    def test_self_join_filter(self):
        cypher_q = """MATCH (michael:Customer {first_name: "michael"}) -- (company: Company) -- (person: Customer)
        RETURN person
        """
        res = run_cypher(TestSimple.schema, cypher_q)

        assert len(res) == 18, "18 ppl including michael works for the same company"

    def test_self_join_filter_no_michael(self):
        cypher_q = """MATCH (michael:Customer {first_name: "michael"}) -- (company: Company) -- (person: Customer)
        RETURN person
        """
        res = run_cypher(TestSimple.schema, cypher_q)
        assert len(res) == 18, "18 ppl including michael works for the same company"

    def test_count(self):
        cypher_q = """MATCH (company:Company {company: "google"}) -- (customer: Customer)
        RETURN count(customer)
        """
        res = run_cypher(TestSimple.schema, cypher_q)
        assert len(res) == 1, "single row"
        assert res.fetchall()[0][0] == 18, "single value == 18"

    def test_max_age(self):
        cypher_q = """MATCH (customer: Customer) -- (customer_info: CustomerInfo)
        RETURN max(customer_info.age)
        """
        res = run_cypher(TestSimple.schema, cypher_q).fetchall()
        assert res[0][0] == 79, "max age is 79"

    def test_find_by_two_filters(self):
        cypher_q = """MATCH (customer: Customer) -- (customer_info: CustomerInfo)
        where customer_info.age = 32 and customer_info.state = "TX" 
        RETURN customer.first_name
        """
        res = run_cypher(TestSimple.schema, cypher_q).fetchall()
        assert (
            res[0][0]
        ) == "michael", "michael is the only person with age 32 and lives in TX"
        assert (len(res)) == 1, "only one person"

    def test_find_lt(self):
        cypher_q = """MATCH (customer: Customer) -- (customer_info: CustomerInfo)
        where customer_info.age <=22  and customer_info.state = "TX" 
        RETURN customer.first_name
        """
        res = run_cypher(TestSimple.schema, cypher_q).fetchall()
        assert (
            res[0][0]
        ) == "Nicholas", "Nicholas is the only person with age 22 and lives in TX"
        assert (len(res)) == 1, "only one person"

    def test_find_younger_than_another_customer(self):
        cypher_q = """MATCH (customer: Customer {first_name: "Lisa"}) -- (lisa: CustomerInfo {state: "TX"})
        with lisa
        match (cu: Customer) -- (c2_info: CustomerInfo {state: "FL"})
        where c2_info.age > lisa.age and cu.first_name <> "Lisa"
        RETURN cu.first_name, c2_info
        """
        res = run_cypher(TestSimple.schema, cypher_q).fetchall()
        assert (len(res)) == 11, "11 people lives in FL are younger than Lisa"

    def test_as(self):
        cypher_q = """MATCH (customer: Customer {first_name: "Lisa"}) -- (cu: CustomerInfo {state: "TX"})
        return customer.first_name as name, cu.age as age
        """
        res = run_cypher(TestSimple.schema, cypher_q)
        assert (len(res)) == 1, "one row"

    def test_as_complex(self):
        cypher_q = """MATCH (customer: Customer {first_name: "Lisa"}) -- (lisa: CustomerInfo {state: "TX"})
        with lisa.age as lisa_age
        match (cu: Customer) -- (c2_info: CustomerInfo {state: "FL"})
        where c2_info.age > lisa_age and cu.first_name <> "Lisa"
        RETURN cu.first_name, c2_info
        """
        res = run_cypher(TestSimple.schema, cypher_q).fetchall()
        assert (len(res)) == 11, "11 people lives in FL are younger than Lisa"

    def test_order_by(self):
        cypher_q = """MATCH (customer: Customer) -- (customer_info: CustomerInfo)
        return customer, customer_info.age
        order by customer_info.age desc
        limit 1 
        """
        res = run_cypher(TestSimple.schema, cypher_q).fetchall()
        assert (len(res)) == 1, "one row"
        assert "Scott" in res[0], "Scott is the oldest"
        assert res[0][-1] == 79, "Scott is 79"


class TestParseJoin:
    def setup_class(cls):
        cls.base_customer_schema = {
            NODES: [
                {
                    NAME: "Customer",
                    PROPERTIES: [
                        {
                            NAME: "id",
                            TYPE: "string",
                        },
                        {
                            NAME: "name",
                            TYPE: "string",
                        },
                        {
                            NAME: "age",
                            TYPE: "int",
                        },
                    ],
                }
            ]
        }

    def test_simple(self):
        schema = {
            **self.base_customer_schema,
            TABLES: [
                {
                    NAME: "customer",
                    TYPE: "csv",
                    PATH: "tests/data/customer.csv",
                },
            ],
            MAPPINGS: [
                {
                    TYPE: NODE,
                    FROM : 'customer.id', 
                    TO: 'Customer.id',
                }, 
                {
                    TYPE: NODE,
                    FROM : 'customer.id', 
                    TO: 'Customer.id',
                }, 
                {
                    TYPE: NODE,
                    FROM : 'customer.id', 
                    TO: 'Customer.id',
                }, 
                {
                    TYPE: NODE,
                    FROM : 'customer.id', 
                    TO: 'Customer.id',
                }, 
            ],
        }
        _process_single_match(
            [
                {ALIAS: "customer", NODE_TYPE: "Customer"},
            ],
            [
                {
                    ENTITY_ID: "customer.age",
                }
            ],
        )
