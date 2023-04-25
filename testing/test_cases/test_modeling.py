from typing import Dict
import yaml
from modeling import load_from_schema


def _load_yaml(path: str) -> Dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)


class TestModeling:
    def test_simple(self):
        conn = load_from_schema(
            _load_yaml("testing/test_schemas/one_node_one_table.yml")
        )
        res = conn.execute("match (p:Person)  return p").get_as_arrow(chunk_size=200)
        assert res.num_rows == 10, "10 rows expected"
        res = conn.execute(
            "match (p: Person) where p.name = 'John Smith' return p.age as age"
        ).get_as_arrow(chunk_size=200)
        assert res.to_pydict()["age"] == [24]

    def test_one_node_two_tables(self):
        conn = load_from_schema(
            _load_yaml("testing/test_schemas/one_node_two_tables.yml")
        )
        res = conn.execute("match (p:Person)  return p").get_as_arrow(chunk_size=200)
        assert res.num_rows == 10, "10 rows expected"
        res = conn.execute(
            "match (p:Person {name: 'Mary Anderson'}) return p.gender as gender"
        ).get_as_df()
        assert res.iloc[0, 0] == "Male"
        res = conn.execute(
            "match (p:Person {name: 'John Smith'}) return p.gender as gender"
        ).get_as_df()
        assert res.iloc[0, 0] == "Female"

    def test_two_nodes_one_table(self):
        conn = load_from_schema(
            _load_yaml("testing/test_schemas/two_nodes_one_table.yml")
        )
        res = conn.execute(
            "match (p:Person {name: 'Mary Anderson' }) -[:LIVES_IN]-> (s:State)  return s.name"
        ).get_as_df()
        assert res.iloc[0, 0] == "Texas"

    def test_two_nodes_two_tables(self):
        conn = load_from_schema(
            _load_yaml("testing/test_schemas/two_nodes_two_tables.yml")
        )
        res = conn.execute(
            "match (p:Person {name: 'Mary Anderson' }) -[:LIVES_IN]-> (s:State)  return s.short_name"
        ).get_as_df()
        assert res.iloc[0, 0] == "TX"

    def test_self_referring(self):
        conn = load_from_schema(_load_yaml("testing/test_schemas/self_reference.yml"))
        res = conn.execute(
            "match (e:Employee {name: 'Jane Doe' }) -[:REPORTS_TO]-> (m:Employee)  return m.name"
        ).get_as_df()
        assert res.iloc[0, 0] == "John Smith"
