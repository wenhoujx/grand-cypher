from typing import Dict
import yaml
from modeling import load_from_schema


def _load_yaml(path: str) -> Dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)


class TestModeling:
    def test_simple(self):
        conn = load_from_schema(_load_yaml("testing/test1/one_node_one_table.yml"))
        res = conn.execute("match (c:Customer)  return c").get_as_arrow(chunk_size=200)
        assert res.num_rows == 100, "100 rows expected"
        res = conn.execute(
            "match (c:Customer) where c.first_name = 'Norma' return c.last_name"
        ).get_as_arrow(chunk_size=200)
        assert res.to_pydict() == {"c.last_name": ["C.", "W."]}
