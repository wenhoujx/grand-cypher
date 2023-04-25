import random
import string
import kuzu


# this file is to test kuzu support with int16, int32, int64, https://github.com/kuzudb/kuzu/issues/1482
def _random_id():
    return "".join(random.choices(string.ascii_letters, k=8))


def test_int32():
    db = kuzu.Database(f"./testdbs/test-{_random_id()}")
    conn = kuzu.Connection(db)
    conn.execute("CREATE NODE TABLE User(name STRING, age INT32, PRIMARY KEY (name))")
    conn.execute("CREATE (u:User {name: 'Alice', age: 35})")
    conn.execute("match(u:User) return u").get_as_arrow(chunk_size=10)


def test_int16():
    db = kuzu.Database(f"./testdbs/test-{_random_id()}")
    conn = kuzu.Connection(db)
    conn.execute("CREATE NODE TABLE User(name STRING, age INT16, PRIMARY KEY (name))")
    conn.execute("CREATE (u:User {name: 'Alice', age: 35})")
    conn.execute("match(u:User) return u").get_as_arrow(chunk_size=10)


def test_int64():
    db = kuzu.Database(f"./testdbs/test-{_random_id()}")
    conn = kuzu.Connection(db)
    conn.execute("CREATE NODE TABLE User(name STRING, age INT64, PRIMARY KEY (name))")
    conn.execute("CREATE (u:User {name: 'Alice', age: 35})")
    conn.execute("match(u:User) return u").get_as_arrow(chunk_size=10)


def test_load_data_with_fewer_columns():
    db = kuzu.Database(f"./testdbs/test-{_random_id()}")
    conn = kuzu.Connection(db)
    # this fails b/c the csv file has fewer columns than the schema.
    conn.execute(
        "CREATE NODE TABLE Person(id STRING, first_name STRING, last_name STRING,  age INT64, state STRING, PRIMARY KEY (id))"
    )
    conn.execute("COPY Person FROM './data/persons.csv' (header=true)")


def test_load_data_by_column_name():
    db = kuzu.Database(f"./testdbs/test-{_random_id()}")
    conn = kuzu.Connection(db)
    # this fails b/c kuzu loading by index position instead of column name.
    conn.execute(
        "CREATE NODE TABLE Person(id STRING,  age INT64, name STRING,  state STRING, PRIMARY KEY (id))"
    )
    conn.execute("COPY Person FROM './data/persons.csv' (header=true)")
    res = conn.execute("match (p: Person) return p.name").get_as_df()
    print(res)


def test_load_two_tables():
    db = kuzu.Database(f"./testdbs/test-{_random_id()}")
    conn = kuzu.Connection(db)
    # this fails b/c kuzu loading by index position instead of column name.
    conn.execute(
        "CREATE NODE TABLE Person(id STRING,  name STRING, age INT64, PRIMARY KEY (id))"
    )
    conn.execute(
        "create node table State(name STRING, short_name STRING, PRIMARY KEY (name))"
    )
    conn.execute("create rel table LIVES_IN(from Person to State)")
    conn.execute("COPY  Person FROM './data/persons.csv' (header=true)")
    conn.execute("COPY State FROM './data/states.csv' (header=true)")
    conn.execute("COPY LIVES_IN from './data/lives_in.csv' (header=true)")
    res = conn.execute("match (p: Person) return p.name").get_as_df()
    print(res)
