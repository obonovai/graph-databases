from pathlib import Path
from typing import Any
import pandas as pd
import mgclient

DATA_PATH = Path(__file__).parent.parent.parent / "tpch-osx" / "dbgen"


class MemgraphTPCH:
    def __init__(self, config: dict[str, Any] | None = None) -> None:
        self.config = config or {
            "host": "127.0.0.1",
            "port": 7687,
        }
        self.connection = None
        self.cursor = None

    def connect(self) -> None:
        self.connection = mgclient.connect(**self.config)
        self.cursor = self.connection.cursor()

    def setup(self) -> None:
        self.setup_indices()

    def setup_indices(self) -> None:
        """Create indices on all key fields to speed up loading"""
        indices = [
            "CREATE INDEX ON :Region(regionkey)",
            "CREATE INDEX ON :Nation(nationkey)",
            "CREATE INDEX ON :Supplier(suppkey)",
            "CREATE INDEX ON :Customer(custkey)",
            "CREATE INDEX ON :Part(partkey)",
            "CREATE INDEX ON :Order(orderkey)",
            "CREATE INDEX ON :Customer(nationkey)",
            "CREATE INDEX ON :Supplier(nationkey)",
            "CREATE INDEX ON :Nation(regionkey)",
            "CREATE INDEX ON :Order(custkey)",
            "CREATE INDEX ON :LineItem(orderkey)",
            "CREATE INDEX ON :LineItem(partkey)",
            "CREATE INDEX ON :LineItem(suppkey)",
        ]

        # Set autocommit for index creation
        # self.connection.autocommit = True  ## Error

        for index_query in indices:
            try:
                self.cursor.execute(index_query)
            except Exception as e:
                print(f"Index creation warning: {e}")

        # Reset autocommit back to default (False) for data loading
        # self.connection.autocommit = False

    def clear(self):
        try:
            self.cursor.execute("MATCH (n) DETACH DELETE n")
        except Exception as e:
            print(f"Clear database warning: {e}")

    def batch_load(self, query: str, data: list[dict], table_name: str, batch_size: int = 1000) -> None:
        """Load data in batches using UNWIND with logging"""
        total_batches = (len(data) + batch_size - 1) // batch_size
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            self.cursor.execute(query, {"rows": batch})
            self.connection.commit()  # Commit each batch
            print(f"{table_name}: Loaded batch {i // batch_size + 1} of {total_batches}")

    def aload_region(self):
        """Load region.tbl"""
        path = DATA_PATH / "region.tbl"
        df = pd.read_csv(path, sep="|", header=None, usecols=range(3))
        df.columns = ["regionkey", "name", "comment"]
        data = df.to_dict(orient="records")

        query = """
            UNWIND $rows AS row
            MERGE (r:Region {regionkey: row.regionkey})
            SET r.name = row.name, r.comment = row.comment
        """
        self.batch_load(query, data, "Region")

    def aload_nation(self):
        """Load nation.tbl"""
        path = DATA_PATH / "nation.tbl"
        df = pd.read_csv(path, sep="|", header=None, usecols=range(4))
        df.columns = ["nationkey", "name", "regionkey", "comment"]
        data = df.to_dict(orient="records")

        query = """
            UNWIND $rows AS row
            MERGE (n:Nation {nationkey: row.nationkey})
            SET n.name = row.name, n.comment = row.comment
            WITH n, row
            MATCH (r:Region {regionkey: row.regionkey})
            MERGE (n)-[:BELONGS_TO]->(r)
        """
        self.batch_load(query, data, "Nation")

    def aload_supplier(self):
        """Load supplier.tbl"""
        path = DATA_PATH / "supplier.tbl"
        df = pd.read_csv(path, sep="|", header=None, usecols=range(7))
        df.columns = ["suppkey", "name", "address", "nationkey", "phone", "acctbal", "comment"]
        data = df.to_dict(orient="records")

        query = """
            UNWIND $rows AS row
            MERGE (s:Supplier {suppkey: row.suppkey})
            SET s.name = row.name, s.address = row.address, s.phone = row.phone,
                s.acctbal = row.acctbal, s.comment = row.comment
            WITH s, row
            MATCH (n:Nation {nationkey: row.nationkey})
            MERGE (s)-[:LOCATED_IN]->(n)
        """
        self.batch_load(query, data, "Supplier")

    def aload_customer(self):
        """Load customer.tbl with nation relationship"""
        path = DATA_PATH / "customer.tbl"
        df = pd.read_csv(path, sep="|", header=None, usecols=range(8))
        df.columns = ["custkey", "name", "address", "nationkey", "phone", "acctbal", "mktsegment", "comment"]
        data = df.to_dict(orient="records")

        query = """
            UNWIND $rows AS row
            MERGE (c:Customer {custkey: row.custkey})
            SET c.name = row.name, c.address = row.address, c.phone = row.phone,
                c.acctbal = row.acctbal, c.mktsegment = row.mktsegment, c.comment = row.comment
            WITH c, row
            MATCH (n:Nation {nationkey: row.nationkey})
            MERGE (c)-[:LOCATED_IN]->(n)
        """
        self.batch_load(query, data, "Customer")

    def aload_part(self):
        """Load part.tbl"""
        path = DATA_PATH / "part.tbl"
        df = pd.read_csv(path, sep="|", header=None, usecols=range(9))
        df.columns = ["partkey", "name", "mfgr", "brand", "type", "size", "container", "retailprice", "comment"]
        data = df.to_dict(orient="records")

        query = """
            UNWIND $rows AS row
            MERGE (p:Part {partkey: row.partkey})
            SET p.name = row.name, p.mfgr = row.mfgr, p.brand = row.brand, p.type = row.type,
                p.size = row.size, p.container = row.container, p.retailprice = row.retailprice,
                p.comment = row.comment
        """
        self.batch_load(query, data, "Part")

    def aload_partsupp(self):
        """Load partsupp.tbl"""
        path = DATA_PATH / "partsupp.tbl"
        df = pd.read_csv(path, sep="|", header=None, usecols=range(5))
        df.columns = ["partkey", "suppkey", "availqty", "supplycost", "comment"]
        data = df.to_dict(orient="records")

        query = """
            UNWIND $rows AS row
            MATCH (p:Part {partkey: row.partkey})
            MATCH (s:Supplier {suppkey: row.suppkey})
            MERGE (s)-[ps:SUPPLIES]->(p)
            SET ps.availqty = row.availqty, ps.supplycost = row.supplycost, ps.comment = row.comment
        """
        self.batch_load(query, data, "PartSupp")

    def aload_orders(self):
        """Load orders.tbl"""
        path = DATA_PATH / "orders.tbl"
        df = pd.read_csv(path, sep="|", header=None, usecols=range(9))
        df.columns = ["orderkey", "custkey", "orderstatus", "totalprice", "orderdate", "orderpriority", "clerk", "shippriority", "comment"]
        data = df.to_dict(orient="records")

        query = """
            UNWIND $rows AS row
            MERGE (o:Order {orderkey: row.orderkey})
            SET o.orderstatus = row.orderstatus, o.totalprice = row.totalprice,
                o.orderdate = row.orderdate, o.orderpriority = row.orderpriority,
                o.clerk = row.clerk, o.shippriority = row.shippriority, o.comment = row.comment
            WITH o, row
            MATCH (c:Customer {custkey: row.custkey})
            MERGE (c)-[:PLACED]->(o)
        """
        self.batch_load(query, data, "Orders")

    def aload_lineitem(self):
        """Load lineitem.tbl"""
        path = DATA_PATH / "lineitem.tbl"
        df = pd.read_csv(path, sep="|", header=None, usecols=range(16))
        df.columns = ["orderkey", "partkey", "suppkey", "linenumber", "quantity", "extendedprice",
                      "discount", "tax", "returnflag", "linestatus", "shipdate", "commitdate",
                      "receiptdate", "shipinstruct", "shipmode", "comment"]
        data = df.to_dict(orient="records")

        query = """
            UNWIND $rows AS row
            MATCH (o:Order {orderkey: row.orderkey})
            MATCH (p:Part {partkey: row.partkey})
            MATCH (s:Supplier {suppkey: row.suppkey})
            CREATE (li:LineItem {
                orderkey: row.orderkey, partkey: row.partkey, suppkey: row.suppkey,
                linenumber: row.linenumber, quantity: row.quantity, extendedprice: row.extendedprice,
                discount: row.discount, tax: row.tax, returnflag: row.returnflag,
                linestatus: row.linestatus, shipdate: row.shipdate, commitdate: row.commitdate,
                receiptdate: row.receiptdate, shipinstruct: row.shipinstruct,
                shipmode: row.shipmode, comment: row.comment
            })
            CREATE (o)-[:CONTAINS]->(li)
            CREATE (li)-[:OF_PART]->(p)
            CREATE (li)-[:SUPPLIED_BY]->(s)
        """
        self.batch_load(query, data, "LineItem")

    def aload(self):
        """Load all TPC-H tables in correct order (respecting foreign key dependencies)"""
        print("Starting TPC-H data load...")

        # Load in dependency order
        self.aload_region()
        self.aload_nation()
        self.aload_supplier()
        self.aload_customer()
        self.aload_part()
        self.aload_partsupp()
        self.aload_orders()
        self.aload_lineitem()

        print("All tables loaded successfully!")

    def close(self) -> None:
        self.connection.close()


def main():
    db = MemgraphTPCH()
    db.connect()
    db.clear()
    db.setup()
    db.aload()
    db.close()


if __name__ == "__main__":
    main()
