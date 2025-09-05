import asyncio
from pathlib import Path
from typing import Any
from arangoasync import ArangoClient
from arangoasync.typings import CollectionType
from arangoasync.auth import Auth
import pandas as pd

DATA_PATH = Path(__file__).parent.parent.parent / "tpch-osx" / "dbgen"

VERTICES = ["region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem"]
EDGES = ["nation_region", "customer_nation", "supplier_nation", "customer_orders", "order_lineitems", "lineitem_supplier", "lineitem_part", "partsupp_part", "partsupp_supplier"]
INDICES = {
    "customer": [("c_custkey", True)],
    "orders": [("o_orderkey", True), ("o_custkey", False)],
    "lineitem": [("l_orderkey", False), ("l_partkey", False), ("l_suppkey", False)],
    "partsupp": [("ps_partkey", False), ("ps_suppkey", False)],
    "supplier": [("s_suppkey", True)],
    "part": [("p_partkey", True)],
    "nation": [("n_nationkey", True)],
    "region": [("r_regionkey", True)],
}


class ArangoTPCH:
    def __init__(self, config: dict[str, Any] | None = None) -> None:
        self.config = config or {
            "host": "http://localhost:8529",
            "username": "root",
            "password": "password",
            "database": "tpch",
            "graph": "tpchgraph",
        }
        self.client = None
        self.db = None


    async def aconnect(self) -> None:
        self.client = ArangoClient(hosts=self.config["host"])
        auth = Auth(username=self.config["username"], password=self.config["password"])
        sys_db = await self.client.db("_system", auth=auth)
        if not await sys_db.has_database(self.config["database"]):
            print(f"Database: {self.config['database']} does not exist, creating...")
            await sys_db.create_database(self.config["database"])
        self.db = await self.client.db(self.config["database"], auth=auth)


    async def asetup(self) -> None:
        for coll in VERTICES:
            if not await self.db.has_collection(coll):
                await self.db.create_collection(coll, col_type=CollectionType.DOCUMENT)

        for coll in EDGES:
            if not await self.db.has_collection(coll):
                await self.db.create_collection(coll, col_type=CollectionType.EDGE)

        print("Creating indices...")
        for name, indices in INDICES.items():
            if await self.db.has_collection(name):
                collection = self.db.collection(name)
                for field, unique in indices:
                    try:
                        await collection.add_index(
                            type="persistent",
                            fields=[field],
                            options={"unique": unique, "name": f"{name}_{field}_idx"}
                        )
                    except Exception as e:
                        print(f"Index creation warning for {name}_{field}_idx: {e}")


    async def aclear(self) -> None:
        if await self.db.has_graph(self.config["graph"]):
            await self.db.delete_graph(self.config["graph"], drop_collections=True)
        else:
            for coll in VERTICES + EDGES:
                if await self.db.has_collection(coll):
                    await self.db.delete_collection(coll)


    async def aload(self):
        print("Starting TPC-H data load for ArangoDB...")

        await self.aload_region()
        await self.aload_nation()
        await self.aload_supplier()
        await self.aload_customer()
        await self.aload_part()
        await self.aload_partsupp()
        await self.aload_orders()
        await self.aload_lineitem()

        print("All tables loaded successfully!")

    async def agraph(self) -> None:
        if not await self.db.has_graph(self.config["graph"]):
            graph = await self.db.create_graph(self.config["graph"])
            for coll in EDGES:
                _from, _to = coll.split("_")
                await graph.create_edge_definition(
                    edge_collection=coll,
                    from_vertex_collections=[_from],
                    to_vertex_collections=[_to]
                )

    async def aclose(self) -> None:
        if self.client:
            await self.client.close()

    async def aload_region(self) -> None:
        """Load region.tbl"""
        print("Loading region data...")
        path = DATA_PATH / "region.tbl"
        df = pd.read_csv(path, sep="|", header=None, usecols=range(3))
        df.columns = ["r_regionkey", "r_name", "r_comment"]

        # Convert to list of dictionaries and add _key field for ArangoDB
        data = []
        for _, row in df.iterrows():
            doc = {
                "_key": str(row["r_regionkey"]),
                "r_regionkey": row["r_regionkey"],
                "r_name": row["r_name"],
                "r_comment": row["r_comment"]
            }
            data.append(doc)

        # Insert data into ArangoDB collection
        region_collection = self.db.collection("region")
        await region_collection.insert_many(data)
        print(f"Loaded {len(data)} regions")

    async def aload_nation(self) -> None:
        """Load nation.tbl"""
        print("Loading nation data...")
        path = DATA_PATH / "nation.tbl"
        df = pd.read_csv(path, sep="|", header=None, usecols=range(4))
        df.columns = ["n_nationkey", "n_name", "n_regionkey", "n_comment"]

        # Convert to list of dictionaries and add _key field for ArangoDB
        nation_data = []
        edge_data = []

        for _, row in df.iterrows():
            # Create nation document
            nation_doc = {
                "_key": str(row["n_nationkey"]),
                "n_nationkey": row["n_nationkey"],
                "n_name": row["n_name"],
                "n_comment": row["n_comment"]
            }
            nation_data.append(nation_doc)

            # Create edge from nation to region
            edge_doc = {
                "_from": f"nation/{row['n_nationkey']}",
                "_to": f"region/{row['n_regionkey']}",
                "n_regionkey": row["n_regionkey"]
            }
            edge_data.append(edge_doc)

        # Insert nations
        nation_collection = self.db.collection("nation")
        await nation_collection.insert_many(nation_data)
        print(f"Loaded {len(nation_data)} nations")

        # Insert nation-region relationships
        nation_region_collection = self.db.collection("nation_region")
        await nation_region_collection.insert_many(edge_data)
        print(f"Created {len(edge_data)} nation-region relationships")

    async def aload_supplier(self) -> None:
        """Load supplier.tbl"""
        print("Loading supplier data...")
        path = DATA_PATH / "supplier.tbl"
        df = pd.read_csv(path, sep="|", header=None, usecols=range(7))
        df.columns = ["s_suppkey", "s_name", "s_address", "s_nationkey", "s_phone", "s_acctbal", "s_comment"]

        # Convert to list of dictionaries and add _key field for ArangoDB
        supplier_data = []
        edge_data = []

        for _, row in df.iterrows():
            # Create supplier document
            supplier_doc = {
                "_key": str(row["s_suppkey"]),
                "s_suppkey": row["s_suppkey"],
                "s_name": row["s_name"],
                "s_address": row["s_address"],
                "s_phone": row["s_phone"],
                "s_acctbal": row["s_acctbal"],
                "s_comment": row["s_comment"]
            }
            supplier_data.append(supplier_doc)

            # Create edge from supplier to nation
            edge_doc = {
                "_from": f"supplier/{row['s_suppkey']}",
                "_to": f"nation/{row['s_nationkey']}",
                "s_nationkey": row["s_nationkey"]
            }
            edge_data.append(edge_doc)

        # Insert suppliers
        supplier_collection = self.db.collection("supplier")
        await supplier_collection.insert_many(supplier_data)
        print(f"Loaded {len(supplier_data)} suppliers")

        # Insert supplier-nation relationships
        supplier_nation_collection = self.db.collection("supplier_nation")
        await supplier_nation_collection.insert_many(edge_data)
        print(f"Created {len(edge_data)} supplier-nation relationships")

    async def aload_customer(self) -> None:
        """Load customer.tbl"""
        print("Loading customer data...")
        path = DATA_PATH / "customer.tbl"
        df = pd.read_csv(path, sep="|", header=None, usecols=range(8))
        df.columns = ["c_custkey", "c_name", "c_address", "c_nationkey", "c_phone", "c_acctbal", "c_mktsegment", "c_comment"]

        # Convert to list of dictionaries and add _key field for ArangoDB
        customer_data = []
        edge_data = []

        for _, row in df.iterrows():
            # Create customer document
            customer_doc = {
                "_key": str(row["c_custkey"]),
                "c_custkey": row["c_custkey"],
                "c_name": row["c_name"],
                "c_address": row["c_address"],
                "c_phone": row["c_phone"],
                "c_acctbal": row["c_acctbal"],
                "c_mktsegment": row["c_mktsegment"],
                "c_comment": row["c_comment"]
            }
            customer_data.append(customer_doc)

            # Create edge from customer to nation
            edge_doc = {
                "_from": f"customer/{row['c_custkey']}",
                "_to": f"nation/{row['c_nationkey']}",
                "c_nationkey": row["c_nationkey"]
            }
            edge_data.append(edge_doc)

        # Insert customers
        customer_collection = self.db.collection("customer")
        await customer_collection.insert_many(customer_data)
        print(f"Loaded {len(customer_data)} customers")

        # Insert customer-nation relationships
        customer_nation_collection = self.db.collection("customer_nation")
        await customer_nation_collection.insert_many(edge_data)
        print(f"Created {len(edge_data)} customer-nation relationships")

    async def aload_part(self) -> None:
        """Load part.tbl"""
        print("Loading part data...")
        path = DATA_PATH / "part.tbl"
        df = pd.read_csv(path, sep="|", header=None, usecols=range(9))
        df.columns = ["p_partkey", "p_name", "p_mfgr", "p_brand", "p_type", "p_size", "p_container", "p_retailprice", "p_comment"]

        # Convert to list of dictionaries and add _key field for ArangoDB
        data = []
        for _, row in df.iterrows():
            doc = {
                "_key": str(row["p_partkey"]),
                "p_partkey": row["p_partkey"],
                "p_name": row["p_name"],
                "p_mfgr": row["p_mfgr"],
                "p_brand": row["p_brand"],
                "p_type": row["p_type"],
                "p_size": row["p_size"],
                "p_container": row["p_container"],
                "p_retailprice": row["p_retailprice"],
                "p_comment": row["p_comment"]
            }
            data.append(doc)

        # Insert data into ArangoDB collection
        part_collection = self.db.collection("part")
        await part_collection.insert_many(data)
        print(f"Loaded {len(data)} parts")

    async def aload_partsupp(self) -> None:
        """Load partsupp.tbl"""
        print("Loading partsupp data...")
        path = DATA_PATH / "partsupp.tbl"
        df = pd.read_csv(path, sep="|", header=None, usecols=range(5))
        df.columns = ["ps_partkey", "ps_suppkey", "ps_availqty", "ps_supplycost", "ps_comment"]

        # Convert to list of dictionaries and add _key field for ArangoDB
        partsupp_data = []
        part_edges = []
        supplier_edges = []

        for _, row in df.iterrows():
            # Create partsupp document with composite key
            partsupp_key = f"{row['ps_partkey']}_{row['ps_suppkey']}"
            partsupp_doc = {
                "_key": partsupp_key,
                "ps_partkey": row["ps_partkey"],
                "ps_suppkey": row["ps_suppkey"],
                "ps_availqty": row["ps_availqty"],
                "ps_supplycost": row["ps_supplycost"],
                "ps_comment": row["ps_comment"]
            }
            partsupp_data.append(partsupp_doc)

            # Create edge from partsupp to part
            part_edge = {
                "_from": f"partsupp/{partsupp_key}",
                "_to": f"part/{row['ps_partkey']}",
                "ps_partkey": row["ps_partkey"]
            }
            part_edges.append(part_edge)

            # Create edge from partsupp to supplier
            supplier_edge = {
                "_from": f"partsupp/{partsupp_key}",
                "_to": f"supplier/{row['ps_suppkey']}",
                "ps_suppkey": row["ps_suppkey"]
            }
            supplier_edges.append(supplier_edge)

        # Insert partsupp documents
        partsupp_collection = self.db.collection("partsupp")
        await partsupp_collection.insert_many(partsupp_data)
        print(f"Loaded {len(partsupp_data)} partsupp records")

        # Insert partsupp-part relationships
        partsupp_part_collection = self.db.collection("partsupp_part")
        await partsupp_part_collection.insert_many(part_edges)
        print(f"Created {len(part_edges)} partsupp-part relationships")

        # Insert partsupp-supplier relationships
        partsupp_supplier_collection = self.db.collection("partsupp_supplier")
        await partsupp_supplier_collection.insert_many(supplier_edges)
        print(f"Created {len(supplier_edges)} partsupp-supplier relationships")

    async def aload_orders(self) -> None:
        """Load orders.tbl"""
        print("Loading orders data...")
        path = DATA_PATH / "orders.tbl"
        df = pd.read_csv(path, sep="|", header=None, usecols=range(9))
        df.columns = ["o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice", "o_orderdate", "o_orderpriority", "o_clerk", "o_shippriority", "o_comment"]

        # Convert to list of dictionaries and add _key field for ArangoDB
        orders_data = []
        edge_data = []

        for _, row in df.iterrows():
            # Create order document
            order_doc = {
                "_key": str(row["o_orderkey"]),
                "o_orderkey": row["o_orderkey"],
                "o_orderstatus": row["o_orderstatus"],
                "o_totalprice": row["o_totalprice"],
                "o_orderdate": row["o_orderdate"],
                "o_orderpriority": row["o_orderpriority"],
                "o_clerk": row["o_clerk"],
                "o_shippriority": row["o_shippriority"],
                "o_comment": row["o_comment"]
            }
            orders_data.append(order_doc)

            # Create edge from customer to order
            edge_doc = {
                "_from": f"customer/{row['o_custkey']}",
                "_to": f"orders/{row['o_orderkey']}",
                "o_custkey": row["o_custkey"]
            }
            edge_data.append(edge_doc)

        # Insert orders
        orders_collection = self.db.collection("orders")
        await orders_collection.insert_many(orders_data)
        print(f"Loaded {len(orders_data)} orders")

        # Insert customer-orders relationships
        customer_orders_collection = self.db.collection("customer_orders")
        await customer_orders_collection.insert_many(edge_data)
        print(f"Created {len(edge_data)} customer-orders relationships")

    async def aload_lineitem(self) -> None:
        """Load lineitem.tbl"""
        print("Loading lineitem data...")
        path = DATA_PATH / "lineitem.tbl"
        df = pd.read_csv(path, sep="|", header=None, usecols=range(16))
        df.columns = ["l_orderkey", "l_partkey", "l_suppkey", "l_linenumber", "l_quantity", "l_extendedprice",
                      "l_discount", "l_tax", "l_returnflag", "l_linestatus", "l_shipdate", "l_commitdate",
                      "l_receiptdate", "l_shipinstruct", "l_shipmode", "l_comment"]

        # Process in batches to avoid connection timeout
        batch_size = 1000
        total_rows = len(df)
        total_batches = (total_rows + batch_size - 1) // batch_size

        print(f"Processing {total_rows} lineitem records in {total_batches} batches of {batch_size}")

        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min((batch_num + 1) * batch_size, total_rows)
            batch_df = df.iloc[start_idx:end_idx]

            # Convert to list of dictionaries and add _key field for ArangoDB
            lineitem_data = []
            order_edges = []
            part_edges = []
            supplier_edges = []

            for _, row in batch_df.iterrows():
                # Create lineitem document with composite key
                lineitem_key = f"{row['l_orderkey']}_{row['l_linenumber']}"
                lineitem_doc = {
                    "_key": lineitem_key,
                    "l_orderkey": row["l_orderkey"],
                    "l_partkey": row["l_partkey"],
                    "l_suppkey": row["l_suppkey"],
                    "l_linenumber": row["l_linenumber"],
                    "l_quantity": row["l_quantity"],
                    "l_extendedprice": row["l_extendedprice"],
                    "l_discount": row["l_discount"],
                    "l_tax": row["l_tax"],
                    "l_returnflag": row["l_returnflag"],
                    "l_linestatus": row["l_linestatus"],
                    "l_shipdate": row["l_shipdate"],
                    "l_commitdate": row["l_commitdate"],
                    "l_receiptdate": row["l_receiptdate"],
                    "l_shipinstruct": row["l_shipinstruct"],
                    "l_shipmode": row["l_shipmode"],
                    "l_comment": row["l_comment"]
                }
                lineitem_data.append(lineitem_doc)

                # Create edge from order to lineitem
                order_edge = {
                    "_from": f"orders/{row['l_orderkey']}",
                    "_to": f"lineitem/{lineitem_key}",
                    "l_orderkey": row["l_orderkey"]
                }
                order_edges.append(order_edge)

                # Create edge from lineitem to part
                part_edge = {
                    "_from": f"lineitem/{lineitem_key}",
                    "_to": f"part/{row['l_partkey']}",
                    "l_partkey": row["l_partkey"]
                }
                part_edges.append(part_edge)

                # Create edge from lineitem to supplier
                supplier_edge = {
                    "_from": f"lineitem/{lineitem_key}",
                    "_to": f"supplier/{row['l_suppkey']}",
                    "l_suppkey": row["l_suppkey"]
                }
                supplier_edges.append(supplier_edge)

            # Insert lineitem documents
            lineitem_collection = self.db.collection("lineitem")
            await lineitem_collection.insert_many(lineitem_data)

            # Insert order-lineitem relationships
            order_lineitems_collection = self.db.collection("order_lineitems")
            await order_lineitems_collection.insert_many(order_edges)

            # Insert lineitem-part relationships
            lineitem_part_collection = self.db.collection("lineitem_part")
            await lineitem_part_collection.insert_many(part_edges)

            # Insert lineitem-supplier relationships
            lineitem_supplier_collection = self.db.collection("lineitem_supplier")
            await lineitem_supplier_collection.insert_many(supplier_edges)

            print(f"Completed batch {batch_num + 1}/{total_batches} ({len(lineitem_data)} records)")

        print(f"Loaded {total_rows} lineitem records and all relationships successfully!")


async def main():
    db = ArangoTPCH()
    await db.aconnect()
    await db.aclear()
    await db.asetup()
    await db.aload()
    await db.agraph()
    await db.aclose()


if __name__ == "__main__":
    asyncio.run(main())
