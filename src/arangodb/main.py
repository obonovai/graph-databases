import asyncio
from typing import Any
from arangoasync import ArangoClient
from arangoasync.auth import Auth


class ArangoDB:
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


    async def connect(self) -> None:
        self.client = ArangoClient(hosts=self.config["host"])
        auth = Auth(username=self.config["username"], password=self.config["password"])
        sys_db = await self.client.db("_system", auth=auth)
        if not await sys_db.has_database(self.config["database"]):
            print(f"Database: {self.config['database']} does not exist, creating...")
            await sys_db.create_database(self.config["database"])
        self.db = await self.client.db(self.config["database"], auth=auth)


    async def arun(self, query: str):
        cursor = await self.db.aql.execute(query)
        results = []
        async for doc in cursor:
            results.append(doc)
        return results


    async def aclose(self) -> None:
        if self.client:
            await self.client.close()


async def main():
    db = ArangoDB()
    await db.connect()

    # A. Selection, Projection, Source (of Data)
    # A1. Non-Indexed Columns
    query_a1 = """
        FOR c IN customer
            FILTER c.c_name == "Customer#000000001"
            RETURN {custKey: c.c_custkey, name: c.c_name}
    """
    print("A1. Selection/Projection (Non-Indexed):", await db.arun(query_a1))

    # A2. Non-Indexed Columns with complex condition
    query_a2 = """
        FOR c IN customer
            FILTER CONTAINS(c.c_comment, "foxes")
                AND LENGTH(c.c_comment) > 20
            RETURN {custKey: c.c_custkey, comment: c.c_comment}
    """
    print("A2. Selection/Projection (Non-Indexed Complex):", await db.arun(query_a2))

    # A3. Indexed Columns
    query_a3 = """
        FOR c IN customer
            FILTER c.c_custkey == 1
            RETURN {custKey: c.c_custkey, name: c.c_name}
    """
    print("A3. Selection/Projection (Indexed):", await db.arun(query_a3))

    # B. Aggregation
    # B1. COUNT
    query_b1 = """
        FOR c IN customer
            COLLECT segment = c.c_mktsegment
            AGGREGATE cnt = COUNT(c)
            RETURN {segment, cnt}
    """
    print("B1. Aggregation (COUNT):", await db.arun(query_b1))

    # B2. MAX/MIN/AVG
    query_b2 = """
        FOR c IN customer
            COLLECT AGGREGATE maxBal = MAX(c.c_acctbal)
            RETURN {maxBalance: maxBal}
    """
    print("B2. Aggregation (MAX/MIN/AVG):", await db.arun(query_b2))

    # C. Joins
    # C1. Non-Indexed Columns Join
    # query_c1 = """
    #     FOR c IN customer
    #         FOR o IN orders
    #             FILTER c.c_name == o.o_comment   // non-indexed
    #             RETURN {customer: c.c_name, order: o.o_orderkey}
    # """
    # print("C1. Joins (Non-Indexed):", await db.arun(query_c1))

    # C2. Indexed Columns Join
    query_c2 = """
        FOR c IN customer
            FOR o IN orders
                FILTER c.c_custkey == o.o_custkey
                RETURN {customer: c.c_name, order: o.o_orderkey}
    """
    print("C2. Joins (Indexed):", await db.arun(query_c2))

    # C3. Complex Join 1
    query_c3 = """
        FOR c IN customer
            FOR o IN orders
                FILTER c.c_custkey == o.o_custkey
                AND c.c_mktsegment == "AUTOMOBILE"
                RETURN {customer: c.c_name, segment: c.c_mktsegment, order: o.o_orderkey}
    """
    print("C3. Complex Join 1:", await db.arun(query_c3))

    # C4. Complex Join 2
    query_c4 = """
        FOR c IN customer
            FOR o IN orders
                FILTER c.c_custkey == o.o_custkey
                FOR l IN lineitem
                    FILTER o.o_orderkey == l.l_orderkey
                    RETURN {
                        customer: c.c_name,
                        order: o.o_orderkey,
                        lineitem: l.l_linenumber
                    }
    """
    print("C4. Complex Join 2:", await db.arun(query_c4))

    # C5. Neighborhood Search
    query_c5 = """
        FOR v, e, p IN 1 OUTBOUND "customer/1" GRAPH "tpchgraph"
            RETURN {vertex: v, edge: e}
    """
    print("C5. Neighborhood Search:", await db.arun(query_c5))

    # C6. Shortest Path
    query_c6 = """
        FOR v, e IN OUTBOUND SHORTEST_PATH
            "customer/1" TO "customer/5"
            GRAPH "tpchgraph"
            RETURN v
    """
    print("C6. Shortest Path:", await db.arun(query_c6))

    # C7. Optional Traversal
    # query_c7 = """
    #     FOR c IN customer
    #         LET orders = (
    #             FOR o IN OUTBOUND c GRAPH "tpchgraph"
    #                 RETURN o
    #         )
    #         RETURN {
    #             customer: c.c_name,
    #             orders: LENGTH(orders) > 0 ? orders : null
    #         }
    # """
    # print("C7. Optional Traversal:", await db.arun(query_c7))

    # D. Set Operations
    # D1. Union
    query_d1 = """
        LET auto = (
            FOR c IN customer
                FILTER c.c_mktsegment == "AUTOMOBILE"
                RETURN c.c_custkey
        )
        LET building = (
            FOR c IN customer
                FILTER c.c_mktsegment == "BUILDING"
                RETURN c.c_custkey
        )
        RETURN UNION_DISTINCT(auto, building)
    """
    print("D1. Set Operations (UNION):", await db.arun(query_d1))

    # D2. Intersection
    query_d2 = """
        LET customers_with_orders = (
            FOR o IN orders
                RETURN o.o_custkey
        )
        LET customers_with_lineitems = (
            FOR l IN lineitem
                LET o = DOCUMENT("orders", l.l_orderkey)
                RETURN o.o_custkey
        )
        RETURN INTERSECTION(customers_with_orders, customers_with_lineitems)
    """
    print("D2. Set Operations (INTERSECTION):", await db.arun(query_d2))

    # D3. Difference
    query_d3 = """
        LET customers_with_orders = (
            FOR o IN orders
                RETURN o.o_custkey
        )
        LET customers_with_lineitems = (
            FOR l IN lineitem
                LET o = DOCUMENT("orders", l.l_orderkey)
                RETURN o.o_custkey
        )
        RETURN MINUS(customers_with_orders, customers_with_lineitems)
    """
    print("D3. Set Operations (DIFFERENCE):", await db.arun(query_d3))

    # E. Result Modification
    # E1. Non-Indexed Columns Sorting
    query_e1 = """
        FOR c IN customer
            SORT c.c_name ASC
            LIMIT 10
            RETURN {custKey: c.c_custkey, name: c.c_name}
    """
    print("E1. Result Modification (Non-Indexed Sorting):", await db.arun(query_e1))

    # E2. Indexed Columns Sorting
    query_e2 = """
        FOR c IN customer
            SORT c.c_custkey DESC
            LIMIT 10
            RETURN {custKey: c.c_custkey, name: c.c_name}
    """
    print("E2. Result Modification (Indexed Sorting):", await db.arun(query_e2))

    # E3. Distinct
    query_e3 = """
        FOR c IN customer
            COLLECT segment = c.c_mktsegment
            RETURN segment
    """
    print("E3. Result Modification (DISTINCT):", await db.arun(query_e3))

    await db.aclose()


if __name__ == "__main__":
    asyncio.run(main())
