import asyncio
from typing import Any
from neo4j import AsyncDriver, AsyncGraphDatabase, Record
from neo4j.graph import Node, Relationship

class Neo4jDB:
    def __init__(self, config: dict[str, Any] | None = None) -> None:
        self.config = config or {
            "uri": "bolt://localhost:7687",
            "user": "neo4j",
            "password": "password",
            # "database": "???"
        }
        self.driver: AsyncDriver | None = None

    def connect(self) -> None:
        self.driver = AsyncGraphDatabase.driver(
            uri=self.config["uri"],
            auth=(self.config["user"], self.config["password"]),
            database=self.config["database"] if "database" in self.config else None,
        )

    async def arun(self, query: str, params: dict[str, Any] | None = None):
        async with self.driver.session(max_transaction_retry_time=60) as session:
            result = await session.run(query, params)
            records = await result.data()
            return records

    async def aclose(self) -> None:
        if self.driver:
            await self.driver.close()


async def main():
    db = Neo4jDB()
    db.connect()

    # A. Selection, Projection, Source (of Data)
    # A1. Non-Indexed Columns
    query_a1 = """
        MATCH (c:Customer)
        WHERE c.mktsegment = 'BUILDING'
        RETURN c.name, c.address, c.phone
        LIMIT 10
    """
    print("A1. Selection/Projection (Non-Indexed):", await db.arun(query_a1))

    # A2. Non-Indexed Columns with complex condition
    query_a2 = """
        MATCH (p:Part)
        WHERE p.type CONTAINS 'STEEL' AND p.size > 25
        RETURN p.name, p.type, p.size, p.retailprice
        LIMIT 10
    """
    print("A2. Selection/Projection (Non-Indexed Complex):", await db.arun(query_a2))

    # A3. Indexed Columns
    query_a3 = """
        MATCH (s:Supplier)
        WHERE s.suppkey IN [1, 10, 100]
        RETURN s.suppkey, s.name, s.address
    """
    print("A3. Selection/Projection (Indexed):", await db.arun(query_a3))

    # B. Aggregation
    # B1. COUNT
    query_b1 = """
        MATCH (c:Customer)-[:LOCATED_IN]->(n:Nation)
        RETURN n.name as nation, COUNT(c) as customer_count
        ORDER BY customer_count DESC
        LIMIT 5
    """
    print("B1. Aggregation (COUNT):", await db.arun(query_b1))

    # B2. MAX
    query_b2 = """
        MATCH (o:Order)
        RETURN MAX(o.totalprice) as max_order_value,
               MIN(o.totalprice) as min_order_value,
               AVG(o.totalprice) as avg_order_value
    """
    print("B2. Aggregation (MAX/MIN/AVG):", await db.arun(query_b2))

    # C. Joins
    # C1. Non-Indexed Columns Join
    query_c1 = """
        MATCH (c:Customer)-[:LOCATED_IN]->(n:Nation)
        WHERE c.mktsegment = 'AUTOMOBILE'
        RETURN c.name, n.name as nation
        LIMIT 10
    """
    print("C1. Joins (Non-Indexed):", await db.arun(query_c1))

    # C2. Indexed Columns Join
    query_c2 = """
        MATCH (o:Order)<-[:PLACED]-(c:Customer)
        WHERE o.orderkey = 1
        RETURN o.orderkey, o.totalprice, c.name, c.custkey
    """
    print("C2. Joins (Indexed):", await db.arun(query_c2))

    # C3. Complex Join 1
    query_c3 = """
        MATCH (c:Customer)-[:PLACED]->(o:Order)-[:CONTAINS]->(li:LineItem)-[:OF_PART]->(p:Part)
        WHERE p.type CONTAINS 'BRASS'
        RETURN c.name, o.orderkey, p.name, li.quantity, li.extendedprice
        LIMIT 10
    """
    print("C3. Complex Join 1:", await db.arun(query_c3))

    # C4. Complex Join 2
    query_c4 = """
        MATCH (s:Supplier)-[:LOCATED_IN]->(n:Nation)-[:BELONGS_TO]->(r:Region)
        MATCH (s)-[:SUPPLIES]->(p:Part)
        WHERE r.name = 'AMERICA'
        RETURN s.name, n.name, p.name, p.retailprice
        LIMIT 10
    """
    print("C4. Complex Join 2:", await db.arun(query_c4))

    # C5. Neighborhood Search
    query_c5 = """
        MATCH (c:Customer {custkey: 1})-[:PLACED]->(o:Order)-[:CONTAINS]->(li:LineItem)
        RETURN c.name, COUNT(o) as order_count, SUM(li.extendedprice) as total_spent
    """
    print("C5. Neighborhood Search:", await db.arun(query_c5))

    # C6. Shortest Path
    query_c6 = """
        MATCH path = shortestPath((s:Supplier)-[*]-(c:Customer))
        WHERE s.suppkey = 1 AND c.custkey = 1
        RETURN length(path) as path_length, nodes(path)
        LIMIT 1
    """
    print("C6. Shortest Path:", await db.arun(query_c6))

    # C7. Optional Traversal
    query_c7 = """
        MATCH (c:Customer)
        OPTIONAL MATCH (c)-[:PLACED]->(o:Order)
        RETURN c.name, COUNT(o) as order_count
        ORDER BY order_count DESC
        LIMIT 10
    """
    print("C7. Optional Traversal:", await db.arun(query_c7))

    # D. Set Operations
    # D1. Union
    # query_d1 = """
    #     MATCH (n:Nation)-[:BELONGS_TO]->(r:Region {name: 'AMERICA'})
    #     RETURN n.name as nation, 'AMERICA' as region
    #     UNION
    #     MATCH (n:Nation)-[:BELONGS_TO]->(r:Region {name: 'EUROPE'})
    #     RETURN n.name as nation, 'EUROPE' as region
    #     ORDER BY nation
    #     LIMIT 10
    # """
    # print("D1. Set Operations (UNION):", await db.arun(query_d1))

    # D2. Intersection
    # query_d2 = """
    #     MATCH (c1:Customer)-[:PLACED]->(o1:Order)-[:CONTAINS]->(li1:LineItem)-[:OF_PART]->(p:Part)
    #     MATCH (c2:Customer)-[:PLACED]->(o2:Order)-[:CONTAINS]->(li2:LineItem)-[:OF_PART]->(p)
    #     WHERE c1.custkey < c2.custkey
    #     RETURN p.name, COUNT(DISTINCT c1) + COUNT(DISTINCT c2) as shared_customers
    #     LIMIT 5
    # """
    # print("D2. Set Operations (INTERSECTION):", await db.arun(query_d2))

    # D3. Difference
    query_d3 = """
        MATCH (c:Customer)
        WHERE NOT EXISTS((c)-[:PLACED]->(:Order))
        RETURN c.name, c.custkey
        LIMIT 10
    """
    print("D3. Set Operations (DIFFERENCE):", await db.arun(query_d3))

    # E. Result Modification
    # E1. Non-Indexed Columns Sorting
    query_e1 = """
        MATCH (p:Part)
        RETURN p.name, p.retailprice
        ORDER BY p.retailprice DESC
        LIMIT 10
    """
    print("E1. Result Modification (Non-Indexed Sorting):", await db.arun(query_e1))

    # E2. Indexed Columns Sorting
    query_e2 = """
        MATCH (c:Customer)
        RETURN c.custkey, c.name, c.acctbal
        ORDER BY c.custkey ASC
        LIMIT 10
    """
    print("E2. Result Modification (Indexed Sorting):", await db.arun(query_e2))

    # E3. Distinct
    query_e3 = """
        MATCH (li:LineItem)
        RETURN DISTINCT li.shipmode
        ORDER BY li.shipmode
    """
    print("E3. Result Modification (DISTINCT):", await db.arun(query_e3))

    await db.aclose()


if __name__ == "__main__":
    asyncio.run(main())
