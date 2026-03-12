package test.query.tpch

import test.{SQLStringQueryTest, SQLStringAggregationTest, TestDatabase}
import tyql.*
import tyql.Expr.{sum, avg, min, max, count, toRow}

import language.experimental.namedTuples
import NamedTuple.*
import scala.language.implicitConversions

// Simplified TPC-H table definitions
case class Lineitem(
  orderkey: Int, partkey: Int, suppkey: Int,
  quantity: Double, extendedprice: Double, discount: Double, tax: Double,
  returnflag: String, linestatus: String,
  shipdate: Int, commitdate: Int, receiptdate: Int
)

case class Orders(
  orderkey: Int, custkey: Int,
  totalprice: Double, orderdate: Int,
  orderpriority: String, shippriority: Int
)

case class Customer(
  custkey: Int, name: String,
  nationkey: Int, acctbal: Double,
  mktsegment: String, phone: String
)

case class Nation(
  nationkey: Int, name: String, regionkey: Int
)

case class Supplier(
  suppkey: Int, name: String,
  nationkey: Int, acctbal: Double
)

case class Partsupp(
  partkey: Int, suppkey: Int,
  availqty: Double, supplycost: Double
)

type TPCHDB = (
  lineitem: Lineitem,
  orders: Orders,
  customer: Customer,
  nation: Nation,
  supplier: Supplier,
  partsupp: Partsupp
)

given tpchDBs: TestDatabase[TPCHDB] with
  override def tables = (
    lineitem = Table[Lineitem]("lineitem"),
    orders = Table[Orders]("orders"),
    customer = Table[Customer]("customer"),
    nation = Table[Nation]("nation"),
    supplier = Table[Supplier]("supplier"),
    partsupp = Table[Partsupp]("partsupp")
  )

  override def init(): String = ""


/**
 * TPC-H Q6 (Forecasting Revenue Change)
 * Features: filter, aggregate (sum), arithmetic (multiplication)
 *
 * SELECT SUM(l_extendedprice * l_discount) as revenue
 * FROM lineitem
 * WHERE l_shipdate > 19940000 AND l_shipdate < 19950101
 *   AND l_discount > 0.05 AND l_discount < 0.07
 *   AND l_quantity < 24.0
 */
class TPCHQ6Test extends SQLStringAggregationTest[TPCHDB, Double] {
  def testDescription = "TPCH Q6: forecasting revenue change"
  def query() =
    testDB.tables.lineitem
      .filter(l =>
        l.shipdate > 19940000 && l.shipdate < 19950101
          && l.discount > 0.05 && l.discount < 0.07
          && l.quantity < 24.0
      )
      .aggregate(l => sum(l.extendedprice * l.discount))

  def expectedQueryPattern = """
    SELECT SUM(lineitem$A.extendedprice * lineitem$A.discount)
    FROM lineitem as lineitem$A
    WHERE lineitem$A.shipdate > 19940000
      AND lineitem$A.shipdate < 19950101
      AND lineitem$A.discount > 0.05
      AND lineitem$A.discount < 0.07
      AND lineitem$A.quantity < 24.0
  """
}


/**
 * TPC-H Q1 (Pricing Summary Report) - simplified
 * Features: filter, groupBy with multiple aggregations, sort
 *
 * SELECT l_returnflag,
 *        SUM(l_quantity) as sum_qty,
 *        SUM(l_extendedprice) as sum_base_price,
 *        AVG(l_quantity) as avg_qty,
 *        AVG(l_extendedprice) as avg_price,
 *        AVG(l_discount) as avg_disc
 * FROM lineitem
 * WHERE l_shipdate <= 19980902
 * GROUP BY l_returnflag
 * ORDER BY l_returnflag ASC
 */
class TPCHQ1Test extends SQLStringQueryTest[TPCHDB, (sumQty: Double, sumBasePrice: Double, avgQty: Double, avgPrice: Double, avgDisc: Double)] {
  def testDescription = "TPCH Q1: pricing summary report"
  def query() =
    import AggregationExpr.toRow
    testDB.tables.lineitem
      .filter(l => l.shipdate <= 19980902)
      .groupBy(
        l => (returnflag = l.returnflag).toRow,
        l => (sumQty = sum(l.quantity), sumBasePrice = sum(l.extendedprice), avgQty = avg(l.quantity), avgPrice = avg(l.extendedprice), avgDisc = avg(l.discount)).toRow
      )
      .sort(_.sumQty, Ord.ASC)

  def expectedQueryPattern = """
    SELECT SUM(lineitem$A.quantity) as sumQty, SUM(lineitem$A.extendedprice) as sumBasePrice,
      AVG(lineitem$A.quantity) as avgQty, AVG(lineitem$A.extendedprice) as avgPrice,
      AVG(lineitem$A.discount) as avgDisc
    FROM lineitem as lineitem$A
    WHERE lineitem$A.shipdate <= 19980902
    GROUP BY lineitem$A.returnflag
    ORDER BY sumQty ASC
  """
}


/**
 * TPC-H Q3 (Shipping Priority) - simplified
 * Features: 3-way join (for-comprehension), filter, sort, limit
 *
 * SELECT c_name, o_orderdate, l_extendedprice, o_shippriority
 * FROM customer, orders, lineitem
 * WHERE c_mktsegment = 'BUILDING'
 *   AND c_custkey = o_custkey
 *   AND l_orderkey = o_orderkey
 *   AND o_orderdate < 19950315
 *   AND l_shipdate > 19950315
 * ORDER BY l_extendedprice DESC
 * LIMIT 10
 */
class TPCHQ3Test extends SQLStringQueryTest[TPCHDB, (name: String, orderdate: Int, extendedprice: Double, shippriority: Int)] {
  def testDescription = "TPCH Q3: shipping priority"
  def query() =
    (for
      c <- testDB.tables.customer
      if c.mktsegment == "BUILDING"
      o <- testDB.tables.orders
      if c.custkey == o.custkey
      l <- testDB.tables.lineitem
      if l.orderkey == o.orderkey && o.orderdate < 19950315 && l.shipdate > 19950315
    yield (name = c.name, orderdate = o.orderdate, extendedprice = l.extendedprice, shippriority = o.shippriority).toRow)
      .sort(_.extendedprice, Ord.DESC)
      .take(10)

  def expectedQueryPattern = """
    SELECT customer$A.name as name, orders$B.orderdate as orderdate,
      lineitem$C.extendedprice as extendedprice, orders$B.shippriority as shippriority
    FROM customer as customer$A, orders as orders$B, lineitem as lineitem$C
    WHERE (customer$A.mktsegment = "BUILDING"
      AND customer$A.custkey = orders$B.custkey
      AND lineitem$C.orderkey = orders$B.orderkey
      AND orders$B.orderdate < 19950315
      AND lineitem$C.shipdate > 19950315)
    ORDER BY extendedprice DESC
    LIMIT 10
  """
}


/**
 * TPC-H Q4 (Order Priority Checking) - simplified
 * Features: correlated subquery (.size), filter, groupBy with count, sort
 *
 * SELECT o_orderpriority, COUNT(o_orderkey) as order_count
 * FROM orders
 * WHERE o_orderdate > 19930700 AND o_orderdate < 19931001
 *   AND (SELECT COUNT(1) FROM lineitem
 *        WHERE l_orderkey = o_orderkey AND l_commitdate < l_receiptdate) > 0
 * GROUP BY o_orderpriority
 * ORDER BY o_orderpriority ASC
 */
class TPCHQ4Test extends SQLStringQueryTest[TPCHDB, (orderCount: Int)] {
  def testDescription = "TPCH Q4: order priority checking"
  def query() =
    import AggregationExpr.toRow
    testDB.tables.orders
      .filter(o =>
        o.orderdate > 19930700 && o.orderdate < 19931001
          && testDB.tables.lineitem
            .filter(l => l.orderkey == o.orderkey && l.commitdate < l.receiptdate)
            .size > 0
      )
      .groupBy(
        o => (orderpriority = o.orderpriority).toRow,
        o => (orderCount = count(o.orderkey)).toRow
      )
      .sort(_.orderCount, Ord.ASC)

  def expectedQueryPattern = """
    SELECT COUNT(orders$A.orderkey) as orderCount
    FROM orders as orders$A
    WHERE orders$A.orderdate > 19930700
      AND orders$A.orderdate < 19931001
      AND (SELECT COUNT(1) FROM lineitem as lineitem$B
           WHERE lineitem$B.orderkey = orders$A.orderkey AND lineitem$B.commitdate < lineitem$B.receiptdate) > 0
    GROUP BY orders$A.orderpriority
    ORDER BY orderCount ASC
  """
}


/**
 * TPC-H Q11 (Important Stock Identification) - simplified
 * Features: 2-way join via aggregate, groupBy with having, sort
 *
 * SELECT SUM(ps_supplycost * ps_availqty) as value
 * FROM partsupp, supplier
 * WHERE ps_suppkey = s_suppkey
 * GROUP BY ps_partkey
 * HAVING SUM(ps_supplycost * ps_availqty) > 1000
 * ORDER BY value DESC
 */
class TPCHQ11Test extends SQLStringQueryTest[TPCHDB, (value: Double)] {
  def testDescription = "TPCH Q11: important stock identification"
  def query() =
    testDB.tables.partsupp.aggregate(ps =>
      testDB.tables.supplier
        .filter(s => ps.suppkey == s.suppkey)
        .aggregate(s =>
          (value = sum(ps.supplycost * ps.availqty)).toGroupingRow
        )
    ).groupBySource(
      p => (partkey = p._1.partkey).toRow
    ).having(p => sum(p._1.supplycost * p._1.availqty) > 1000.0)
      .sort(_.value, Ord.DESC)

  def expectedQueryPattern = """
    SELECT SUM(partsupp$A.supplycost * partsupp$A.availqty) as value
    FROM partsupp as partsupp$A, supplier as supplier$B
    WHERE partsupp$A.suppkey = supplier$B.suppkey
    GROUP BY partsupp$A.partkey
    HAVING SUM(partsupp$A.supplycost * partsupp$A.availqty) > 1000.0
    ORDER BY value DESC
  """
}
