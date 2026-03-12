package test.query.tpch

import test.{SQLStringQueryTest, SQLStringAggregationTest, TestDatabase}
import tyql.*
import tyql.Expr.{sum, avg, min, max, count, countAll, toRow, DoubleLit, DateLit}

import language.experimental.namedTuples
import NamedTuple.*
import scala.language.implicitConversions
import java.time.LocalDate

// TPC-H table definitions
case class Lineitem(
  l_orderkey: Int, l_partkey: Int, l_suppkey: Int,
  l_quantity: Double, l_extendedprice: Double, l_discount: Double, l_tax: Double,
  l_returnflag: String, l_linestatus: String,
  l_shipdate: LocalDate, l_commitdate: LocalDate, l_receiptdate: LocalDate
)

case class Orders(
  o_orderkey: Int, o_custkey: Int,
  o_totalprice: Double, o_orderdate: LocalDate,
  o_orderpriority: String, o_shippriority: Int
)

case class Customer(
  c_custkey: Int, c_name: String,
  c_nationkey: Int, c_acctbal: Double,
  c_mktsegment: String, c_phone: String,
  c_address: String, c_comment: String
)

case class Nation(
  n_nationkey: Int, n_name: String, n_regionkey: Int
)

case class Supplier(
  s_suppkey: Int, s_name: String,
  s_nationkey: Int, s_acctbal: Double
)

case class Partsupp(
  ps_partkey: Int, ps_suppkey: Int,
  ps_availqty: Double, ps_supplycost: Double
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
 * WHERE l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1995-01-01'
 *   AND l_discount >= 0.05 AND l_discount <= 0.07
 *   AND l_quantity < 24.0
 */
class TPCHQ6Test extends SQLStringAggregationTest[TPCHDB, Double] {
  def testDescription = "TPCH Q6: forecasting revenue change"
  def query() =
    testDB.tables.lineitem
      .filter(l =>
        l.l_shipdate >= LocalDate.of(1994, 1, 1) && l.l_shipdate < LocalDate.of(1995, 1, 1)
          && l.l_discount >= 0.05 && l.l_discount <= 0.07
          && l.l_quantity < 24.0
      )
      .aggregate(l => sum(l.l_extendedprice * l.l_discount))

  def expectedQueryPattern = """
    SELECT SUM((lineitem$A.l_extendedprice * lineitem$A.l_discount))
    FROM lineitem as lineitem$A
    WHERE lineitem$A.l_shipdate >= DATE '1994-01-01'
      AND lineitem$A.l_shipdate < DATE '1995-01-01'
      AND lineitem$A.l_discount >= 0.05
      AND lineitem$A.l_discount <= 0.07
      AND lineitem$A.l_quantity < 24.0
  """
}


/**
 * TPC-H Q1 (Pricing Summary Report)
 * Features: filter, groupBy with multiple aggregations (sum, avg, countAll),
 *           arithmetic (subtraction, addition, multiplication), multi-key sort
 *
 * SELECT l_returnflag, l_linestatus,
 *        SUM(l_quantity) as sum_qty,
 *        SUM(l_extendedprice) as sum_base_price,
 *        SUM(l_extendedprice * (1 - l_discount)) as sum_disc_price,
 *        SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
 *        AVG(l_quantity) as avg_qty,
 *        AVG(l_extendedprice) as avg_price,
 *        AVG(l_discount) as avg_disc,
 *        COUNT(*) as count_order
 * FROM lineitem
 * WHERE l_shipdate <= DATE '1998-09-02'
 * GROUP BY l_returnflag, l_linestatus
 * ORDER BY l_returnflag, l_linestatus
 */
class TPCHQ1Test extends SQLStringQueryTest[TPCHDB,
  (l_returnflag: String, l_linestatus: String,
   sum_qty: Double, sum_base_price: Double, sum_disc_price: Double, sum_charge: Double,
   avg_qty: Double, avg_price: Double, avg_disc: Double, count_order: Int)] {
  def testDescription = "TPCH Q1: pricing summary report"
  def query() =
    testDB.tables.lineitem
      .filter(l => l.l_shipdate <= LocalDate.of(1998, 9, 2))
      .groupBy(
        l => (l_returnflag = l.l_returnflag, l_linestatus = l.l_linestatus).toRow,
        l => {
          val res = (
            l_returnflag = l.l_returnflag,
            l_linestatus = l.l_linestatus,
            sum_qty = sum(l.l_quantity),
            sum_base_price = sum(l.l_extendedprice),
            sum_disc_price = sum(l.l_extendedprice * (DoubleLit(1.0) - l.l_discount)),
            sum_charge = sum(l.l_extendedprice * (DoubleLit(1.0) - l.l_discount) * (DoubleLit(1.0) + l.l_tax)),
            avg_qty = avg(l.l_quantity),
            avg_price = avg(l.l_extendedprice),
            avg_disc = avg(l.l_discount),
            count_order = countAll
          )
          res.toRow
        }
      )
      .sort(_.l_linestatus, Ord.ASC).sort(_.l_returnflag, Ord.ASC)

  def expectedQueryPattern = """
    SELECT lineitem$A.l_returnflag as l_returnflag,
      lineitem$A.l_linestatus as l_linestatus,
      SUM(lineitem$A.l_quantity) as sum_qty,
      SUM(lineitem$A.l_extendedprice) as sum_base_price,
      SUM((lineitem$A.l_extendedprice * (1.0 - lineitem$A.l_discount))) as sum_disc_price,
      SUM(((lineitem$A.l_extendedprice * (1.0 - lineitem$A.l_discount)) * (1.0 + lineitem$A.l_tax))) as sum_charge,
      AVG(lineitem$A.l_quantity) as avg_qty,
      AVG(lineitem$A.l_extendedprice) as avg_price,
      AVG(lineitem$A.l_discount) as avg_disc,
      COUNT(*) as count_order
    FROM lineitem as lineitem$A
    WHERE lineitem$A.l_shipdate <= DATE '1998-09-02'
    GROUP BY lineitem$A.l_returnflag, lineitem$A.l_linestatus
    ORDER BY l_returnflag ASC, l_linestatus ASC
  """
}


/**
 * TPC-H Q3 (Shipping Priority)
 * Features: 3-way join via aggregate, filter, groupBy with sum, sort, limit
 *
 * SELECT l_orderkey, SUM(l_extendedprice * (1 - l_discount)) as revenue,
 *        o_orderdate, o_shippriority
 * FROM customer, orders, lineitem
 * WHERE c_mktsegment = 'BUILDING'
 *   AND c_custkey = o_custkey
 *   AND l_orderkey = o_orderkey
 *   AND o_orderdate < DATE '1995-03-15'
 *   AND l_shipdate > DATE '1995-03-15'
 * GROUP BY l_orderkey, o_orderdate, o_shippriority
 * ORDER BY revenue DESC, o_orderdate
 * LIMIT 10
 */
class TPCHQ3Test extends SQLStringQueryTest[TPCHDB,
  (l_orderkey: Int, revenue: Double, o_orderdate: LocalDate, o_shippriority: Int)] {
  def testDescription = "TPCH Q3: shipping priority"
  def query() =
    testDB.tables.customer
      .filter(c => c.c_mktsegment == "BUILDING")
      .aggregate(c =>
        testDB.tables.orders
          .filter(o => c.c_custkey == o.o_custkey && o.o_orderdate < LocalDate.of(1995, 3, 15))
          .aggregate(o =>
            testDB.tables.lineitem
              .filter(l => l.l_orderkey == o.o_orderkey && l.l_shipdate > LocalDate.of(1995, 3, 15))
              .aggregate(l =>
                (l_orderkey = l.l_orderkey, revenue = sum(l.l_extendedprice * (DoubleLit(1.0) - l.l_discount)),
                 o_orderdate = o.o_orderdate, o_shippriority = o.o_shippriority).toGroupingRow
              )
          )
      ).groupBySource(
        p => (l_orderkey = p._3.l_orderkey, o_orderdate = p._2.o_orderdate, o_shippriority = p._2.o_shippriority).toRow
      )
      .sort(_.o_orderdate, Ord.ASC).sort(_.revenue, Ord.DESC)
      .take(10)

  def expectedQueryPattern = """
    SELECT lineitem$C.l_orderkey as l_orderkey,
      SUM((lineitem$C.l_extendedprice * (1.0 - lineitem$C.l_discount))) as revenue,
      orders$B.o_orderdate as o_orderdate, orders$B.o_shippriority as o_shippriority
    FROM customer as customer$A, orders as orders$B, lineitem as lineitem$C
    WHERE (customer$A.c_mktsegment = 'BUILDING'
      AND customer$A.c_custkey = orders$B.o_custkey
      AND orders$B.o_orderdate < DATE '1995-03-15'
      AND lineitem$C.l_orderkey = orders$B.o_orderkey
      AND lineitem$C.l_shipdate > DATE '1995-03-15')
    GROUP BY lineitem$C.l_orderkey, orders$B.o_orderdate, orders$B.o_shippriority
    ORDER BY revenue DESC, o_orderdate ASC
    LIMIT 10
  """
}


/**
 * TPC-H Q4 (Order Priority Checking)
 * Features: correlated subquery (.size), filter, groupBy with count, sort
 *
 * SELECT o_orderpriority, COUNT(*) as order_count
 * FROM orders
 * WHERE o_orderdate >= DATE '1993-07-01' AND o_orderdate < DATE '1993-10-01'
 *   AND EXISTS (SELECT * FROM lineitem
 *        WHERE l_orderkey = o_orderkey AND l_commitdate < l_receiptdate)
 * GROUP BY o_orderpriority
 * ORDER BY o_orderpriority ASC
 */
class TPCHQ4Test extends SQLStringQueryTest[TPCHDB, (o_orderpriority: String, order_count: Int)] {
  def testDescription = "TPCH Q4: order priority checking"
  def query() =
    testDB.tables.orders
      .filter(o =>
        o.o_orderdate >= LocalDate.of(1993, 7, 1) && o.o_orderdate < LocalDate.of(1993, 10, 1)
          && testDB.tables.lineitem
            .filter(l => l.l_orderkey == o.o_orderkey && l.l_commitdate < l.l_receiptdate)
            .size > 0
      )
      .groupBy(
        o => (o_orderpriority = o.o_orderpriority).toRow,
        o => (o_orderpriority = o.o_orderpriority, order_count = countAll).toRow
      )
      .sort(_.o_orderpriority, Ord.ASC)

  def expectedQueryPattern = """
    SELECT orders$A.o_orderpriority as o_orderpriority, COUNT(*) as order_count
    FROM orders as orders$A
    WHERE orders$A.o_orderdate >= DATE '1993-07-01'
      AND orders$A.o_orderdate < DATE '1993-10-01'
      AND (SELECT COUNT(1) FROM lineitem as lineitem$B
           WHERE lineitem$B.l_orderkey = orders$A.o_orderkey AND lineitem$B.l_commitdate < lineitem$B.l_receiptdate) > 0
    GROUP BY orders$A.o_orderpriority
    ORDER BY o_orderpriority ASC
  """
}


/**
 * TPC-H Q11 (Important Stock Identification)
 * Features: 3-way join via aggregate, groupBy with having (scalar subquery), sort
 *
 * SELECT ps_partkey, SUM(ps_supplycost * ps_availqty) as value
 * FROM partsupp, supplier, nation
 * WHERE ps_suppkey = s_suppkey AND s_nationkey = n_nationkey AND n_name = 'GERMANY'
 * GROUP BY ps_partkey
 * HAVING SUM(ps_supplycost * ps_availqty) > (
 *   SELECT SUM(ps_supplycost * ps_availqty) * 0.0001
 *   FROM partsupp, supplier, nation
 *   WHERE ps_suppkey = s_suppkey AND s_nationkey = n_nationkey AND n_name = 'GERMANY'
 * )
 * ORDER BY value DESC
 */
class TPCHQ11Test extends SQLStringQueryTest[TPCHDB, (ps_partkey: Int, value: Double)] {
  def testDescription = "TPCH Q11: important stock identification"
  def query() =
    val threshold =
      testDB.tables.partsupp.aggregate(ps2 =>
        testDB.tables.supplier
          .filter(s2 => ps2.ps_suppkey == s2.s_suppkey)
          .aggregate(s2 =>
            testDB.tables.nation
              .filter(n2 => s2.s_nationkey == n2.n_nationkey && n2.n_name == "GERMANY")
              .aggregate(n2 => sum(ps2.ps_supplycost * ps2.ps_availqty))
          )
      ) * 0.0001

    testDB.tables.partsupp.aggregate(ps =>
      testDB.tables.supplier
        .filter(s => ps.ps_suppkey == s.s_suppkey)
        .aggregate(s =>
          testDB.tables.nation
            .filter(n => s.s_nationkey == n.n_nationkey && n.n_name == "GERMANY")
            .aggregate(n =>
              (ps_partkey = ps.ps_partkey, value = sum(ps.ps_supplycost * ps.ps_availqty)).toGroupingRow
            )
        )
    ).groupBySource(
      p => (ps_partkey = p._1.ps_partkey).toRow
    ).having(p => sum(p._1.ps_supplycost * p._1.ps_availqty) > threshold)
      .sort(_.value, Ord.DESC)

  def expectedQueryPattern = """
    SELECT partsupp$A.ps_partkey as ps_partkey, SUM((partsupp$A.ps_supplycost * partsupp$A.ps_availqty)) as value
    FROM partsupp as partsupp$A, supplier as supplier$B, nation as nation$C
    WHERE (partsupp$A.ps_suppkey = supplier$B.s_suppkey
      AND supplier$B.s_nationkey = nation$C.n_nationkey
      AND nation$C.n_name = 'GERMANY')
    GROUP BY partsupp$A.ps_partkey
    HAVING SUM((partsupp$A.ps_supplycost * partsupp$A.ps_availqty)) > ((SELECT SUM((partsupp$D.ps_supplycost * partsupp$D.ps_availqty)) FROM partsupp as partsupp$D, supplier as supplier$E, nation as nation$F WHERE (partsupp$D.ps_suppkey = supplier$E.s_suppkey AND supplier$E.s_nationkey = nation$F.n_nationkey AND nation$F.n_name = 'GERMANY')) * 1.0E-4)
    ORDER BY value DESC
  """
}


/**
 * TPC-H Q10 (Returned Item Reporting)
 * Features: 4-way join via aggregate, filter, groupBy with sum, sort, limit
 *
 * SELECT c_custkey, c_name, SUM(l_extendedprice * (1 - l_discount)) as revenue,
 *        c_acctbal, n_name, c_address, c_phone, c_comment
 * FROM customer, orders, lineitem, nation
 * WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
 *   AND o_orderdate >= DATE '1993-10-01' AND o_orderdate < DATE '1994-01-01'
 *   AND l_returnflag = 'R' AND c_nationkey = n_nationkey
 * GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment
 * ORDER BY revenue DESC
 * LIMIT 20
 */
class TPCHQ10Test extends SQLStringQueryTest[TPCHDB,
  (c_custkey: Int, c_name: String, revenue: Double, c_acctbal: Double,
   n_name: String, c_address: String, c_phone: String, c_comment: String)] {
  def testDescription = "TPCH Q10: returned item reporting"
  def query() =
    testDB.tables.customer
      .aggregate(c =>
        testDB.tables.orders
          .filter(o => c.c_custkey == o.o_custkey && o.o_orderdate >= LocalDate.of(1993, 10, 1) && o.o_orderdate < LocalDate.of(1994, 1, 1))
          .aggregate(o =>
            testDB.tables.lineitem
              .filter(l => l.l_orderkey == o.o_orderkey && l.l_returnflag == "R")
              .aggregate(l =>
                testDB.tables.nation
                  .filter(n => c.c_nationkey == n.n_nationkey)
                  .aggregate(n =>
                    (c_custkey = c.c_custkey, c_name = c.c_name,
                     revenue = sum(l.l_extendedprice * (DoubleLit(1.0) - l.l_discount)),
                     c_acctbal = c.c_acctbal, n_name = n.n_name,
                     c_address = c.c_address, c_phone = c.c_phone, c_comment = c.c_comment).toGroupingRow
                  )
              )
          )
      ).groupBySource(
        p => (c_custkey = p._1.c_custkey, c_name = p._1.c_name, c_acctbal = p._1.c_acctbal,
              c_phone = p._1.c_phone, n_name = p._4.n_name, c_address = p._1.c_address, c_comment = p._1.c_comment).toRow
      )
      .sort(_.revenue, Ord.DESC)
      .take(20)

  def expectedQueryPattern = """
    SELECT customer$A.c_custkey as c_custkey, customer$A.c_name as c_name,
      SUM((lineitem$C.l_extendedprice * (1.0 - lineitem$C.l_discount))) as revenue,
      customer$A.c_acctbal as c_acctbal, nation$D.n_name as n_name,
      customer$A.c_address as c_address, customer$A.c_phone as c_phone, customer$A.c_comment as c_comment
    FROM customer as customer$A, orders as orders$B, lineitem as lineitem$C, nation as nation$D
    WHERE (customer$A.c_custkey = orders$B.o_custkey
      AND orders$B.o_orderdate >= DATE '1993-10-01'
      AND orders$B.o_orderdate < DATE '1994-01-01'
      AND lineitem$C.l_orderkey = orders$B.o_orderkey
      AND lineitem$C.l_returnflag = 'R'
      AND customer$A.c_nationkey = nation$D.n_nationkey)
    GROUP BY customer$A.c_custkey, customer$A.c_name, customer$A.c_acctbal, customer$A.c_phone, nation$D.n_name, customer$A.c_address, customer$A.c_comment
    ORDER BY revenue DESC
    LIMIT 20
  """
}
