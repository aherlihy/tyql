package test.relalg

import test.{RelAlgQueryTest, RelAlgAggregationTest, TestDatabase}
import test.query.tpch.{TPCHDB, given}
import tyql.*
import tyql.Expr.{sum, avg, min, max, count, countAll, toRow, DoubleLit, DateLit}
import tyql.{RelAlgGenerator, TableSchema}
import tyql.RelAlgGenerator.OutputColumn
import scair.dialects.db.{DecimalType, DateType, CharType, DBStringType}
import scair.dialects.builtin.{IntData, StringData, I32, I64}
import scair.ir.Attribute

import language.experimental.namedTuples
import NamedTuple.*
import scala.language.implicitConversions
import java.time.LocalDate

/** TPC-H table schemas for LingoDB relalg MLIR generation.
  * Types match LingoDB's actual database schema:
  * - decimal(p,s) columns use decimal<12,2>
  * - char(N) columns use !db.char<N>
  * - varchar columns use !db.string
  * - integer columns use i32
  */
object TPCHSchemas:
  val decimal: Attribute = DecimalType(IntData(12), IntData(2))
  val dateDay: Attribute = DateType(StringData("day"))
  val dbString: Attribute = DBStringType()
  val int32: Attribute = I32
  def char(n: Int): Attribute = CharType(IntData(n))

  val schemas = Map(
    "lineitem" -> TableSchema(
      "lineitem",
      Map(
        "l_orderkey" -> int32, "l_partkey" -> int32, "l_suppkey" -> int32,
        "l_quantity" -> decimal, "l_extendedprice" -> decimal,
        "l_discount" -> decimal, "l_tax" -> decimal,
        "l_returnflag" -> char(1), "l_linestatus" -> char(1),
        "l_shipdate" -> dateDay, "l_commitdate" -> dateDay, "l_receiptdate" -> dateDay,
      ),
    ),
    "orders" -> TableSchema(
      "orders",
      Map(
        "o_orderkey" -> int32, "o_custkey" -> int32, "o_totalprice" -> decimal,
        "o_orderdate" -> dateDay, "o_orderpriority" -> char(15), "o_shippriority" -> int32,
      ),
    ),
    "customer" -> TableSchema(
      "customer",
      Map(
        "c_custkey" -> int32, "c_name" -> dbString, "c_nationkey" -> int32,
        "c_acctbal" -> decimal, "c_mktsegment" -> char(10),
        "c_phone" -> char(15), "c_address" -> dbString, "c_comment" -> dbString,
      ),
    ),
    "nation" -> TableSchema(
      "nation",
      Map("n_nationkey" -> int32, "n_name" -> char(25), "n_regionkey" -> int32),
    ),
    "supplier" -> TableSchema(
      "supplier",
      Map("s_suppkey" -> int32, "s_name" -> char(25), "s_nationkey" -> int32, "s_acctbal" -> decimal),
    ),
    "partsupp" -> TableSchema(
      "partsupp",
      Map("ps_partkey" -> int32, "ps_suppkey" -> int32, "ps_availqty" -> decimal, "ps_supplycost" -> decimal),
    ),
  )

/** TPC-H Q6 in RelAlg MLIR.
  * Simple: filter + aggregate (SUM) with computed expression.
  */
class TPCHQ6RelAlgTest extends RelAlgAggregationTest[TPCHDB, Double] {
  def testDescription = "TPCH Q6 RelAlg: forecasting revenue change"
  def tableSchemas = TPCHSchemas.schemas
  override def outputFileName = Some("q6.mlir")
  override def outputColumns = Seq(
    OutputColumn("aggr", "result", "revenue", DecimalType(IntData(24), IntData(4))),
  )

  def query() =
    testDB.tables.lineitem
      .filter(l =>
        l.l_shipdate >= LocalDate.of(1994, 1, 1) && l.l_shipdate < LocalDate.of(1995, 1, 1)
          && l.l_discount >= 0.05 && l.l_discount <= 0.07
          && l.l_quantity < 24.0
      )
      .aggregate(l => sum(l.l_extendedprice * l.l_discount))

  def expectedRelAlgPattern = """
    builtin.module {
      %0 = relalg.basetable {table_identifier = "lineitem"} columns: {l_returnflag => @lineitem::@l_returnflag({type = !db.char<1>}), l_linestatus => @lineitem::@l_linestatus({type = !db.char<1>}), l_quantity => @lineitem::@l_quantity({type = !db.decimal<12, 2>}), l_receiptdate => @lineitem::@l_receiptdate({type = !db.date<day>}), l_tax => @lineitem::@l_tax({type = !db.decimal<12, 2>}), l_shipdate => @lineitem::@l_shipdate({type = !db.date<day>}), l_suppkey => @lineitem::@l_suppkey({type = i32}), l_orderkey => @lineitem::@l_orderkey({type = i32}), l_extendedprice => @lineitem::@l_extendedprice({type = !db.decimal<12, 2>}), l_partkey => @lineitem::@l_partkey({type = i32}), l_discount => @lineitem::@l_discount({type = !db.decimal<12, 2>}), l_commitdate => @lineitem::@l_commitdate({type = !db.date<day>})}
      %1 = relalg.selection %0 (%2: !tuples.tuple) {
        %3 = tuples.getcol %2 @lineitem::@l_shipdate : !db.date<day>
        %4 = db.constant("1994-01-01") : !db.date<day>
        %5 = db.compare gte %3:!db.date<day>, %4:!db.date<day>
        %6 = tuples.getcol %2 @lineitem::@l_shipdate : !db.date<day>
        %7 = db.constant("1995-01-01") : !db.date<day>
        %8 = db.compare lt %6:!db.date<day>, %7:!db.date<day>
        %9 = tuples.getcol %2 @lineitem::@l_discount : !db.decimal<12, 2>
        %10 = db.constant("0.05") : !db.decimal<12, 2>
        %11 = db.compare gte %9:!db.decimal<12, 2>, %10:!db.decimal<12, 2>
        %12 = tuples.getcol %2 @lineitem::@l_discount : !db.decimal<12, 2>
        %13 = db.constant("0.07") : !db.decimal<12, 2>
        %14 = db.compare lte %12:!db.decimal<12, 2>, %13:!db.decimal<12, 2>
        %15 = tuples.getcol %2 @lineitem::@l_quantity : !db.decimal<12, 2>
        %16 = db.constant("24.0") : !db.decimal<12, 2>
        %17 = db.compare lt %15:!db.decimal<12, 2>, %16:!db.decimal<12, 2>
        %18 = db.and %5,%8,%11,%14,%17:i1,i1,i1,i1,i1
        tuples.return %18 : i1
      }
      %2 = relalg.map %1 computes : [@map::@result({type = !db.decimal<24, 4>})] (%3: !tuples.tuple) {
        %4 = tuples.getcol %3 @lineitem::@l_extendedprice : !db.decimal<12, 2>
        %5 = tuples.getcol %3 @lineitem::@l_discount : !db.decimal<12, 2>
        %6 = db.mul %4 : !db.decimal<12, 2>, %5 : !db.decimal<12, 2>
        tuples.return %6 : !db.decimal<24, 4>
      }
      %3 = relalg.aggregation %2 [] computes : [@aggr::@result({type = !db.decimal<24, 4>})] (%4: !tuples.tuplestream, %5: !tuples.tuple) {
        %6 = relalg.aggrfn sum @map::@result %4 : !db.decimal<24, 4>
        tuples.return %6 : !db.decimal<24, 4>
      }
    }
  """
}

/** TPC-H Q1 in RelAlg MLIR.
  * GroupBy with multiple aggregations (including computed), sort.
  */
class TPCHQ1RelAlgTest extends RelAlgQueryTest[TPCHDB,
  (l_returnflag: String, l_linestatus: String,
   sum_qty: Double, sum_base_price: Double, sum_disc_price: Double, sum_charge: Double,
   avg_qty: Double, avg_price: Double, avg_disc: Double, count_order: Int)] {
  def testDescription = "TPCH Q1 RelAlg: pricing summary report"
  def tableSchemas = TPCHSchemas.schemas
  override def outputFileName = Some("q1.mlir")
  override def outputColumns = {
    val decimal12 = DecimalType(IntData(12), IntData(2))
    val decimal24 = DecimalType(IntData(24), IntData(4))
    val decimal36 = DecimalType(IntData(36), IntData(6))
    val decimal31 = DecimalType(IntData(31), IntData(21))
    Seq(
      OutputColumn("lineitem", "l_returnflag", "l_returnflag", CharType(IntData(1))),
      OutputColumn("lineitem", "l_linestatus", "l_linestatus", CharType(IntData(1))),
      OutputColumn("aggr", "sum_qty", "sum_qty", decimal12),
      OutputColumn("aggr", "sum_base_price", "sum_base_price", decimal12),
      OutputColumn("aggr", "sum_disc_price", "sum_disc_price", decimal24),
      OutputColumn("aggr", "sum_charge", "sum_charge", decimal36),
      OutputColumn("aggr", "avg_qty", "avg_qty", decimal31),
      OutputColumn("aggr", "avg_price", "avg_price", decimal31),
      OutputColumn("aggr", "avg_disc", "avg_disc", decimal31),
      OutputColumn("aggr", "count_order", "count_order", I64),
    )
  }

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

  def expectedRelAlgPattern = """
    builtin.module {
      %0 = relalg.basetable {table_identifier = "lineitem"} columns: {l_returnflag => @lineitem::@l_returnflag({type = !db.char<1>}), l_linestatus => @lineitem::@l_linestatus({type = !db.char<1>}), l_quantity => @lineitem::@l_quantity({type = !db.decimal<12, 2>}), l_receiptdate => @lineitem::@l_receiptdate({type = !db.date<day>}), l_tax => @lineitem::@l_tax({type = !db.decimal<12, 2>}), l_shipdate => @lineitem::@l_shipdate({type = !db.date<day>}), l_suppkey => @lineitem::@l_suppkey({type = i32}), l_orderkey => @lineitem::@l_orderkey({type = i32}), l_extendedprice => @lineitem::@l_extendedprice({type = !db.decimal<12, 2>}), l_partkey => @lineitem::@l_partkey({type = i32}), l_discount => @lineitem::@l_discount({type = !db.decimal<12, 2>}), l_commitdate => @lineitem::@l_commitdate({type = !db.date<day>})}
      %1 = relalg.selection %0 (%2: !tuples.tuple) {
        %3 = tuples.getcol %2 @lineitem::@l_shipdate : !db.date<day>
        %4 = db.constant("1998-09-02") : !db.date<day>
        %5 = db.compare lte %3:!db.date<day>, %4:!db.date<day>
        tuples.return %5 : i1
      }
      %2 = relalg.map %1 computes : [@map::@sum_disc_price({type = !db.decimal<24, 4>}), @map::@sum_charge({type = !db.decimal<36, 6>})] (%3: !tuples.tuple) {
        %4 = tuples.getcol %3 @lineitem::@l_extendedprice : !db.decimal<12, 2>
        %5 = db.constant("1.0") : !db.decimal<12, 2>
        %6 = tuples.getcol %3 @lineitem::@l_discount : !db.decimal<12, 2>
        %7 = db.sub %5 : !db.decimal<12, 2>, %6 : !db.decimal<12, 2>
        %8 = db.mul %4 : !db.decimal<12, 2>, %7 : !db.decimal<12, 2>
        %9 = tuples.getcol %3 @lineitem::@l_extendedprice : !db.decimal<12, 2>
        %10 = db.constant("1.0") : !db.decimal<12, 2>
        %11 = tuples.getcol %3 @lineitem::@l_discount : !db.decimal<12, 2>
        %12 = db.sub %10 : !db.decimal<12, 2>, %11 : !db.decimal<12, 2>
        %13 = db.mul %9 : !db.decimal<12, 2>, %12 : !db.decimal<12, 2>
        %14 = db.constant("1.0") : !db.decimal<12, 2>
        %15 = tuples.getcol %3 @lineitem::@l_tax : !db.decimal<12, 2>
        %16 = db.add %14 : !db.decimal<12, 2>, %15 : !db.decimal<12, 2>
        %17 = db.mul %13 : !db.decimal<24, 4>, %16 : !db.decimal<12, 2>
        tuples.return %8, %17 : !db.decimal<24, 4>, !db.decimal<36, 6>
      }
      %3 = relalg.aggregation %2 [@lineitem::@l_returnflag, @lineitem::@l_linestatus] computes : [@aggr::@sum_qty({type = !db.decimal<12, 2>}), @aggr::@sum_base_price({type = !db.decimal<12, 2>}), @aggr::@sum_disc_price({type = !db.decimal<24, 4>}), @aggr::@sum_charge({type = !db.decimal<36, 6>}), @aggr::@avg_qty({type = !db.decimal<31, 21>}), @aggr::@avg_price({type = !db.decimal<31, 21>}), @aggr::@avg_disc({type = !db.decimal<31, 21>}), @aggr::@count_order({type = i64})] (%4: !tuples.tuplestream, %5: !tuples.tuple) {
        %6 = relalg.aggrfn sum @lineitem::@l_quantity %4 : !db.decimal<12, 2>
        %7 = relalg.aggrfn sum @lineitem::@l_extendedprice %4 : !db.decimal<12, 2>
        %8 = relalg.aggrfn sum @map::@sum_disc_price %4 : !db.decimal<24, 4>
        %9 = relalg.aggrfn sum @map::@sum_charge %4 : !db.decimal<36, 6>
        %10 = relalg.aggrfn avg @lineitem::@l_quantity %4 : !db.decimal<31, 21>
        %11 = relalg.aggrfn avg @lineitem::@l_extendedprice %4 : !db.decimal<31, 21>
        %12 = relalg.aggrfn avg @lineitem::@l_discount %4 : !db.decimal<31, 21>
        %13 = relalg.count %4
        tuples.return %6, %7, %8, %9, %10, %11, %12, %13 : !db.decimal<12, 2>, !db.decimal<12, 2>, !db.decimal<24, 4>, !db.decimal<36, 6>, !db.decimal<31, 21>, !db.decimal<31, 21>, !db.decimal<31, 21>, i64
      }
      %4 = relalg.sort %3 [(@lineitem::@l_returnflag,asc),(@lineitem::@l_linestatus,asc)]
    }
  """
}

/** TPC-H Q3 in RelAlg MLIR.
  * 3-way join + groupBy + sort + limit.
  */
class TPCHQ3RelAlgTest extends RelAlgQueryTest[TPCHDB,
  (l_orderkey: Int, revenue: Double, o_orderdate: LocalDate, o_shippriority: Int)] {
  def testDescription = "TPCH Q3 RelAlg: shipping priority"
  def tableSchemas = TPCHSchemas.schemas
  override def outputFileName = Some("q3.mlir")
  override def outputColumns = Seq(
    OutputColumn("lineitem", "l_orderkey", "l_orderkey", I32),
    OutputColumn("aggr", "revenue", "revenue", DecimalType(IntData(24), IntData(4))),
    OutputColumn("orders", "o_orderdate", "o_orderdate", DateType(scair.dialects.builtin.StringData("day"))),
    OutputColumn("orders", "o_shippriority", "o_shippriority", I32),
  )

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

  def expectedRelAlgPattern = """
    builtin.module {
      %0 = relalg.basetable {table_identifier = "customer"} columns: {c_acctbal => @customer::@c_acctbal({type = !db.decimal<12, 2>}), c_name => @customer::@c_name({type = !db.string}), c_nationkey => @customer::@c_nationkey({type = i32}), c_custkey => @customer::@c_custkey({type = i32}), c_comment => @customer::@c_comment({type = !db.string}), c_address => @customer::@c_address({type = !db.string}), c_mktsegment => @customer::@c_mktsegment({type = !db.char<10>}), c_phone => @customer::@c_phone({type = !db.char<15>})}
      %1 = relalg.basetable {table_identifier = "orders"} columns: {o_shippriority => @orders::@o_shippriority({type = i32}), o_orderdate => @orders::@o_orderdate({type = !db.date<day>}), o_custkey => @orders::@o_custkey({type = i32}), o_orderpriority => @orders::@o_orderpriority({type = !db.char<15>}), o_totalprice => @orders::@o_totalprice({type = !db.decimal<12, 2>}), o_orderkey => @orders::@o_orderkey({type = i32})}
      %2 = relalg.basetable {table_identifier = "lineitem"} columns: {l_returnflag => @lineitem::@l_returnflag({type = !db.char<1>}), l_linestatus => @lineitem::@l_linestatus({type = !db.char<1>}), l_quantity => @lineitem::@l_quantity({type = !db.decimal<12, 2>}), l_receiptdate => @lineitem::@l_receiptdate({type = !db.date<day>}), l_tax => @lineitem::@l_tax({type = !db.decimal<12, 2>}), l_shipdate => @lineitem::@l_shipdate({type = !db.date<day>}), l_suppkey => @lineitem::@l_suppkey({type = i32}), l_orderkey => @lineitem::@l_orderkey({type = i32}), l_extendedprice => @lineitem::@l_extendedprice({type = !db.decimal<12, 2>}), l_partkey => @lineitem::@l_partkey({type = i32}), l_discount => @lineitem::@l_discount({type = !db.decimal<12, 2>}), l_commitdate => @lineitem::@l_commitdate({type = !db.date<day>})}
      %3 = relalg.crossproduct %0, %1
      %4 = relalg.crossproduct %3, %2
      %5 = relalg.selection %4 (%6: !tuples.tuple) {
        %7 = tuples.getcol %6 @customer::@c_mktsegment : !db.char<10>
        %8 = db.constant("BUILDING") : !db.string
        %9 = db.cast %7 : !db.char<10> -> !db.string
        %10 = db.compare eq %9:!db.string, %8:!db.string
        %11 = tuples.getcol %6 @customer::@c_custkey : i32
        %12 = tuples.getcol %6 @orders::@o_custkey : i32
        %13 = db.compare eq %11:i32, %12:i32
        %14 = tuples.getcol %6 @orders::@o_orderdate : !db.date<day>
        %15 = db.constant("1995-03-15") : !db.date<day>
        %16 = db.compare lt %14:!db.date<day>, %15:!db.date<day>
        %17 = tuples.getcol %6 @lineitem::@l_orderkey : i32
        %18 = tuples.getcol %6 @orders::@o_orderkey : i32
        %19 = db.compare eq %17:i32, %18:i32
        %20 = tuples.getcol %6 @lineitem::@l_shipdate : !db.date<day>
        %21 = db.constant("1995-03-15") : !db.date<day>
        %22 = db.compare gt %20:!db.date<day>, %21:!db.date<day>
        %23 = db.and %10,%13,%16,%19,%22:i1,i1,i1,i1,i1
        tuples.return %23 : i1
      }
      %6 = relalg.map %5 computes : [@map::@revenue({type = !db.decimal<24, 4>})] (%7: !tuples.tuple) {
        %8 = tuples.getcol %7 @lineitem::@l_extendedprice : !db.decimal<12, 2>
        %9 = db.constant("1.0") : !db.decimal<12, 2>
        %10 = tuples.getcol %7 @lineitem::@l_discount : !db.decimal<12, 2>
        %11 = db.sub %9 : !db.decimal<12, 2>, %10 : !db.decimal<12, 2>
        %12 = db.mul %8 : !db.decimal<12, 2>, %11 : !db.decimal<12, 2>
        tuples.return %12 : !db.decimal<24, 4>
      }
      %7 = relalg.aggregation %6 [@lineitem::@l_orderkey, @orders::@o_orderdate, @orders::@o_shippriority] computes : [@aggr::@revenue({type = !db.decimal<24, 4>})] (%8: !tuples.tuplestream, %9: !tuples.tuple) {
        %10 = relalg.aggrfn sum @map::@revenue %8 : !db.decimal<24, 4>
        tuples.return %10 : !db.decimal<24, 4>
      }
      %8 = relalg.sort %7 [(@aggr::@revenue,desc),(@orders::@o_orderdate,asc)]
      %9 = relalg.limit 10 %8
    }
  """
}

/** TPC-H Q10 in RelAlg MLIR.
  * 4-way join + groupBy + sort + limit.
  */
class TPCHQ10RelAlgTest extends RelAlgQueryTest[TPCHDB,
  (c_custkey: Int, c_name: String, revenue: Double, c_acctbal: Double,
   n_name: String, c_address: String, c_phone: String, c_comment: String)] {
  def testDescription = "TPCH Q10 RelAlg: returned item reporting"
  def tableSchemas = TPCHSchemas.schemas
  override def outputFileName = Some("q10.mlir")
  override def outputColumns = Seq(
    OutputColumn("customer", "c_custkey", "c_custkey", I32),
    OutputColumn("customer", "c_name", "c_name", DBStringType()),
    OutputColumn("aggr", "revenue", "revenue", DecimalType(IntData(24), IntData(4))),
    OutputColumn("customer", "c_acctbal", "c_acctbal", DecimalType(IntData(12), IntData(2))),
    OutputColumn("nation", "n_name", "n_name", CharType(IntData(25))),
    OutputColumn("customer", "c_address", "c_address", DBStringType()),
    OutputColumn("customer", "c_phone", "c_phone", CharType(IntData(15))),
    OutputColumn("customer", "c_comment", "c_comment", DBStringType()),
  )

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

  def expectedRelAlgPattern = """
    builtin.module {
      %0 = relalg.basetable {table_identifier = "customer"} columns: {c_acctbal => @customer::@c_acctbal({type = !db.decimal<12, 2>}), c_name => @customer::@c_name({type = !db.string}), c_nationkey => @customer::@c_nationkey({type = i32}), c_custkey => @customer::@c_custkey({type = i32}), c_comment => @customer::@c_comment({type = !db.string}), c_address => @customer::@c_address({type = !db.string}), c_mktsegment => @customer::@c_mktsegment({type = !db.char<10>}), c_phone => @customer::@c_phone({type = !db.char<15>})}
      %1 = relalg.basetable {table_identifier = "orders"} columns: {o_shippriority => @orders::@o_shippriority({type = i32}), o_orderdate => @orders::@o_orderdate({type = !db.date<day>}), o_custkey => @orders::@o_custkey({type = i32}), o_orderpriority => @orders::@o_orderpriority({type = !db.char<15>}), o_totalprice => @orders::@o_totalprice({type = !db.decimal<12, 2>}), o_orderkey => @orders::@o_orderkey({type = i32})}
      %2 = relalg.basetable {table_identifier = "lineitem"} columns: {l_returnflag => @lineitem::@l_returnflag({type = !db.char<1>}), l_linestatus => @lineitem::@l_linestatus({type = !db.char<1>}), l_quantity => @lineitem::@l_quantity({type = !db.decimal<12, 2>}), l_receiptdate => @lineitem::@l_receiptdate({type = !db.date<day>}), l_tax => @lineitem::@l_tax({type = !db.decimal<12, 2>}), l_shipdate => @lineitem::@l_shipdate({type = !db.date<day>}), l_suppkey => @lineitem::@l_suppkey({type = i32}), l_orderkey => @lineitem::@l_orderkey({type = i32}), l_extendedprice => @lineitem::@l_extendedprice({type = !db.decimal<12, 2>}), l_partkey => @lineitem::@l_partkey({type = i32}), l_discount => @lineitem::@l_discount({type = !db.decimal<12, 2>}), l_commitdate => @lineitem::@l_commitdate({type = !db.date<day>})}
      %3 = relalg.basetable {table_identifier = "nation"} columns: {n_nationkey => @nation::@n_nationkey({type = i32}), n_name => @nation::@n_name({type = !db.char<25>}), n_regionkey => @nation::@n_regionkey({type = i32})}
      %4 = relalg.crossproduct %0, %1
      %5 = relalg.crossproduct %4, %2
      %6 = relalg.crossproduct %5, %3
      %7 = relalg.selection %6 (%8: !tuples.tuple) {
        %9 = tuples.getcol %8 @customer::@c_custkey : i32
        %10 = tuples.getcol %8 @orders::@o_custkey : i32
        %11 = db.compare eq %9:i32, %10:i32
        %12 = tuples.getcol %8 @orders::@o_orderdate : !db.date<day>
        %13 = db.constant("1993-10-01") : !db.date<day>
        %14 = db.compare gte %12:!db.date<day>, %13:!db.date<day>
        %15 = tuples.getcol %8 @orders::@o_orderdate : !db.date<day>
        %16 = db.constant("1994-01-01") : !db.date<day>
        %17 = db.compare lt %15:!db.date<day>, %16:!db.date<day>
        %18 = tuples.getcol %8 @lineitem::@l_orderkey : i32
        %19 = tuples.getcol %8 @orders::@o_orderkey : i32
        %20 = db.compare eq %18:i32, %19:i32
        %21 = tuples.getcol %8 @lineitem::@l_returnflag : !db.char<1>
        %22 = db.constant("R") : !db.string
        %23 = db.cast %21 : !db.char<1> -> !db.string
        %24 = db.compare eq %23:!db.string, %22:!db.string
        %25 = tuples.getcol %8 @customer::@c_nationkey : i32
        %26 = tuples.getcol %8 @nation::@n_nationkey : i32
        %27 = db.compare eq %25:i32, %26:i32
        %28 = db.and %11,%14,%17,%20,%24,%27:i1,i1,i1,i1,i1,i1
        tuples.return %28 : i1
      }
      %8 = relalg.map %7 computes : [@map::@revenue({type = !db.decimal<24, 4>})] (%9: !tuples.tuple) {
        %10 = tuples.getcol %9 @lineitem::@l_extendedprice : !db.decimal<12, 2>
        %11 = db.constant("1.0") : !db.decimal<12, 2>
        %12 = tuples.getcol %9 @lineitem::@l_discount : !db.decimal<12, 2>
        %13 = db.sub %11 : !db.decimal<12, 2>, %12 : !db.decimal<12, 2>
        %14 = db.mul %10 : !db.decimal<12, 2>, %13 : !db.decimal<12, 2>
        tuples.return %14 : !db.decimal<24, 4>
      }
      %9 = relalg.aggregation %8 [@customer::@c_custkey, @customer::@c_name, @customer::@c_acctbal, @customer::@c_phone, @nation::@n_name, @customer::@c_address, @customer::@c_comment] computes : [@aggr::@revenue({type = !db.decimal<24, 4>})] (%10: !tuples.tuplestream, %11: !tuples.tuple) {
        %12 = relalg.aggrfn sum @map::@revenue %10 : !db.decimal<24, 4>
        tuples.return %12 : !db.decimal<24, 4>
      }
      %10 = relalg.sort %9 [(@aggr::@revenue,desc)]
      %11 = relalg.limit 20 %10
    }
  """
}
