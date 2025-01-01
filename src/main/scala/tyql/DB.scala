package tyql

import java.sql.{Connection, ResultSet, DriverManager}
import scala.NamedTuple.NamedTuple
import pprint.pprintln
import scala.deriving.Mirror
import scala.Tuple

import pprint.pprintln
import TreePrettyPrinter._

import language.experimental.namedTuples
import NamedTuple.*
import scala.language.implicitConversions
import tyql._
import tyql.Expr._

class DB(conn: Connection) {

  def runRaw(rawSql: String): Unit = {
    val statement = conn.createStatement()
    statement.execute(rawSql)
    statement.close()
  }

  def run(dbast: UpdateToTheDB)(using dialect: tyql.Dialect, config: tyql.Config): Unit =
    val (sqlString, parameters) = dbast.toQueryIR.toSQLQuery()
    println("SQL << " + sqlString + " >>")
    for (p <- parameters) {
      println("Param << " + p + " >>")
    }
    val stmt = conn.createStatement()
    var returnedInt = 0
    config.parameterStyle match
      case tyql.ParameterStyle.DriverParametrized =>
        val ps = conn.prepareStatement(sqlString)
        for (i <- 0 until parameters.length) do
          parameters(i) match
            case null                         => ps.setNull(i + 1, java.sql.Types.NULL)
            case v if v.isInstanceOf[Long]    => ps.setLong(i + 1, v.asInstanceOf[Long])
            case v if v.isInstanceOf[Int]     => ps.setInt(i + 1, v.asInstanceOf[Int])
            case v if v.isInstanceOf[Double]  => ps.setDouble(i + 1, v.asInstanceOf[Double])
            case v if v.isInstanceOf[Float]   => ps.setFloat(i + 1, v.asInstanceOf[Float])
            case v if v.isInstanceOf[Boolean] => ps.setBoolean(i + 1, v.asInstanceOf[Boolean])
            case v if v.isInstanceOf[String]  => ps.setString(i + 1, v.asInstanceOf[String])
            case v                            => ps.setObject(i + 1, v)
        returnedInt = ps.executeUpdate()
      case tyql.ParameterStyle.EscapedInline =>
        returnedInt = stmt.executeUpdate(sqlString)
    println("returned " + returnedInt)
    stmt.close()

  def run[T]
    (dbast: DatabaseAST[T])
    (using resultTag: ResultTag[T], dialect: tyql.Dialect, config: tyql.Config)
    : List[T] = {
    val (sqlString, parameters) = dbast.toQueryIR.toSQLQuery()
    println("SQL << " + sqlString + " >>")
    for (p <- parameters) {
      println("Param << " + p + " >>")
    }
    val stmt = conn.createStatement()
    var rs: java.sql.ResultSet = null
    config.parameterStyle match
      case tyql.ParameterStyle.DriverParametrized =>
        val ps = conn.prepareStatement(sqlString)
        for (i <- 0 until parameters.length) do
          parameters(i) match
            case null                         => ps.setNull(i + 1, java.sql.Types.NULL)
            case v if v.isInstanceOf[Long]    => ps.setLong(i + 1, v.asInstanceOf[Long])
            case v if v.isInstanceOf[Int]     => ps.setInt(i + 1, v.asInstanceOf[Int])
            case v if v.isInstanceOf[Double]  => ps.setDouble(i + 1, v.asInstanceOf[Double])
            case v if v.isInstanceOf[Float]   => ps.setFloat(i + 1, v.asInstanceOf[Float])
            case v if v.isInstanceOf[Boolean] => ps.setBoolean(i + 1, v.asInstanceOf[Boolean])
            case v if v.isInstanceOf[String]  => ps.setString(i + 1, v.asInstanceOf[String])
            case v                            => ps.setObject(i + 1, v)
        rs = ps.executeQuery()
      case tyql.ParameterStyle.EscapedInline =>
        rs = stmt.executeQuery(sqlString)
    val metadata = rs.getMetaData() // _.getColumnName()
    val columnCount = metadata.getColumnCount()
    var results = List[T]()
    while (rs.next()) {
      val row = resultTag match
        case ResultTag.IntTag    => rs.getInt(1)
        case ResultTag.LongTag   => rs.getLong(1)
        case ResultTag.DoubleTag => rs.getDouble(1)
        case ResultTag.FloatTag  => rs.getFloat(1)
        case ResultTag.StringTag => rs.getString(1)
        case ResultTag.BoolTag   => rs.getBoolean(1)
        case ResultTag.OptionalTag(e) => {
          val got = rs.getObject(1)
          if got == null then None
          else
            e match
              case ResultTag.IntTag    => Some(got.asInstanceOf[Int])
              case ResultTag.LongTag   => Some(got.asInstanceOf[Long])
              case ResultTag.DoubleTag => Some(got.asInstanceOf[Double])
              case ResultTag.FloatTag  => Some(got.asInstanceOf[Float])
              case ResultTag.StringTag => Some(got.asInstanceOf[String])
              case ResultTag.BoolTag   => Some(got.asInstanceOf[Boolean])
              case _                   => assert(false, "Unsupported type")
        }
        case ResultTag.ProductTag(_, fields, m) => {
          val nt = fields.asInstanceOf[ResultTag.NamedTupleTag[?, ?]]
          val fieldValues = nt.names.zip(nt.types).zipWithIndex.map { case ((name, tag), idx) =>
            val col = idx + 1 // XXX if you want to use `name` here, you must case-convert it
            tag match
              case ResultTag.IntTag    => rs.getInt(col)
              case ResultTag.LongTag   => rs.getLong(col)
              case ResultTag.DoubleTag => rs.getDouble(col)
              case ResultTag.FloatTag  => rs.getFloat(col)
              case ResultTag.StringTag => rs.getString(col)
              case ResultTag.BoolTag   => rs.getBoolean(col)
              case ResultTag.OptionalTag(e) => {
                val got = rs.getObject(col)
                if got == null then None
                else
                  e match
                    case ResultTag.IntTag    => Some(got.asInstanceOf[Int])
                    case ResultTag.LongTag   => Some(got.asInstanceOf[Long])
                    case ResultTag.DoubleTag => Some(got.asInstanceOf[Double])
                    case ResultTag.FloatTag  => Some(got.asInstanceOf[Float])
                    case ResultTag.StringTag => Some(got.asInstanceOf[String])
                    case ResultTag.BoolTag   => Some(got.asInstanceOf[Boolean])
                    case _                   => assert(false, "Unsupported type")
              }
              case _ => assert(false, "Unsupported type")
          }
          m.fromProduct(Tuple.fromArray(fieldValues.toArray))
        }
        case ResultTag.NamedTupleTag(names, typesResultTags) =>
          val fieldValues = names.zip(typesResultTags).zipWithIndex.map { case ((name, tag), idx) =>
            val col = idx + 1 // XXX if you want to use `name` here, you must case-convert it
            tag match
              case ResultTag.IntTag    => rs.getInt(col)
              case ResultTag.LongTag   => rs.getLong(col)
              case ResultTag.DoubleTag => rs.getDouble(col)
              case ResultTag.FloatTag  => rs.getFloat(col)
              case ResultTag.StringTag => rs.getString(col)
              case ResultTag.BoolTag   => rs.getBoolean(col)
              case ResultTag.OptionalTag(e) => {
                val got = rs.getObject(col)
                if got == null then None
                else
                  e match
                    case ResultTag.IntTag    => Some(got.asInstanceOf[Int])
                    case ResultTag.LongTag   => Some(got.asInstanceOf[Long])
                    case ResultTag.DoubleTag => Some(got.asInstanceOf[Double])
                    case ResultTag.FloatTag  => Some(got.asInstanceOf[Float])
                    case ResultTag.StringTag => Some(got.asInstanceOf[String])
                    case ResultTag.BoolTag   => Some(got.asInstanceOf[Boolean])
                    case _                   => assert(false, "Unsupported type")
              }
              case _ => assert(false, "Unsupported type")
          }
          Tuple.fromArray(fieldValues.toArray)
        case _ => assert(false, "Unsupported type")
      results = row.asInstanceOf[T] :: results
    }
    rs.close()
    stmt.close()
    results.reverse
  }
}

def driverMain(): Unit = {
  import scala.language.implicitConversions
  val conn = DriverManager.getConnection("jdbc:mariadb://localhost:3308/testdb", "testuser", "testpass")
  val db = DB(conn)
  given tyql.Config = new tyql.Config(tyql.CaseConvention.Underscores, tyql.ParameterStyle.DriverParametrized) {}
  import tyql.Dialect.mariadb.given
  case class Flowers(name: Option[String], flowerSize: Int, cost: Double, likes: Int)
  val t = tyql.Table[Flowers]()

  // type flowernames = t.ColumnNames
  // val nnames = scala.compiletime.constValueTuple[flowernames]
  // println("names are " + nnames)
  // type flowertypes = t.Types
  // val flowerTypeExprs : flowertypes = (Some("a"), 1, Some(1.0), -1)
  // println("typevalues are " + flowerTypeExprs)

  // db.runRaw("create table if not exists flowers(name text, flower_size integer, cost double, likes integer);")
  // db.runRaw("create table if not exists customers(cust_id integer primary key, cust_name varchar(255));")
  // db.runRaw("create table if not exists orders(order_id integer primary key, cust_id integer, prod varchar(255));")

  // case class Customers(custId: Int, custName: String)
  // case class Orders(orderId: Int, custId: Int, prod: String)
  // val customers = tyql.Table[Customers]()
  // val orders = tyql.Table[Orders]()

  // val someTable = Values[(a: Int)](Tuple(1), Tuple(2))
  // pprintln(db.run(
  //   t.filter(x =>
  //     someTable.containsRow((a = lit(1)).toRow)
  //   )
  // ))

  // pprintln(db.run(
  //   t.filter(x =>
  //     someTable.containsRow((a = lit(101)).toRow)
  //   )
  // ))

  // pprintln(db.run(
  //   t.filter(x =>
  //     someTable.map(_.a).contains(1)
  //   )
  // ))

  // pprintln(db.run(
  //   t.filter(x =>
  //     someTable.map(_.a).contains(101)
  //   )
  // ))

  // pprintln(db.run(t.partial[("cost", "flowerSize")].insert(
  //   (cost = 12.0, flowerSize = 1),
  //   (cost = 12.0, flowerSize = 1),
  //   (cost = 12.0, flowerSize = 1)
  // )))

  // pprintln(db.run(t.partial[("cost", "flowerSize")].insert(
  //   (flowerSize = lit(1), cost = lit(12.0) + lit(10.2))
  // )))

  // pprintln(db.run(
  //   t.partial[Tuple1["cost"]].insert(
  //     Tuple(12.0),
  //     Tuple(12.0),
  //     Tuple(12.0)
  // )))

  // pprintln(db.run(
  //   t.insert(
  //     Flowers(Some("rose"), 1, 1.0, 1),
  //     Flowers(Some("rose2"), 1, 1.0, 2),
  //     Flowers(Some("rose3"), 1, 1.0, 3),
  // )))

  // pprintln(db.run(
  //   t.map(f => (cost = lit(100.12), flowerSize = lit(10) ))
  // ))

  val qq = t.map(f => (cost = lit(12.125), flowerSize = lit(-1)))
  val q = qq.insertInto(t.partial[("flowerSize", "cost")])
  pprintln(db.run(q))
}
