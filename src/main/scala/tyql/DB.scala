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
            case null                        => ps.setNull(i + 1, java.sql.Types.NULL)
            case v if v.isInstanceOf[Long]   => ps.setLong(i + 1, v.asInstanceOf[Long])
            case v if v.isInstanceOf[Int]    => ps.setInt(i + 1, v.asInstanceOf[Int])
            case v if v.isInstanceOf[Double] => ps.setDouble(i + 1, v.asInstanceOf[Double])
            case v if v.isInstanceOf[String] => ps.setString(i + 1, v.asInstanceOf[String])
            case v                           => ps.setObject(i + 1, v)
        rs = ps.executeQuery()
      case tyql.ParameterStyle.EscapedInline =>
        rs = stmt.executeQuery(sqlString)
    val metadata = rs.getMetaData() // _.getColumnName()
    val columnCount = metadata.getColumnCount()
    var results = List[T]()
    while (rs.next()) {
      val row = resultTag match
        case ResultTag.IntTag    => rs.getInt(1)
        case ResultTag.DoubleTag => rs.getDouble(1)
        case ResultTag.StringTag => rs.getString(1)
        case ResultTag.BoolTag   => rs.getBoolean(1)
        case ResultTag.OptionalTag(e) => {
          val got = rs.getObject(1)
          if got == null then None
          else
            e match
              case ResultTag.IntTag    => Some(got.asInstanceOf[Int])
              case ResultTag.DoubleTag => Some(got.asInstanceOf[Double])
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
              case ResultTag.DoubleTag => rs.getDouble(col)
              case ResultTag.StringTag => rs.getString(col)
              case ResultTag.BoolTag   => rs.getBoolean(col)
              case ResultTag.OptionalTag(e) => {
                val got = rs.getObject(col)
                if got == null then None
                else
                  e match
                    case ResultTag.IntTag    => Some(got.asInstanceOf[Int])
                    case ResultTag.DoubleTag => Some(got.asInstanceOf[Double])
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
              case ResultTag.DoubleTag => rs.getDouble(col)
              case ResultTag.StringTag => rs.getString(col)
              case ResultTag.BoolTag   => rs.getBoolean(col)
              case ResultTag.OptionalTag(e) => {
                val got = rs.getObject(col)
                if got == null then None
                else
                  e match
                    case ResultTag.IntTag    => Some(got.asInstanceOf[Int])
                    case ResultTag.DoubleTag => Some(got.asInstanceOf[Double])
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
  given tyql.Config = new tyql.Config(tyql.CaseConvention.Underscores, tyql.ParameterStyle.EscapedInline) {}
  import tyql.Dialect.mariadb.given
  case class Flowers(name: Option[String], flowerSize: Int, cost: Option[Double], likes: Int)
  val t = tyql.Table[Flowers]()

  db.runRaw("create table if not exists flowers(name text, flower_size integer, cost double, likes integer);")
  db.runRaw("create table if not exists customers(cust_id integer primary key, cust_name varchar(255));")
  db.runRaw("create table if not exists orders(order_id integer primary key, cust_id integer, prod varchar(255));")

  case class Customers(custId: Int, custName: String)
  case class Orders(orderId: Int, custId: Int, prod: String)
  val customers = tyql.Table[Customers]()
  val orders = tyql.Table[Orders]()


  val inlineValues = tyql.Values(
    (a = 1, b = 2.0), (a = 10, b = 12.0)
    )
  println(db.run(
    inlineValues.map(x => (a = x.a + 1, b = x.b + 1.0))
  ))

  pprintln(db.run(
    t.filter(x => x.cost.get.between(lit(11.0), lit(19.0)))
  ))
}
