package tyql

import java.sql.{Connection, ResultSet, DriverManager}
import scala.NamedTuple.NamedTuple
import pprint.pprintln
import scala.deriving.Mirror
import scala.Tuple

class DB(conn: Connection) {
  def run[T](dbast: DatabaseAST[T])(using resultTag: ResultTag[T],
                                          dialect: tyql.Dialect,
                                          config: tyql.Config): List[T] = {
    val sqlString = dbast.toQueryIR.toSQLString()
    println("SQL << " + sqlString + " >>")
    val stmt = conn.createStatement()
    val rs = stmt.executeQuery(sqlString)
    val metadata = rs.getMetaData()
    val columnCount = metadata.getColumnCount()
    var results = List[T]()
    while (rs.next()) {
      val row = resultTag match
        case ResultTag.IntTag => rs.getInt(1)
        case ResultTag.DoubleTag => rs.getDouble(1)
        case ResultTag.StringTag => rs.getString(1)
        case ResultTag.BoolTag => rs.getBoolean(1)
        case ResultTag.OptionalTag(e) => {
          val got = rs.getObject(1)
          if got == null then None
          else e match
            case ResultTag.IntTag => Some(got.asInstanceOf[Int])
            case ResultTag.DoubleTag => Some(got.asInstanceOf[Double])
            case ResultTag.StringTag => Some(got.asInstanceOf[String])
            case ResultTag.BoolTag => Some(got.asInstanceOf[Boolean])
            case _ => assert(false, "Unsupported type")
        }
        case ResultTag.ProductTag(_, fields, m) => {
          val nt = fields.asInstanceOf[ResultTag.NamedTupleTag[?,?]]
          val fieldValues = nt.names.zip(nt.types).zipWithIndex.map { case ((name, tag), idx) =>
            val col = idx + 1 // XXX if you want to use `name` here, you must case-convert it
            tag match
              case ResultTag.IntTag => rs.getInt(col)
              case ResultTag.DoubleTag => rs.getDouble(col)
              case ResultTag.StringTag => rs.getString(col)
              case ResultTag.BoolTag => rs.getBoolean(col)
              case ResultTag.OptionalTag(e) => {
                val got = rs.getObject(col)
                if got == null then None
                else e match
                  case ResultTag.IntTag => Some(got.asInstanceOf[Int])
                  case ResultTag.DoubleTag => Some(got.asInstanceOf[Double])
                  case ResultTag.StringTag => Some(got.asInstanceOf[String])
                  case ResultTag.BoolTag => Some(got.asInstanceOf[Boolean])
                  case _ => assert(false, "Unsupported type")
              }
              case _ => assert(false, "Unsupported type")
          }
          m.fromProduct(Tuple.fromArray(fieldValues.toArray))
        }
        case _ => assert(false, "Unsupported type")
      results = row.asInstanceOf[T] :: results
    }
    rs.close()
    stmt.close()
    results.reverse
  }

  def runDebug[T](dbast: DatabaseAST[T])(using resultTag: ResultTag[T],
                                               dialect: tyql.Dialect,
                                               config: tyql.Config): List[Map[String, Any]] = {
    val sqlString = dbast.toQueryIR.toSQLString()
    println("SQL << " + sqlString + " >>")
    val stmt = conn.createStatement()
    val rs = stmt.executeQuery(sqlString)
    val metadata = rs.getMetaData()
    val columnCount = metadata.getColumnCount()
    var results = List[Map[String, Any]]()
    while (rs.next()) {
      val row = (1 to columnCount).map { i =>
        metadata.getColumnName(i) -> rs.getObject(i)
      }.toMap
      results = row :: results
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
  given tyql.Config = new tyql.Config(tyql.CaseConvention.Underscores) {}
  case class Flowers(name: Option[String], flowerSize: Int, cost: Option[Double], likes: Int)
  val t = new tyql.Table[Flowers]("flowers")

  println("------------1------------")
  val zzz = db.run(t.filter(t => t.flowerSize.isNull || t.flowerSize >= 2))
  println("received:")
  pprintln(zzz)
  println("likes are " + zzz.head.likes.toString())

  println("------------2------------")
  val zzz2 = db.run(t.map(_.name.getOrElse("UNKNOWN")))
  println("received:")
  pprintln(zzz2)

  println("------------3------------")
  val zzz3 = db.run(t.max(_.flowerSize))
  println("received:")
  pprintln(zzz3)
}
