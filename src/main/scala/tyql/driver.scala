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
    println("SQL STRING WAS " + sqlString)
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
        case ResultTag.ProductTag(_, fields, m) => {
          val kkk = fields.asInstanceOf[ResultTag.NamedTupleTag[?,?]]
          val fieldValues = kkk.names.zip(kkk.types).map { (name, tag) =>
            val casedName = config.caseConvention.convert(name)
            tag match
              case ResultTag.IntTag => rs.getInt(casedName)
              case ResultTag.DoubleTag => rs.getDouble(casedName)
              case ResultTag.StringTag => rs.getString(casedName)
              case ResultTag.BoolTag => rs.getBoolean(casedName)
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
    println("SQL STRING WAS " + sqlString)
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
  case class Flowers(name: String, flowerSize: Int, cost: Double, likes: Int)
  val t = new tyql.Table[Flowers]("flowers")

  println("------------1------------")
  val zzz = db.run(t.filter(t => t.flowerSize >= 10))
  println("received:")
  pprintln(zzz)
  println("likes are " + zzz.head.likes.toString())

  println("------------2------------")
  val zzz2 = db.run(t.map(_.name)).map(_ + "!!!") // TODO if this map is inside then we generate incorrect SQL due to incorrect subquery field naming :(
  println("received:")
  pprintln(zzz2)

  println("------------3------------")
  val zzz3 = db.run(t.max(_.flowerSize))
  println("received:")
  pprintln(zzz3)
}
