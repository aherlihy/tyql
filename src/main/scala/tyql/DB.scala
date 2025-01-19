package tyql

import java.sql.{Connection, ResultSet, DriverManager}
import java.time.{LocalDate, LocalDateTime}
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

  def runUpdate(dbast: UpdateToTheDB)(using dialect: tyql.Dialect, config: tyql.Config): Unit =
    val (sqlString, parameters) = dbast.toQueryIR.toSQLQuery()
    // println("SQL << " + sqlString + " >>")
    // for (p <- parameters) {
    // println("Param << " + p + " >>")
    // }
    val stmt = conn.createStatement()
    var returnedInt = 0
    config.parameterStyle match
      case tyql.ParameterStyle.DriverParametrized =>
        val ps = conn.prepareStatement(sqlString)
        for (i <- 0 until parameters.length) do
          parameters(i) match
            case null                             => ps.setNull(i + 1, java.sql.Types.NULL)
            case v if v.isInstanceOf[Long]        => ps.setLong(i + 1, v.asInstanceOf[Long])
            case v if v.isInstanceOf[Int]         => ps.setInt(i + 1, v.asInstanceOf[Int])
            case v if v.isInstanceOf[Double]      => ps.setDouble(i + 1, v.asInstanceOf[Double])
            case v if v.isInstanceOf[Float]       => ps.setFloat(i + 1, v.asInstanceOf[Float])
            case v if v.isInstanceOf[Boolean]     => ps.setBoolean(i + 1, v.asInstanceOf[Boolean])
            case v if v.isInstanceOf[String]      => ps.setString(i + 1, v.asInstanceOf[String])
            case v if v.isInstanceOf[Array[Byte]] => ps.setBytes(i + 1, v.asInstanceOf[Array[Byte]])
            case v if v.isInstanceOf[LocalDate]   => ps.setDate(i + 1, java.sql.Date.valueOf(v.asInstanceOf[LocalDate]))
            case v if v.isInstanceOf[LocalDateTime] =>
              ps.setTimestamp(i + 1, java.sql.Timestamp.valueOf(v.asInstanceOf[LocalDateTime]))
            case v if v.isInstanceOf[Function0[?]] =>
              ps.setBinaryStream(i + 1, v.asInstanceOf[() => java.io.InputStream]())
            case v => ps.setObject(i + 1, v)
        returnedInt = ps.executeUpdate()
      case tyql.ParameterStyle.EscapedInline =>
        returnedInt = stmt.executeUpdate(sqlString)
    // println("returned " + returnedInt)
    stmt.close()

  inline def run(dbast: UpdateToTheDB)(using dialect: tyql.Dialect, config: tyql.Config): Nothing =
    scala.compiletime.error("Update queries do not return results")

  def run[T]
    (dbast: DatabaseAST[T])
    (using resultTag: ResultTag[T], dialect: tyql.Dialect, config: tyql.Config)
    : List[T] = {
    val (sqlString, parameters) = dbast.toQueryIR.toSQLQuery()
    // println("SQL << " + sqlString + " >>")
    // for (p <- parameters) {
    //   println("Param << " + p + " >>")
    // }
    val stmt = conn.createStatement()
    var rs: java.sql.ResultSet = null
    config.parameterStyle match
      case tyql.ParameterStyle.DriverParametrized =>
        val ps = conn.prepareStatement(sqlString)
        for (i <- 0 until parameters.length) do
          parameters(i) match
            case null                             => ps.setNull(i + 1, java.sql.Types.NULL)
            case v if v.isInstanceOf[Long]        => ps.setLong(i + 1, v.asInstanceOf[Long])
            case v if v.isInstanceOf[Int]         => ps.setInt(i + 1, v.asInstanceOf[Int])
            case v if v.isInstanceOf[Double]      => ps.setDouble(i + 1, v.asInstanceOf[Double])
            case v if v.isInstanceOf[Float]       => ps.setFloat(i + 1, v.asInstanceOf[Float])
            case v if v.isInstanceOf[Boolean]     => ps.setBoolean(i + 1, v.asInstanceOf[Boolean])
            case v if v.isInstanceOf[String]      => ps.setString(i + 1, v.asInstanceOf[String])
            case v if v.isInstanceOf[Array[Byte]] => ps.setBytes(i + 1, v.asInstanceOf[Array[Byte]])
            case v if v.isInstanceOf[LocalDate]   => ps.setDate(i + 1, java.sql.Date.valueOf(v.asInstanceOf[LocalDate]))
            case v if v.isInstanceOf[LocalDateTime] =>
              ps.setTimestamp(i + 1, java.sql.Timestamp.valueOf(v.asInstanceOf[LocalDateTime]))
            case v if v.isInstanceOf[Function0[?]] =>
              ps.setBinaryStream(i + 1, v.asInstanceOf[() => java.io.InputStream]())
            case v if v.isInstanceOf[Expr.VariableInput[?]] =>
              val objNow = v.asInstanceOf[Expr.VariableInput[?]].$thunk()
              if objNow == null then ps.setNull(i + 1, java.sql.Types.NULL)
              else
                objNow match
                  case v if v.isInstanceOf[Long]    => ps.setLong(i + 1, v.asInstanceOf[Long])
                  case v if v.isInstanceOf[Int]     => ps.setInt(i + 1, v.asInstanceOf[Int])
                  case v if v.isInstanceOf[Double]  => ps.setDouble(i + 1, v.asInstanceOf[Double])
                  case v if v.isInstanceOf[Float]   => ps.setFloat(i + 1, v.asInstanceOf[Float])
                  case v if v.isInstanceOf[Boolean] => ps.setBoolean(i + 1, v.asInstanceOf[Boolean])
                  case v if v.isInstanceOf[String]  => ps.setString(i + 1, v.asInstanceOf[String])
                  case v if v.isInstanceOf[LocalDate] =>
                    ps.setDate(i + 1, java.sql.Date.valueOf(v.asInstanceOf[LocalDate]))
                  case v if v.isInstanceOf[LocalDateTime] =>
                    ps.setTimestamp(i + 1, java.sql.Timestamp.valueOf(v.asInstanceOf[LocalDateTime]))
                  case _ => assert(false, "Unexpected type out of the VariableInput's thunk.")
            case v => ps.setObject(i + 1, v)
        rs = ps.executeQuery()
      case tyql.ParameterStyle.EscapedInline =>
        rs = stmt.executeQuery(sqlString)
    val metadata = rs.getMetaData() // _.getColumnName()
    val columnCount = metadata.getColumnCount()
    var results = List[T]()
    while (rs.next()) {
      val row = resultTag match
        case ResultTag.IntTag       => rs.getInt(1)
        case ResultTag.LongTag      => rs.getLong(1)
        case ResultTag.DoubleTag    => rs.getDouble(1)
        case ResultTag.FloatTag     => rs.getFloat(1)
        case ResultTag.StringTag    => rs.getString(1)
        case ResultTag.BoolTag      => rs.getBoolean(1)
        case ResultTag.ByteArrayTag => rs.getBytes(1)
        case ResultTag.ByteStreamTag =>
          val rememberedStream = rs.getBinaryStream(1)
          () => rememberedStream
        case ResultTag.LocalDateTag     => rs.getDate(1).toLocalDate
        case ResultTag.LocalDateTimeTag => rs.getTimestamp(1).toLocalDateTime
        case ResultTag.OptionalTag(e) => {
          val got = rs.getObject(1)
          if got == null then None
          else
            e match
              case ResultTag.IntTag       => Some(got.asInstanceOf[Int])
              case ResultTag.LongTag      => Some(got.asInstanceOf[Long])
              case ResultTag.DoubleTag    => Some(got.asInstanceOf[Double])
              case ResultTag.FloatTag     => Some(got.asInstanceOf[Float])
              case ResultTag.StringTag    => Some(got.asInstanceOf[String])
              case ResultTag.BoolTag      => Some(got.asInstanceOf[Boolean])
              case ResultTag.ByteArrayTag => Some(got.asInstanceOf[Array[Byte]])
              case ResultTag.ByteStreamTag =>
                val rememberedStream = got.asInstanceOf[java.io.InputStream]
                Some(() => rememberedStream)
              case ResultTag.LocalDateTag     => Some(got.asInstanceOf[java.sql.Date].toLocalDate)
              case ResultTag.LocalDateTimeTag => Some(got.asInstanceOf[java.sql.Timestamp].toLocalDateTime)
              case _                          => assert(false, "Unsupported type")
        }
        case ResultTag.ProductTag(_, fields, m) => {
          val nt = fields.asInstanceOf[ResultTag.NamedTupleTag[?, ?]]
          val fieldValues = nt.names.zip(nt.types).zipWithIndex.map { case ((name, tag), idx) =>
            val col = idx + 1 // XXX if you want to use `name` here, you must case-convert it
            tag match
              case ResultTag.IntTag       => rs.getInt(col)
              case ResultTag.LongTag      => rs.getLong(col)
              case ResultTag.DoubleTag    => rs.getDouble(col)
              case ResultTag.FloatTag     => rs.getFloat(col)
              case ResultTag.StringTag    => rs.getString(col)
              case ResultTag.BoolTag      => rs.getBoolean(col)
              case ResultTag.ByteArrayTag => rs.getBytes(col)
              case ResultTag.ByteStreamTag =>
                val rememberedStream = rs.getBinaryStream(col)
                () => rememberedStream
              case ResultTag.LocalDateTag     => rs.getDate(col).toLocalDate
              case ResultTag.LocalDateTimeTag => rs.getTimestamp(col).toLocalDateTime
              case ResultTag.OptionalTag(e) => {
                e match
                  case ResultTag.IntTag =>
                    val got = rs.getInt(col)
                    if rs.wasNull() then None else Some(got)
                  case ResultTag.LongTag =>
                    val got = rs.getLong(col)
                    if rs.wasNull() then None else Some(got)
                  case ResultTag.DoubleTag =>
                    val got = rs.getDouble(col)
                    if rs.wasNull() then None else Some(got)
                  case ResultTag.FloatTag =>
                    val got = rs.getFloat(col)
                    if rs.wasNull() then None else Some(got)
                  case ResultTag.StringTag =>
                    val got = rs.getString(col)
                    if rs.wasNull() then None else Some(got.asInstanceOf[String])
                  case ResultTag.BoolTag =>
                    val got = rs.getBoolean(col)
                    if rs.wasNull() then None else Some(got)
                  case ResultTag.ByteArrayTag =>
                    val got = rs.getObject(col)
                    if rs.wasNull() then None else Some(got.asInstanceOf[Array[Byte]])
                  case ResultTag.ByteStreamTag =>
                    val got = rs.getObject(col)
                    if rs.wasNull() then None
                    else
                      val rememberedStream = got.asInstanceOf[java.io.InputStream]
                      Some(() => rememberedStream)
                  case ResultTag.LocalDateTag =>
                    val got = rs.getDate(col)
                    if rs.wasNull() then None else Some(got.toLocalDate)
                  case ResultTag.LocalDateTimeTag =>
                    val got = rs.getTimestamp(col)
                    if rs.wasNull() then None else Some(got.toLocalDateTime)
                  case _ => assert(false, "Unsupported type")
              }
              case ResultTag.ListTag(e) => {
                val gottenArray = rs.getArray(col).getArray()
                e match
                  case ResultTag.IntTag => gottenArray.asInstanceOf[Array[Integer]].toList
                  case ResultTag.LongTag => gottenArray.asInstanceOf[Array[Object]].toList.map(o =>
                      if o.isInstanceOf[Integer] then o.asInstanceOf[Integer].toLong else Long.unbox(o)
                    ) // XXX postgres will return this as integers
                  case ResultTag.DoubleTag =>
                    gottenArray.asInstanceOf[Array[java.math.BigDecimal]].toList.toList.map(_.doubleValue())
                  case ResultTag.FloatTag =>
                    gottenArray.asInstanceOf[Array[java.math.BigDecimal]].toList.map(_.floatValue())
                  case ResultTag.StringTag => gottenArray.asInstanceOf[Array[Object]].toList.map(_.toString)
                  case ResultTag.BoolTag =>
                    gottenArray.asInstanceOf[Array[Object]].toList.map(a => a.asInstanceOf[Boolean])
                  case ResultTag.LocalDateTag =>
                    gottenArray.asInstanceOf[Array[java.sql.Date]].toList.map(_.toLocalDate())
                  case ResultTag.LocalDateTimeTag =>
                    gottenArray.asInstanceOf[Array[java.sql.Timestamp]].toList.map(_.toLocalDateTime())
                  case _ => assert(false, "Unsupported type inside an array")
              }
              case _ => assert(false, "Unsupported type")
          }
          m.fromProduct(Tuple.fromArray(fieldValues.toArray))
        }
        case ResultTag.NamedTupleTag(names, typesResultTags) =>
          val fieldValues = names.zip(typesResultTags).zipWithIndex.map { case ((name, tag), idx) =>
            val col = idx + 1 // XXX if you want to use `name` here, you must case-convert it
            tag match
              case ResultTag.IntTag           => rs.getInt(col)
              case ResultTag.LongTag          => rs.getLong(col)
              case ResultTag.DoubleTag        => rs.getDouble(col)
              case ResultTag.FloatTag         => rs.getFloat(col)
              case ResultTag.StringTag        => rs.getString(col)
              case ResultTag.BoolTag          => rs.getBoolean(col)
              case ResultTag.ByteArrayTag     => rs.getBytes(col)
              case ResultTag.ByteStreamTag    => () => rs.getBinaryStream(col)
              case ResultTag.LocalDateTag     => rs.getDate(col).toLocalDate
              case ResultTag.LocalDateTimeTag => rs.getTimestamp(col).toLocalDateTime
              case ResultTag.OptionalTag(e) => {
                val got = rs.getObject(col)
                if got == null then None
                else
                  e match
                    case ResultTag.IntTag           => Some(got.asInstanceOf[Int])
                    case ResultTag.LongTag          => Some(got.asInstanceOf[Long])
                    case ResultTag.DoubleTag        => Some(got.asInstanceOf[Double])
                    case ResultTag.FloatTag         => Some(got.asInstanceOf[Float])
                    case ResultTag.StringTag        => Some(got.asInstanceOf[String])
                    case ResultTag.BoolTag          => Some(got.asInstanceOf[Boolean])
                    case ResultTag.ByteArrayTag     => Some(got.asInstanceOf[Array[Byte]])
                    case ResultTag.ByteStreamTag    => Some(() => got.asInstanceOf[java.io.InputStream])
                    case ResultTag.LocalDateTag     => Some(got.asInstanceOf[java.sql.Date].toLocalDate)
                    case ResultTag.LocalDateTimeTag => Some(got.asInstanceOf[java.sql.Timestamp].toLocalDateTime)
                    case _                          => assert(false, "Unsupported type")
              }
              case ResultTag.ListTag(elementType) =>
                val gottenArray = rs.getArray(col).getArray()
                elementType match
                  case ResultTag.IntTag => gottenArray.asInstanceOf[Array[Integer]].toList
                  case ResultTag.LongTag => gottenArray.asInstanceOf[Array[Object]].toList.map(o =>
                      if o.isInstanceOf[Integer] then o.asInstanceOf[Integer].toLong else Long.unbox(o)
                    ) // XXX postgres will return this as integers
                  case ResultTag.DoubleTag =>
                    gottenArray.asInstanceOf[Array[java.math.BigDecimal]].toList.toList.map(_.doubleValue())
                  case ResultTag.FloatTag =>
                    gottenArray.asInstanceOf[Array[java.math.BigDecimal]].toList.map(_.floatValue())
                  case ResultTag.StringTag => gottenArray.asInstanceOf[Array[Object]].toList.map(_.toString)
                  case ResultTag.BoolTag =>
                    gottenArray.asInstanceOf[Array[Object]].toList.map(a => a.asInstanceOf[Boolean])
                  case ResultTag.LocalDateTag =>
                    gottenArray.asInstanceOf[Array[java.sql.Date]].toList.map(_.toLocalDate())
                  case ResultTag.LocalDateTimeTag =>
                    gottenArray.asInstanceOf[Array[java.sql.Timestamp]].toList.map(_.toLocalDateTime())
                  case _ => assert(false, "Unsupported type inside an array")
              case _ => assert(false, "Unsupported type")
          }
          Tuple.fromArray(fieldValues.toArray)
        case ResultTag.ListTag(elementType) =>
          val gottenArray = rs.getArray(1).getArray()
          elementType match
            case ResultTag.IntTag => gottenArray.asInstanceOf[Array[Integer]].toList
            case ResultTag.LongTag => gottenArray.asInstanceOf[Array[Object]].toList.map(o =>
                if o.isInstanceOf[Integer] then o.asInstanceOf[Integer].toLong else Long.unbox(o)
              ) // XXX postgres will return this as integers
            case ResultTag.DoubleTag =>
              gottenArray.asInstanceOf[Array[java.math.BigDecimal]].toList.toList.map(_.doubleValue())
            case ResultTag.FloatTag  => gottenArray.asInstanceOf[Array[java.math.BigDecimal]].toList.map(_.floatValue())
            case ResultTag.StringTag => gottenArray.asInstanceOf[Array[Object]].toList.map(_.toString)
            case ResultTag.BoolTag   => gottenArray.asInstanceOf[Array[Object]].toList.map(a => a.asInstanceOf[Boolean])
            case ResultTag.LocalDateTag => gottenArray.asInstanceOf[Array[java.sql.Date]].toList.map(_.toLocalDate())
            case ResultTag.LocalDateTimeTag =>
              gottenArray.asInstanceOf[Array[java.sql.Timestamp]].toList.map(_.toLocalDateTime())
            case _ => assert(false, "Unsupported type inside an array")
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
  val conn = java.sql.DriverManager.getConnection("jdbc:postgresql://localhost:5433/testdb", "testuser", "testpass")
  // val conn = java.sql.DriverManager.getConnection("jdbc:mysql://localhost:3307/testdb", "testuser", "testpass")
  // val conn = DriverManager.getConnection("jdbc:mariadb://localhost:3308/testdb", "testuser", "testpass")
  val db = DB(conn)
  given tyql.Config = new tyql.Config(tyql.CaseConvention.Underscores, tyql.ParameterStyle.EscapedInline) {}
  import tyql.Dialect.postgresql.given

  final case class Person(pid: Long, name: String, age: Int)
  final case class Orders(oid: Long, personId: Long, orderDate: String)

  // val q = Exprs[(y: Int, m: Int, d: Int)]((CurrentTime.year, CurrentTime.month, lit(java.time.LocalDateTime.now()).day))
  // println(q.toQueryIR.toSQLQuery()._1)
  // println(db.run(q))

  val q2 = Exprs[(a: Int)](Tuple1(lit(10) + VariableInput(() => 20)))
  println(q2.toQueryIR.toSQLQuery()._1)
  pprintln(db.run(q2))
}
