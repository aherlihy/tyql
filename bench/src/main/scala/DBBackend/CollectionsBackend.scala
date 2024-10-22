package tyql.bench

import buildinfo.BuildInfo

import java.sql.{Connection, DriverManager, ResultSet, Statement}
import scala.annotation.experimental

class CollectionsBackend {
  def connect(): Unit = ???

  def loadData(benchmark: String): Unit = ???

  def runQuery[T](f: T => T): Unit = ???

  def close(): Unit = ???
}