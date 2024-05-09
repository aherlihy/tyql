package test // test package so that it can be imported by bench.
import tyql.*
import language.experimental.namedTuples
import NamedTuple.*

class TestDatabase[Rows <: AnyNamedTuple] {
  def tables: NamedTuple.Map[Rows, Table] = ???
  def init(): Unit = ???
}

trait TestQuery[Rows <: AnyNamedTuple, Return](using val testDB: TestDatabase[Rows]) {
  def testDescription: String
  def query(): Query[Return]
  def sqlString: String
}

trait TestSQLString[Rows <: AnyNamedTuple, Return] extends munit.FunSuite with TestQuery[Rows, Return] {
  test(testDescription) {
    val q = query()
    println(s"query tree for $testDescription:\t$q")
    assertEquals(q.toSQLString, sqlString)
  }
}

abstract class SQLStringTest[Rows <: AnyNamedTuple, Return](using TestDatabase[Rows]) extends TestSQLString[Rows, Return] with TestQuery[Rows, Return]