package test // test package so that it can be imported by bench.
import tyql.*

class TestDatabase[Rows <: Tuple] { // this could be a named tuple?
  def tables: Tuple.Map[Rows, Table] = ???
  def init(): Unit = ???
}

trait TestQuery[Rows <: Tuple, Return](using val testDB: TestDatabase[Rows]) {
  def testDescription: String
  def query(): Query[Return]
  def sqlString: String
}

trait TestSQLString[Rows <: Tuple, Return] extends munit.FunSuite with TestQuery[Rows, Return] {
  test(testDescription) {
    val q = query()
    println(s"query tree for $testDescription: $q")
    assertEquals(q.toSQLString, sqlString)
  }
}

abstract class SQLStringTest[Rows <: Tuple, Return](using TestDatabase[Rows]) extends TestSQLString[Rows, Return] with TestQuery[Rows, Return]