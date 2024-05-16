package test // test package so that it can be imported by bench. Use subpackages so that SBT test reporting is easier to read.
import tyql.*
import language.experimental.namedTuples
import NamedTuple.*

class TestDatabase[Rows <: AnyNamedTuple] {
  def tables: NamedTuple.Map[Rows, Table] = ???
  def init(): Unit = ???
}

trait TestQuery[Rows <: AnyNamedTuple, Return](using val testDB: TestDatabase[Rows]) {
  def testDescription: String
  def query(): Query[Return] // DatabaseAST if distinguishing between aggregates and queries
  def sqlString: String
}

trait TestSQLString[Rows <: AnyNamedTuple, Return] extends munit.FunSuite with TestQuery[Rows, Return] {
  test(testDescription) {
    val q = query()
    println(s"$testDescription:\n\t$q")
    assertEquals(q.toSQLString, sqlString)
  }
}

abstract class SQLStringTest[Rows <: AnyNamedTuple, Return](using TestDatabase[Rows]) extends TestSQLString[Rows, Return] with TestQuery[Rows, Return]