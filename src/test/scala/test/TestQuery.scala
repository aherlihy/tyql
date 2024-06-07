package test // test package so that it can be imported by bench. Use subpackages so that SBT test reporting is easier to read.
import tyql.*
import language.experimental.namedTuples
import NamedTuple.*

class TestDatabase[Rows <: AnyNamedTuple] {
  def tables: NamedTuple.Map[Rows, Table] = ???
  def init(): Unit = ???
}

trait TestQuery[Rows <: AnyNamedTuple, ReturnShape <: DatabaseAST[?]](using val testDB: TestDatabase[Rows]) {
  def testDescription: String
  def query(): ReturnShape
  def sqlString: String
}

trait TestSQLString[Rows <: AnyNamedTuple, ReturnShape <: DatabaseAST[?]] extends munit.FunSuite with TestQuery[Rows, ReturnShape] {
  test(testDescription) {
    val q = query()
    println(s"$testDescription:\n\t$q")
    assertEquals(q.toSQLString, sqlString)
  }
}

abstract class SQLStringQueryTest[Rows <: AnyNamedTuple, Return](using TestDatabase[Rows]) 
  extends TestSQLString[Rows, Query[Return]] with TestQuery[Rows, Query[Return]]
abstract class SQLStringAggregationTest[Rows <: AnyNamedTuple, Return](using TestDatabase[Rows]) 
  extends TestSQLString[Rows, Aggregation[Return]] with TestQuery[Rows, Aggregation[Return]]
