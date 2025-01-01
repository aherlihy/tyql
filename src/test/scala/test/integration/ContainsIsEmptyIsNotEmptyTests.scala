package test.integration.containsisemptyisnonempty

import munit.FunSuite
import test.{withDBNoImplicits, withDB, checkExpr, checkExprDialect}
import java.sql.{Connection, Statement, ResultSet}
import tyql._


class ContainsIsEmptyIsNotEmptyTests extends FunSuite {
  def expect(expected: Boolean)(rs: ResultSet) = assertEquals(rs.getBoolean(1), expected)

  test("empty table is empty") {
    val someTable = Values[(a: Int)](Tuple(1), Tuple(2))
    checkExprDialect[Boolean](someTable.isEmpty, expect(false))(withDB.all)
    checkExprDialect[Boolean](someTable.nonEmpty, expect(true))(withDB.all)
  }

  test("contains") {
    val someTable = Values[(a: Int)](Tuple(1), Tuple(2))
    checkExprDialect[Boolean](someTable.map(_.a).contains(1), expect(true))(withDB.all)
    checkExprDialect[Boolean](someTable.map(_.a).contains(3), expect(false))(withDB.all)
  }
}