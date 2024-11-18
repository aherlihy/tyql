package test.config.caseconvention

import munit.FunSuite
import tyql.{Config, Dialect}
import tyql.CaseConvention
import test.withDB
import test.needsDBs

import tyql.{Table, DatabaseAST, Query}
import scala.language.experimental.namedTuples
import NamedTuple.{NamedTuple, AnyNamedTuple}

class CaseConventionTests extends FunSuite {
  private val expectations = Seq(
    ("aa bb cc", "aa_bb_cc", "aaBbCc", "AaBbCc"),
    ("aabbcc",   "aabbcc",   "aabbcc", "Aabbcc"),
    ("aaBb_cc",  "aa_bb_cc", "aaBbCc", "AaBbCc"),
    ("AaBbCc",   "aa_bb_cc", "aaBbCc", "AaBbCc"),
    ("abc12",    "abc12",    "abc12",  "Abc12"),
    ("abc_12",   "abc_12",   "abc12",  "Abc12"),
    ("abC12",    "ab_c12",   "abC12",  "AbC12"),
    ("ABC",      "a_b_c",    "aBC",    "ABC"),
  )

  test("expected case conversions") {
    for (e <- expectations) {
      val (in, underscores, camelCase, pascalCase) = e
      assertEquals(CaseConvention.Exact.convert(in), in)
      assertEquals(CaseConvention.Underscores.convert(in), underscores)
      assertEquals(CaseConvention.CamelCase.convert(in), camelCase)
      assertEquals(CaseConvention.PascalCase.convert(in), pascalCase)
    }
  }

  test("by default exact case conversion") {
    def check()(using c: Config) = {
      assertEquals(c.caseConvention.convert("aa bb cc"), "aa bb cc")
      assertEquals(c.caseConvention.convert("aaBb_cc"), "aaBb_cc")
    }
    check()
  }

  test("you can select a different case conversion") {
    def check()(using c: Config) = {
      assertEquals(c.caseConvention.convert("aa bb cc"), "aaBbCc")
      assertEquals(c.caseConvention.convert("aaBb_cc"), "aaBbCc")
    }
    given Config = new Config(caseConvention = CaseConvention.CamelCase) {}
    check()
  }

  test("postgres handles it".tag(needsDBs)) {
    withDB.postgres{ conn =>

      def check(tableName: String, columnName: String)(using cnf: Config) = {
        val escapedTableName = summon[Dialect].quoteIdentifier(tableName)
        val escapedColunmName = summon[Dialect].quoteIdentifier(columnName)

        case class Tbl(id: Int, aaBb_Cc: String)
        val q = Table[Tbl](tableName).map(b => b.aaBb_Cc)
        val stmt = conn.createStatement()      
        try {
          stmt.executeUpdate(s"drop table if exists ${escapedTableName};")
          stmt.executeUpdate(s"create table ${escapedTableName}(${escapedColunmName} int);")
          stmt.executeUpdate(s"insert into ${escapedTableName} (${escapedColunmName}) values (117);")
          val r = stmt.executeQuery(q.toQueryIR.toSQLString())
          assert(r.next())
          assertEquals(r.getInt(cnf.caseConvention.convert(columnName)), 117)
          stmt.executeUpdate(s"drop table ${escapedTableName};")
        }
        catch {
          case e: Exception => throw e
        }
        finally stmt.close()
      }

      check("caseCon ventionTests 19471", "aaBb_Cc")(using new Config(CaseConvention.Exact) {})
      check("caseConventionTests19471", "aaBbCc")(using new Config(CaseConvention.CamelCase) {})
      check("CaseConventionTests19471", "AaBbCc")(using new Config(CaseConvention.PascalCase) {})
      check("case_convention_tests19471", "aa_bb_cc")(using new Config(CaseConvention.Underscores) {})
    }
  }
}
