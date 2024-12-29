package test.config.caseconvention

import munit.FunSuite
import tyql.{Config, Dialect}
import tyql.{CaseConvention, ParameterStyle}
import test.withDB
import test.needsDBs

import tyql.{Table, DatabaseAST, Query}
import scala.language.experimental.namedTuples
import NamedTuple.{NamedTuple, AnyNamedTuple}
import com.mysql.cj.x.protobuf.MysqlxNotice.SessionStateChanged.Parameter
import org.junit.experimental.theories.ParameterSupplier

class CaseConventionTests extends FunSuite {
  private val expectations = Seq(
    ("aa bb cc", "aa_bb_cc", "aaBbCc", "AaBbCc", "AA_BB_CC", "aabbcc", "AABBCC"),
    ("aabbcc",   "aabbcc",   "aabbcc", "Aabbcc", "AABBCC",   "aabbcc", "AABBCC"),
    ("aaBb_cc",  "aa_bb_cc", "aaBbCc", "AaBbCc", "AA_BB_CC", "aabbcc", "AABBCC"),
    ("AaBbCc",   "aa_bb_cc", "aaBbCc", "AaBbCc", "AA_BB_CC", "aabbcc", "AABBCC"),
    ("abc12",    "abc12",    "abc12",  "Abc12",  "ABC12",    "abc12",  "ABC12"),
    ("abc_12",   "abc_12",   "abc12",  "Abc12",  "ABC_12",   "abc12",  "ABC12"),
    ("abC12",    "ab_c12",   "abC12",  "AbC12",  "AB_C12",   "abc12",  "ABC12"),
    ("ABC",      "a_b_c",    "aBC",    "ABC",    "A_B_C",    "abc",    "ABC"),
  )

  test("expected case conversions") {
    for (e <- expectations) {
      val (in, underscores, camelCase, pascalCase, capitalUnderscores, joined, joinedCapital) = e
      assertEquals(CaseConvention.Exact.convert(in), in)
      assertEquals(CaseConvention.Underscores.convert(in), underscores)
      assertEquals(CaseConvention.CamelCase.convert(in), camelCase)
      assertEquals(CaseConvention.PascalCase.convert(in), pascalCase)
      assertEquals(CaseConvention.CapitalUnderscores.convert(in), capitalUnderscores)
      assertEquals(CaseConvention.Joined.convert(in), joined)
      assertEquals(CaseConvention.JoinedCapital.convert(in), joinedCapital)
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
    given Config = new Config(caseConvention = CaseConvention.CamelCase, parameterStyle = ParameterStyle.EscapedInline) {}
    check()
  }

  test("postgres handles it".tag(needsDBs)) {
    withDB.postgres{ conn =>

      def check(tableName: String, columnName: String, postgresTableName: String = null, postgresColumnName: String = null)(using cnf: Config) = {
        val escapedTableName = summon[Dialect].quoteIdentifier(Option(postgresTableName).getOrElse(tableName))
        val escapedColunmName = summon[Dialect].quoteIdentifier(Option(postgresColumnName).getOrElse(columnName))

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

      check("caseCon ventionTests 19471", "aaBb_Cc")(using new Config(CaseConvention.Exact, ParameterStyle.EscapedInline) {})
      check("caseConventionTests19471", "aaBbCc")(using new Config(CaseConvention.CamelCase, ParameterStyle.EscapedInline) {})
      check("CaseConventionTests19471", "AaBbCc")(using new Config(CaseConvention.PascalCase, ParameterStyle.EscapedInline) {})
      check("case_convention_tests19471", "aa_bb_cc")(using new Config(CaseConvention.Underscores, ParameterStyle.EscapedInline) {})
      check("CaseConventionTests19471", "aaBb_Cc", postgresTableName="CASE_CONVENTION_TESTS19471", postgresColumnName="AA_BB_CC")(using new Config(CaseConvention.CapitalUnderscores, ParameterStyle.EscapedInline) {}) // XXX AA_BB_CC would be interpreted as a_a_b_c !
      check("caseconventiontests19471", "aabbcc")(using new Config(CaseConvention.Joined, ParameterStyle.EscapedInline) {})
      check("CaseConventionTests19471", "aaBb_Cc", postgresTableName="CASECONVENTIONTESTS19471", postgresColumnName="AABBCC")(using new Config(CaseConvention.JoinedCapital, ParameterStyle.EscapedInline) {}) // XXX AA_BB_CC would be interpreted as a_a_b_c !
      // TODO document this weird (?) behavior, or maybe change it?
      // In the Scala code you must use `_`s or capital letters as separators, and the config
      //   changes only what is ouputted to the DB, so you cannot use something like ABC as the column name from
      //   the Scala code since it will be interpreted as a compound name ['a', 'b', 'c'].
    }
  }
}
