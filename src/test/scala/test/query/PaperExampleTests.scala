package test.query.paperexamples

import test.{SQLStringQueryTest, TestDatabase}
import tyql.*
import Query.{fix, restrictedFix}
import Expr.{IntLit, StringLit, count, max, min, sum}
import RestrictedQuery.*

import language.experimental.namedTuples
import NamedTuple.*
import scala.language.implicitConversions

// ========== Example 1: Non-monotonic (BOM) ==========

type BasicPart = (part: String, days: Int)
type SubPart = (part: String, sub: String)
type BOMDB = (basicparts: BasicPart, subparts: SubPart)

given BOMDBs: TestDatabase[BOMDB] with
  override def tables = (
    basicparts = Table[BasicPart]("basicparts"),
    subparts = Table[SubPart]("subparts")
  )

// Figure 7a: Non-monotonic query (P2 disables monotonicity)
class PaperExampleNonMonotonicTest extends SQLStringQueryTest[BOMDB, BasicPart] {
  def testDescription: String = "Paper Example Fig 7a: Non-monotonic BOM query"

  def query() =
    val P2 = (
      constructorFreedom = RestrictedConstructors(),
      monotonicity = NonMonotone(),
      category = SetResult(),
      linearity = Linear(),
      mutual = NoMutual())
    val BasicParts = testDB.tables.basicparts
    val SubParts = testDB.tables.subparts
    BasicParts.fix(P2)(waitFor =>
      SubParts.aggregate(sp =>
        waitFor
          .filter(wf => sp.sub == wf.part)
          .aggregate(wf => (part = sp.part, days = max(wf.days))))
        .groupBySource((wf, _) => (part = wf.part)) // paper syntax shortens to groupBy
        .distinct
    )

  def expectedQueryPattern: String =
    """
    WITH RECURSIVE recursive$A AS
      ((SELECT * FROM basicparts as basicparts$B)
        UNION
      ((SELECT subparts$C.part as part, MAX(ref$D.days) as days FROM subparts as subparts$C, recursive$A as ref$D WHERE subparts$C.sub = ref$D.part GROUP BY subparts$C.part)))
    SELECT * FROM recursive$A as recref$E
    """
}

// ========== Example 2: Non-linear (TC) ==========

type Edge = (x: Int, y: Int)
type EdgeDB = (edges: Edge)

given EdgeDBs: TestDatabase[EdgeDB] with
  override def tables = (
    edges = Table[Edge]("edges")
  )

// Figure 7b: Non-linear query (P4 disables linearity)
class PaperExampleNonLinearTest extends SQLStringQueryTest[EdgeDB, Edge] {
  def testDescription: String = "Paper Example Fig 7b: Non-linear TC query"

  def query() =
    val P4 = (
      constructorFreedom = RestrictedConstructors(),
      monotonicity = Monotone(),
      category = SetResult(),
      linearity = NonLinear(),
      mutual = NoMutual()
    )
    val Edges = testDB.tables.edges
    Edges.fix(P4)(pathR =>
      pathR.flatMap(p =>
        pathR
          .filter(e => p.y == e.x)
          .map(e => (x = p.x, y = e.y)))
        .distinct
    )

  def expectedQueryPattern: String =
    """
    WITH RECURSIVE recursive$A AS
      ((SELECT * FROM edges as edges$B)
        UNION
      ((SELECT ref$C.x as x, ref$D.y as y FROM recursive$A as ref$C, recursive$A as ref$D WHERE ref$C.y = ref$D.x)))
    SELECT * FROM recursive$A as recref$E
    """
}

// ========== Example 3: Bag-semantic (Ancestry) ==========

type Parent = (par: String, ch: String)
type Generation = (nm: String, g: Int)
type ParentDB = (parents: Parent)

given ParentDBs: TestDatabase[ParentDB] with
  override def tables = (
    parents = Table[Parent]("parents")
  )

// Figure 7c: Bag-semantic query with constructors (P5P6 disables set semantics and constructor freedom)
class PaperExampleBagSemanticTest extends SQLStringQueryTest[ParentDB, Generation] {
  def testDescription: String = "Paper Example Fig 7c: Bag-semantic, constructors, same-generation query"

  def query() =
    val P5P6 = (
      constructorFreedom = NonRestrictedConstructors(),
      monotonicity = Monotone(),
      category = BagResult(),
      linearity = Linear(),
      mutual = NoMutual()
    )
    val Parents = testDB.tables.parents


    Parents
      .filter(p => p.par == "A")
      .map(e => (nm = e.ch, g = 1))
      .fix(P5P6)(gensR =>
        Parents.flatMap(p =>
          gensR
            .filter(g => p.par == g.nm)
            .map(g => (nm = p.ch, g = g.g + 1))))
        .filter(g => g.g == 2)

  def expectedQueryPattern: String =
    """
    WITH RECURSIVE recursive$A AS
      ((SELECT parents$B.ch as nm, 1 as g FROM parents as parents$B WHERE parents$B.par = "A")
        UNION ALL
      ((SELECT parents$C.ch as nm, ref$D.g + 1 as g FROM parents as parents$C, recursive$A as ref$D WHERE parents$C.par = ref$D.nm)))
    SELECT * FROM recursive$A as recref$E WHERE recref$E.g = 2
    """
}
