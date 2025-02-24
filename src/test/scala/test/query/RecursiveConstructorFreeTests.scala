package test.query.constructorfree
import test.{SQLStringAggregationTest, SQLStringQueryTest, TestDatabase}
import tyql.*
import Query.{fixConstructors}
import Expr.{IntLit, StringLit, count, max, min, sum}

import language.experimental.namedTuples
import NamedTuple.*
import scala.language.implicitConversions
import test.query.recursive.{Edge}
import test.query.recursivebenchmarks.{WeightedGraphDB, WeightedGraphDBs}

type FibNum = (recursionDepth: Int, fibonacciNumber: Int, nextNumber: Int)
type FibNumDB = (base: FibNum, result: FibNum)

given FibNumDBs: TestDatabase[FibNumDB] with
  override def tables = (
    base = Table[FibNum]("base"),
    result = Table[FibNum]("result")
  )

  override def init(): String = """
    CREATE TABLE base (
      recursionDepth INT,
      fibonacciNumber INT,
      nextNumber INT
    );
    INSERT INTO base (recursionDepth, fibonacciNumber, nextNumber) VALUES (0, 0, 1);
  """
class RecursionFibTest extends SQLStringQueryTest[FibNumDB, (FibonacciNumberIndex: Int, FibonacciNumber: Int)] {
  def testDescription: String = "Fibonacci example from duckdb docs"

  def query() =
    val fib = testDB.tables.base
    fib.fixConstructors(fib =>
      fib
        .filter(f => (f.recursionDepth + 1) < 10)
        .map(f => (recursionDepth = f.recursionDepth + 1, fibonacciNumber = f.nextNumber, nextNumber = f.fibonacciNumber + f.nextNumber).toRow)
        .distinct
    ).map(f => (FibonacciNumberIndex = f.recursionDepth, FibonacciNumber = f.fibonacciNumber).toRow)
  def expectedQueryPattern: String =
    """
    WITH RECURSIVE
      recursive$1 AS
        ((SELECT * FROM base as base$1)
            UNION
        ((SELECT
            ref$0.recursionDepth + 1 as recursionDepth, ref$0.nextNumber as fibonacciNumber, ref$0.fibonacciNumber + ref$0.nextNumber as nextNumber
         FROM recursive$1 as ref$0
         WHERE ref$0.recursionDepth + 1 < 10)))
    SELECT recref$0.recursionDepth as FibonacciNumberIndex, recref$0.fibonacciNumber as FibonacciNumber FROM recursive$1 as recref$0
      """
}

type Path = (startNode: Int, endNode: Int, path: List[Int])
type ReachabilityDB = (edge: Edge)

given ReachabilityDBs: TestDatabase[ReachabilityDB] with
  override def tables = (
    edge = Table[Edge]("edge")
    )

  override def init(): String = """
    CREATE TABLE edge (x INTEGER, y INTEGER);
    INSERT INTO edge
      VALUES
        (1, 3), (1, 5), (2, 4), (2, 5), (2, 10), (3, 1), (3, 5), (3, 8), (3, 10),
        (5, 3), (5, 4), (5, 8), (6, 3), (6, 4), (7, 4), (8, 1), (9, 4);
    """
class RecursionShortestPathTest extends SQLStringQueryTest[ReachabilityDB, Path] {
  def testDescription: String = "Shortest path example from duckdb docs"

  def query() =
    val pathBase = testDB.tables.edge
      .map(e => (startNode = e.x, endNode = e.y, path = List(e.x, e.y).toExpr).toRow)
      .filter(p => p.startNode == 1) // Filter after map means subquery

    pathBase.fixConstructors(path =>
      path.flatMap(p =>
        testDB.tables.edge
          .filter(e =>
            e.x == p.endNode && path.filter(p2 => p2.path.contains(e.y)).isEmpty
          )
          .map(e =>
            (startNode = p.startNode, endNode = e.y, path = p.path.append(e.y)).toRow
          )
      ).distinct
    ).sort(p => p.path, Ord.ASC).sort(p => p.path.length, Ord.ASC)

  def expectedQueryPattern: String =
    """
      WITH RECURSIVE
          recursive$116 AS
            ((SELECT * FROM
              (SELECT edge$116.x as startNode, edge$116.y as endNode, [edge$116.x, edge$116.y] as path
               FROM edge as edge$116) as subquery$117
             WHERE subquery$117.startNode = 1)
                UNION
            ((SELECT
                ref$58.startNode as startNode, edge$118.y as endNode, list_append(ref$58.path, edge$118.y) as path
             FROM recursive$116 as ref$58, edge as edge$118
             WHERE edge$118.x = ref$58.endNode
                AND
             NOT EXISTS (SELECT * FROM recursive$116 as ref$60 WHERE list_contains(ref$60.path, edge$118.y)))))
      SELECT * FROM recursive$116 as recref$9 ORDER BY length(recref$9.path) ASC, path ASC
      """
}

type Tag = (id: Int, name: String, subclassof: Int)
type TagHierarchy = (id: Int, source: String, path: List[String])
type TagDB = (tag: Tag, hierarchy: TagHierarchy)

given TagDBs: TestDatabase[TagDB] with
  override def tables = (
    tag = Table[Tag]("tag"),
    hierarchy = Table[TagHierarchy]("hierarchy")
  )

  override def init(): String = """
    CREATE TABLE tag (id INTEGER, name VARCHAR, subclassof INTEGER);
    INSERT INTO tag VALUES
      (1, 'U2',     5),
      (2, 'Blur',   5),
      (3, 'Oasis',  5),
      (4, '2Pac',   6),
      (5, 'Rock',   7),
      (6, 'Rap',    7),
      (7, 'Music',  9),
      (8, 'Movies', 9),
      (9, 'Art', -1);
    """

class RecursionTreeTest extends SQLStringQueryTest[TagDB, List[String]] {
  def testDescription: String = "Tag tree example from duckdb docs"

  def query() =
    // For now encode NULL as -1, TODO: implement nulls
    import Expr.{toRow, toExpr}
    val tagHierarchy0 = testDB.tables.tag
      .filter(t => t.subclassof == -1)
      .map(t =>
        val initListPath: Expr.ListExpr[String] = List(t.name).toExpr
        (id = t.id, source = t.name, path = initListPath).toRow
      )
    tagHierarchy0.fixConstructors(tagHierarchy1 =>
      tagHierarchy1.flatMap(hier =>
        testDB.tables.tag
          .filter(t => t.subclassof == hier.id)
          .map(t =>
            val listPath = hier.path.prepend(t.name)
            (id = t.id, source = t.name, path = listPath).toRow
          )
      ).distinct
    ).filter(h => h.source == "Oasis").map(h => h.path)
  def expectedQueryPattern: String =
    """
      WITH RECURSIVE
        recursive$62 AS
          ((SELECT
              tag$62.id as id, tag$62.name as source, [tag$62.name] as path
           FROM tag as tag$62
           WHERE tag$62.subclassof = -1)
                UNION
           ((SELECT
              tag$64.id as id, tag$64.name as source, list_prepend(tag$64.name, ref$30.path) as path
            FROM recursive$62 as ref$30, tag as tag$64
            WHERE tag$64.subclassof = ref$30.id)))
      SELECT recref$5.path FROM recursive$62 as recref$5 WHERE recref$5.source = "Oasis"
      """
}

type Parent = (parent: String, child: String)
type AncestryDB = (parent: Parent)

given ancestryDBs: TestDatabase[AncestryDB] with
  override def tables = (
    parent = Table[Parent]("parents")
    )

class AncestryTest extends SQLStringQueryTest[AncestryDB, (name: String)] {
  def testDescription: String = "Ancestry query to calculate total number of descendants in the same generation"

  def query() =
    val base = testDB.tables.parent.filter(p => p.parent == "Alice").map(e => (name = e.child, gen = IntLit(1)).toRow)
    base.fixConstructors(gen =>
      testDB.tables.parent.flatMap(parent =>
        gen
          .filter(g => parent.parent == g.name)
          .map(g => (name = parent.child, gen = g.gen + 1).toRow)
      ).distinct
    ).filter(g => g.gen == 2).map(g => (name = g.name).toRow)


  def expectedQueryPattern: String =
    """
      WITH RECURSIVE
        recursive$162 AS
          ((SELECT parents$162.child as name, 1 as gen
            FROM parents as parents$162
            WHERE parents$162.parent = "Alice")
            UNION
          ((SELECT parents$164.child as name, ref$86.gen + 1 as gen
            FROM parents as parents$164, recursive$162 as ref$86
            WHERE parents$164.parent = ref$86.name)))
      SELECT recref$14.name as name FROM recursive$162 as recref$14 WHERE recref$14.gen = 2
            """
}

type Number = (id: Int, value: Int)
type NumberType = (value: Int, typ: String)
type EvenOddDB = (numbers: Number)

given EvenOddDBs: TestDatabase[EvenOddDB] with
  override def tables = (
    numbers = Table[Number]("numbers")
    )

class EvenOddTest extends SQLStringQueryTest[EvenOddDB, NumberType] {
  def testDescription: String = "Mutually recursive even-odd (classic)"

  def query() =
    val evenBase = testDB.tables.numbers.filter(n => n.value == 0).map(n => (value = n.value, typ = StringLit("even")).toRow)
    val oddBase = testDB.tables.numbers.filter(n => n.value == 1).map(n => (value = n.value, typ = StringLit("odd")).toRow)

    val (even, odd) = fixConstructors((evenBase, oddBase))((even, odd) =>
      val evenResult = testDB.tables.numbers.flatMap(num =>
        odd.filter(o => num.value == o.value + 1).map(o => (value = num.value, typ = StringLit("even")))
      ).distinct
      val oddResult = testDB.tables.numbers.flatMap(num =>
        even.filter(e => num.value == e.value + 1).map(e => (value = num.value, typ = StringLit("odd")))
      ).distinct
      (evenResult, oddResult)
    )
    odd

  def expectedQueryPattern: String =
    """
      WITH RECURSIVE
        recursive$1 AS
          ((SELECT numbers$2.value as value, "even" as typ
            FROM numbers as numbers$2
            WHERE numbers$2.value = 0)
          UNION
            ((SELECT numbers$4.value as value, "even" as typ
              FROM numbers as numbers$4, recursive$2 as ref$5
              WHERE numbers$4.value = ref$5.value + 1))),
         recursive$2 AS
          ((SELECT numbers$8.value as value, "odd" as typ
            FROM numbers as numbers$8
            WHERE numbers$8.value = 1)
              UNION
          ((SELECT numbers$10.value as value, "odd" as typ
            FROM numbers as numbers$10, recursive$1 as ref$8
            WHERE numbers$10.value = ref$8.value + 1)))
        SELECT * FROM recursive$2 as recref$1
    """
}

class RecursionSSSPTest extends SQLStringQueryTest[WeightedGraphDB, (dst: Int, cost: Int)] {
  def testDescription: String = "Single source shortest path"

  def query() =
    val base = testDB.tables.base
    base.fixConstructors(sp =>
      testDB.tables.edge.flatMap(edge =>
        sp
          .filter(s => s.dst == edge.src)
          .map(s => (dst = edge.dst, cost = s.cost + edge.cost).toRow)
      ).distinct
    ).aggregate(s => (dst = s.dst, cost = min(s.cost)).toGroupingRow).groupBySource(s => (dst = s._1.dst).toRow)

  def expectedQueryPattern: String =
    """
          WITH RECURSIVE
            recursive$62 AS
              ((SELECT * FROM base as base$62)
                  UNION
                ((SELECT
                    edge$64.dst as dst, ref$29.cost + edge$64.cost as cost
                  FROM edge as edge$64, recursive$62 as ref$29
                  WHERE ref$29.dst = edge$64.src)))
          SELECT recref$5.dst as dst, MIN(recref$5.cost) as cost FROM recursive$62 as recref$5 GROUP BY recref$5.dst
        """
}
