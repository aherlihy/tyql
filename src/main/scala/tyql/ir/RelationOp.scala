package tyql

import tyql.SelectFlags.Final

// TODO in this file we probably lose the original ASTs when merging, they're only replaced with the second operand's AST
// TODO in `mergeWith`, some cases are missing, unclear if the catch-all implementation is enough

/**
 * Modifiers for query generation, e.g. queries at the expression level need surrounding parens.
 */
enum SelectFlags:
  case Distinct   // used by select queries
  case Final      // top-level final result, e.g. avoid extra parens or aliasing
  case ExprLevel  // expression-level relation operation

enum PositionFlag:
  case Project, Where, Subquery

class SymbolTable(val innerTable: Map[String, RelationOp] = Map.empty):
  def bind(alias: String, pointsTo: RelationOp): SymbolTable =
    SymbolTable(innerTable + (alias -> pointsTo))
  def bind(aliases: Iterable[(String, RelationOp)]): SymbolTable =
    SymbolTable(innerTable ++ aliases)
  def apply(alias: String): RelationOp =
    if (innerTable.contains(alias))
      innerTable(alias)
    else
      throw new Exception(s"Symbol $alias missing from symbol table. Contents: ${innerTable.keys.mkString("[", ", ", "]")}")

/**
 * Relation-level operations, e.g. a table, union of tables, SELECT query, etc.
 */
trait RelationOp extends QueryIRNode:
  var flags: Set[SelectFlags] = Set.empty
  def alias: String
  val carriedSymbols: List[(String, RecursiveIRVar)] = List.empty

  /**
   * Avoid overloading
   * TODO: decide if we want to mutate, or copy + discard IR nodes. Right now its a mix, which should be improved
   */
  def mergeWith(r: RelationOp, astOther: DatabaseAST[?]): RelationOp

  /**
   * Equivalent to adding a .filter(w)
   * @param w - predicate expression
   */
  def appendWhere(w: WhereClause, astOther: DatabaseAST[?]): RelationOp

  /**
   * Equivalent to adding a .map(p)
   * @param p - a projection expression
   */
  def appendProject(p: QueryIRNode, astOther: DatabaseAST[?]): RelationOp

  /**
   * Add some extra metadata needed to generate "nice" SQL strings.
   * Also used to handle edge cases, e.g. expression-level relation-ops should get aliases, etc.
   * @param f - the flag
   */
  def appendFlag(f: SelectFlags): RelationOp
  def appendFlags(f: Set[SelectFlags]): RelationOp =
    f.foldLeft(this)((r: RelationOp, f) => r.appendFlag(f))

  def wrapStringStart(ctx: SQLRenderingContext): Unit =
    if !flags.contains(SelectFlags.Final) then
      ctx.sql.append("(")

  def wrapStringEnd(ctx: SQLRenderingContext): Unit =
    if !flags.contains(SelectFlags.Final) then
      ctx.sql.append(")")
    if !(flags.contains(SelectFlags.Final) || flags.contains(SelectFlags.ExprLevel)) then
      ctx.sql.append(" as ")
      ctx.sql.append(alias)

/**
 * Simple table read.
 */
case class TableLeaf(tableName: String, ast: Table[?]) extends RelationOp with QueryIRLeaf:
  val name = s"$tableName${QueryIRTree.idCount}"
  QueryIRTree.idCount += 1
  override def alias = name
  override def computeSQL(using d: Dialect)(using cnf: Config)(ctx: SQLRenderingContext): Unit =
    val escapedTableName = d.quoteIdentifier(cnf.caseConvention.convert(tableName))
    ctx.sql.append(escapedTableName)
    if !(flags.contains(SelectFlags.Final)) then
      ctx.sql.append(" as ")
      val escapedAlias = d.quoteIdentifier(cnf.caseConvention.convert(name))
      ctx.sql.append(escapedAlias)

  override def toString: String = s"TableLeaf($tableName as $name)"

  override def mergeWith(r: RelationOp, astOther: DatabaseAST[?]): RelationOp =
    r match
      case t: (TableLeaf | RecursiveIRVar) =>
        SelectAllQuery(Seq(this, t), Seq(), None, astOther)
      case q: SelectAllQuery =>
        SelectAllQuery(this +: q.from, q.where, None, astOther)
      case q: SelectQuery =>
        SelectQuery(
          q.project,
          this +: q.from,
          q.where,
          None,
          astOther
        )
      case r: RelationOp =>
        // default to subquery, some ops may want to override
        SelectAllQuery(Seq(this, r), Seq(), None, astOther)

  override def appendWhere(w: WhereClause, astOther: DatabaseAST[?]): RelationOp =
    SelectAllQuery(Seq(this), Seq(w), Some(alias), astOther).appendFlags(flags)

  override def appendProject(p: QueryIRNode, astOther: DatabaseAST[?]): RelationOp =
    SelectQuery(p, Seq(this), Seq(), None, astOther).appendFlags(flags)

  override def appendFlag(f: SelectFlags): RelationOp =
    val q = f match
      case SelectFlags.Distinct => // Distinct is special case because needs to be "hoisted" to enclosing SELECT, so alias is kept
        SelectAllQuery(Seq(this), Seq(), Some(alias), ast)
      case _ =>
        SelectAllQuery(Seq(this), Seq(), None, ast)

    q.flags = q.flags + f
    q

/**
 * Select query, e.g. SELECT * FROM <Relations> WHERE <where?>
 * Separate from SelectQuery because SELECT * queries can be unnested
 */
case class SelectAllQuery(from: Seq[RelationOp],
                       where: Seq[QueryIRNode],
                       overrideAlias: Option[String],
                       ast: DatabaseAST[?]) extends RelationOp:
  val name = overrideAlias.getOrElse({
    val latestVar = s"subquery${QueryIRTree.idCount}"
    QueryIRTree.idCount += 1
    latestVar
  })

  override def alias = name

  override def appendWhere(w: WhereClause, astOther: DatabaseAST[?]): RelationOp =
    SelectAllQuery(from, where :+ w, Some(alias), astOther).appendFlags(flags)

  override def appendProject(p: QueryIRNode, astOther: DatabaseAST[?]): RelationOp =
    SelectQuery(p, from, where, None, ast).appendFlags(flags)

  override def mergeWith(r: RelationOp, astOther: DatabaseAST[?]): RelationOp =
    r match
      case t: (TableLeaf | RecursiveIRVar) =>
        SelectAllQuery(from :+ t, where, None, astOther)
      case q: SelectAllQuery =>
        val newF = from ++ q.from
        val newW = where ++ q.where
        SelectAllQuery(newF, newW, None, astOther)
      case q: SelectQuery =>
        val newF = from ++ q.from
        val newW = where ++ q.where
        SelectQuery(q.project, newF, newW, None, astOther)
      case n: NaryRelationOp =>
        n.mergeWith(this, astOther)

      case r: RelationOp =>
        // default to subquery, some ops may want to override
        SelectAllQuery(Seq(this, r), Seq(), None, astOther)

  override def appendFlag(f: SelectFlags): RelationOp =
    flags = flags + f
    this

  override def computeSQL(using d: Dialect)(using cnf: Config)(ctx: SQLRenderingContext): Unit =
    wrapStringStart(ctx)
    ctx.sql.append("SELECT ")
    if flags.contains(SelectFlags.Distinct) then
      ctx.sql.append("DISTINCT ")
    ctx.sql.append("* FROM ")
    ctx.mkString(from, ", ")
    if where.nonEmpty then
      ctx.sql.append(" WHERE ")
      if where.size == 1 then
        where.head.computeSQL(ctx)
      else
        ctx.mkString(where, "(", " AND ", ")")
    wrapStringEnd(ctx)

  override def toString: String = // for debugging
    s"SelectAllQuery(\n\talias=$alias,\n\tfrom=$from,\n\twhere=$where\n)"

/**
 * Select query, e.g. SELECT <Project> FROM <Relations> WHERE <where>
 * TODO: Eventually specialize join nodes, for now use Select with multiple FROM fields to indicate join.
 * Right now the AST only supports nested flatMaps, will eventually expand syntax to support specifying join type.
 */
case class SelectQuery(project: QueryIRNode,
                       from: Seq[RelationOp],
                       where: Seq[QueryIRNode],
                       overrideAlias: Option[String],
                       ast: DatabaseAST[?]) extends RelationOp:
  val name = overrideAlias.getOrElse({
    val latestVar = s"subquery${QueryIRTree.idCount}"
    QueryIRTree.idCount += 1
    latestVar
  })
  override def alias = name

  override def appendWhere(w: WhereClause, astOther: DatabaseAST[?]): RelationOp =
    // SelectQuery(project, from, where :+ w, Some(alias), astOther).appendFlags(flags)
    // Appending a where clause to something that already has a project triggers subquery
    SelectAllQuery(Seq(this), Seq(w), Some(alias), astOther)

  override def appendProject(p: QueryIRNode, astOther: DatabaseAST[?]): RelationOp =
    // TODO define semantics of map(f1).map(f2), could collapse into map(f2(f1))? For now just trigger subquery
    SelectQuery(p, Seq(this), Seq(), None, astOther).appendFlags(flags)


  override def mergeWith(r: RelationOp, astOther: DatabaseAST[?]): RelationOp =
    r match
      case t: (TableLeaf  | RecursiveIRVar) =>
        SelectAllQuery(Seq(this, t), Seq(), None, astOther)
      case q: SelectAllQuery =>
        SelectAllQuery(this +: q.from, q.where, None, astOther)
      case q: SelectQuery =>
        // TODO define semantics of map(f1).map(f2), could collapse into map(f2(f1))? For now just trigger subquery
        SelectAllQuery(Seq(this, q), Seq(), None, astOther)
      case r: RelationOp =>
        // default to subquery, some ops may want to override
        SelectAllQuery(Seq(this, r), Seq(), None, astOther)

  override def appendFlag(f: SelectFlags): RelationOp =
    flags = flags + f
    this

  override def computeSQL(using d: Dialect)(using cnf: Config)(ctx: SQLRenderingContext): Unit =
    wrapStringStart(ctx)
    ctx.sql.append("SELECT ")
    if flags.contains(SelectFlags.Distinct) then ctx.sql.append("DISTINCT ")
    project.computeSQL(ctx)
    ctx.sql.append(" FROM ")
    ctx.mkString(from, ", ")
    if where.nonEmpty then
      ctx.sql.append(" WHERE ")
      if where.size == 1 then
        where.head.computeSQL(ctx)
      else
        ctx.mkString(where, "(", " AND ", ")")
    wrapStringEnd(ctx)

  override def toString: String = // for debugging
    s"SelectQuery(\n\talias=$alias,\n\tproject=$project,\n\tfrom=$from,\n\twhere=$where\n)"

/**
 * Query with ORDER BY clause
 */
case class OrderedQuery(query: RelationOp, sortFn: Seq[(QueryIRNode, Ord)], ast: DatabaseAST[?]) extends RelationOp:
  override def alias = query.alias

  val orders: Seq[Ord] = sortFn.map(_._2)

  override def appendWhere(w: WhereClause, astOther: DatabaseAST[?]): RelationOp =
    // Does not trigger subquery, e.g. relation.sort(s).filter(f) => SELECT * FROM relation WHERE f ORDER BY s
    OrderedQuery(query.appendWhere(w, astOther), sortFn, ast).appendFlags(flags)

  override def appendProject(p: QueryIRNode, astOther: DatabaseAST[?]): RelationOp =
    // Triggers a subquery, e.g. relation.sort(s).map(m) => SELECT m FROM (SELECT * from relation ORDER BY s).
    // Note relation.map(m).sort(s) does not trigger a subquery => SELECT m FROM relation ORDER BY s
    SelectQuery(
      p,
      Seq(this),
      Seq(),
      None,
      astOther
    )

  override def mergeWith(r: RelationOp, astOther: DatabaseAST[?]): RelationOp =
    r match
      case t: (TableLeaf | RecursiveIRVar) =>
        OrderedQuery(query.mergeWith(t, astOther), sortFn, ast)
      case q: SelectAllQuery =>
        SelectAllQuery(
          this +: q.from,
          q.where,
          Some(q.alias),
          astOther
        )
      case q: SelectQuery =>
        SelectQuery(
          q.project,
          this +: q.from,
          q.where,
          Some(q.alias),
          astOther
        )
      case o: OrderedQuery =>
        // hoist where
        val (newQ1, whereQ1) = query match
          case q1: SelectAllQuery =>
            (
              OrderedQuery(SelectAllQuery(q1.from, Seq(), Some(q1.alias), q1.ast).appendFlag(SelectFlags.Final), sortFn, ast),
              q1.where
            )
          case _ => (this, Seq())
        val (newQ2, whereQ2) = o.query match
          case q2: SelectAllQuery =>
            (
              OrderedQuery(SelectAllQuery(q2.from, Seq(), Some(q2.alias), q2.ast).appendFlag(SelectFlags.Final), o.sortFn, o.ast),
              q2.where
            )
          case _ => (this, Seq())
        SelectAllQuery(Seq(newQ1, newQ2), whereQ1 ++ whereQ2, None, astOther)
      case r: RelationOp =>
        // default to subquery, some ops may want to override
        SelectAllQuery(Seq(this, r), Seq(), None, astOther)

  override def appendFlag(f: SelectFlags): RelationOp =
    f match
      case SelectFlags.Distinct =>
        query.appendFlag(f)
      case _ =>
        flags = flags + f
    this

  override def computeSQL(using d: Dialect)(using cnf: Config)(ctx: SQLRenderingContext): Unit =
    wrapStringStart(ctx)
    query.computeSQL(ctx)
    ctx.sql.append(" ORDER BY ")
    var first = true
    for s <- sortFn do
      if first then first = false else ctx.sql.append(", ")
      s._1 match // NOTE: special case orderBy alias since for now, don't bother prefixing, TODO: which prefix to use for multi-relation select?
        case v: SelectExpr =>
          ctx.sql.append(v.attrName)
        case o =>
          o.computeSQL(ctx)
      ctx.sql.append(" ")
      ctx.sql.append(s._2.toString)
    wrapStringEnd(ctx)

/**
 * N-ary relation-level operation
 */
case class NaryRelationOp(children: Seq[QueryIRNode], op: String, ast: DatabaseAST[?]) extends RelationOp:
  val latestVar = s"subquery${QueryIRTree.idCount}"
  QueryIRTree.idCount += 1
  override def alias = latestVar

  override def appendWhere(w: WhereClause, astOther: DatabaseAST[?]): RelationOp =
    SelectAllQuery(
      Seq(this),
      Seq(w),
      Some(alias),
      astOther
    )

  override def appendProject(p: QueryIRNode, astOther: DatabaseAST[?]): RelationOp =
    SelectQuery(
      p,
      Seq(this),
      Seq(),
      Some(alias),
      astOther
    )

  override def mergeWith(r: RelationOp, astOther: DatabaseAST[?]): RelationOp =
    r match
      case t:(TableLeaf  | RecursiveIRVar) =>
        SelectAllQuery(
          Seq(this, r),
          Seq(),
          Some(alias),
          astOther
        )
      case q: SelectAllQuery =>
        SelectAllQuery(
          this +: q.from,
          q.where,
          Some(q.alias),
          astOther
        )
      case q: SelectQuery =>
        SelectQuery(
          q.project,
          this +: q.from,
          q.where,
          Some(q.alias),
          astOther
        )
      case r: RelationOp =>
        // default to subquery, some ops may want to override
        SelectAllQuery(Seq(this, r), Seq(), None, astOther)

  override def appendFlag(f: SelectFlags): RelationOp =
    f match
      case SelectFlags.Distinct =>
        SelectAllQuery(Seq(this), Seq(), Some(alias), ast).appendFlag(f)
      case _ =>
        flags = flags + f
        this

  override def computeSQL(using d: Dialect)(using cnf: Config)(ctx: SQLRenderingContext): Unit =
    wrapStringStart(ctx)
    ctx.mkString(children, s" $op ")
    wrapStringEnd(ctx)

case class MultiRecursiveRelationOp(aliases: Seq[String],
                                    query: Seq[RelationOp],
                                    finalQ: RelationOp,
                                    override val carriedSymbols: List[(String, RecursiveIRVar)],
                                    ast: DatabaseAST[?]) extends RelationOp:
  val alias = finalQ.alias
  override def appendWhere(w: WhereClause, astOther: DatabaseAST[?]): RelationOp =
    MultiRecursiveRelationOp(
      aliases, query, finalQ.appendWhere(w, astOther), carriedSymbols, ast
    ).appendFlags(flags)

  override def appendProject(p: QueryIRNode, astOther: DatabaseAST[?]): RelationOp =
    MultiRecursiveRelationOp(
      aliases, query, finalQ.appendProject(p, astOther), carriedSymbols, ast
    ).appendFlags(flags)

  override def mergeWith(r: RelationOp, astOther: DatabaseAST[?]): RelationOp =
    MultiRecursiveRelationOp(
      aliases, query, finalQ.mergeWith(r, astOther).appendFlag(Final), carriedSymbols, ast
    ).appendFlags(flags)

  override def appendFlag(f: SelectFlags): RelationOp =
    f match
      case SelectFlags.Distinct =>
        finalQ.appendFlag(f)
      case _ =>
        flags = flags + f
    this

  override def computeSQL(using d: Dialect)(using cnf: Config)(ctx: SQLRenderingContext): Unit =
    // NOTE: no parens or alias needed, since already defined
    ctx.sql.append("WITH RECURSIVE ")
    var first = true
    for (a, q) <- aliases.zip(query) do
      if first then first = false else ctx.sql.append(",\n")
      ctx.sql.append(a)
      ctx.sql.append(s" AS (")
      q.computeSQL(ctx)
      ctx.sql.append(")")
    ctx.sql.append("\n ")
    finalQ.computeSQL(ctx)

/**
 * A recursive variable that points to a table or subquery.
 */
case class RecursiveIRVar(pointsToAlias: String, alias: String, ast: DatabaseAST[?]) extends RelationOp:
  override def computeSQL(using d: Dialect)(using cnf: Config)(ctx: SQLRenderingContext): Unit =
    ctx.sql.append(pointsToAlias)
    ctx.sql.append(" as ")
    ctx.sql.append(alias)
  override def toString: String = s"RVAR($alias->$pointsToAlias)"

  // TODO: for now reuse TableOp's methods
  override def mergeWith(r: RelationOp, astOther: DatabaseAST[?]): RelationOp =
    r match
      case t: (TableLeaf  | RecursiveIRVar) =>
        SelectAllQuery(Seq(this, r), Seq(), None, astOther)
      case q: SelectAllQuery =>
        SelectAllQuery(this +: q.from, q.where, None, astOther)
      case q: SelectQuery =>
        SelectQuery(
          q.project,
          this +: q.from,
          q.where,
          None,
          astOther
        )
      case r: RelationOp =>
        // default to subquery, some ops may want to override
        SelectAllQuery(Seq(this, r), Seq(), None, astOther)

  override def appendWhere(w: WhereClause, astOther: DatabaseAST[?]): RelationOp =
    SelectAllQuery(Seq(this), Seq(w), Some(alias), astOther)

  override def appendProject(p: QueryIRNode, astOther: DatabaseAST[?]): RelationOp =
    SelectQuery(p, Seq(this), Seq(), None, astOther)

  override def appendFlag(f: SelectFlags): RelationOp =
    val q = f match
      case SelectFlags.Distinct => // Distinct is special case because needs to be "hoisted" to enclosing SELECT, so alias is kept
        SelectAllQuery(Seq(this), Seq(), Some(alias), ast).appendFlag(f)
      case _ =>
        SelectAllQuery(Seq(this), Seq(), None, ast)

    q.flags = q.flags + f
    q

case class GroupByQuery(
                   source: RelationOp,
                   groupBy: QueryIRNode,
                   having: Option[QueryIRNode],
                   overrideAlias: Option[String],
                   ast: DatabaseAST[?]) extends RelationOp:
  val name = overrideAlias.getOrElse({
    val latestVar = s"subquery${QueryIRTree.idCount}"
    QueryIRTree.idCount += 1
    latestVar
  })
  override def alias = name

  override def mergeWith(r: RelationOp, astOther: DatabaseAST[?]): RelationOp =
    SelectAllQuery(Seq(this, r), Seq(), None, astOther)

  override def appendWhere(w: WhereClause, astOther: DatabaseAST[?]): RelationOp =
    SelectAllQuery(Seq(this), Seq(w), Some(alias), astOther)

  override def appendProject(p: QueryIRNode, astOther: DatabaseAST[?]): RelationOp =
    SelectQuery(p, Seq(this), Seq(), None, astOther)

  override def appendFlag(f: SelectFlags): RelationOp =
    f match
      case SelectFlags.Distinct =>
        source.appendFlag(f)
      case _ =>
        flags = flags + f
    this

  override def computeSQL(using d: Dialect)(using cnf: Config)(ctx: SQLRenderingContext): Unit =
    wrapStringStart(ctx)
    source.computeSQL(ctx)
    ctx.sql.append(" GROUP BY ")
    groupBy.computeSQL(ctx)
    if having.nonEmpty then
      ctx.sql.append(" HAVING ")
      having.get.computeSQL(ctx)
    wrapStringEnd(ctx)
