package tyql

/**
 * Modifiers for query generation, e.g. queries at the expression level need surrounding parens.
 */
enum SelectFlags:
  case Distinct   // used by select queries
  case FinalLevel // top-level final result, e.g. avoid extra parens or aliasing
  case ExprLevel  // expression-level relation operation

type SymbolTable = Map[String, RelationOp]

/**
 * Relation-level operations, e.g. a table, union of tables, SELECT query, etc.
 */
trait RelationOp extends QueryIRNode:
  var flags: Set[SelectFlags] = Set.empty
  def alias: String
  // TODO: decide if we want to mutate, or copy + discard IR nodes. Right now its a mix
  def appendWhere(w: Seq[QueryIRNode], astOther: DatabaseAST[?]): RelationOp
  def appendProject(p: QueryIRNode, astOther: DatabaseAST[?]): RelationOp
  def appendSubquery(q: SelectQuery, astOther: DatabaseAST[?]): RelationOp
  def appendFlag(f: SelectFlags): RelationOp

/**
 * Simple table read.
 */
case class TableLeaf(tableName: String, ast: Table[?]) extends RelationOp with QueryIRLeaf:
  val name = s"$tableName${QueryIRTree.idCount}"
  QueryIRTree.idCount += 1
  override def alias = name
  override def toSQLString(): String = s"$tableName as $name"

  override def toString: String = s"TableLeaf($tableName as $name)"

  override def appendWhere(w: Seq[QueryIRNode], astOther: DatabaseAST[?]): RelationOp =
    SelectQuery(SelectAllExpr(), Seq(this), w, Some(alias), astOther)

  override def appendProject(p: QueryIRNode, astOther: DatabaseAST[?]): RelationOp =
    SelectQuery(p, Seq(this), Seq(), None, astOther)

  override def appendSubquery(q: SelectQuery, astOther: DatabaseAST[?]): RelationOp =
    SelectQuery(
      q.project,
      this +: q.from,
      q.where,
      None,
      astOther
    )

  override def appendFlag(f: SelectFlags): RelationOp =
    val q = f match
      case SelectFlags.Distinct => // Distinct is special case because needs to be "hoisted" to enclosing SELECT
        SelectQuery(SelectAllExpr(), Seq(this), Seq(), Some(alias), ast)
      case _ =>
        SelectQuery(SelectAllExpr(), Seq(this), Seq(), None, ast)

    q.flags = q.flags + f
    q

/**
 * Select query, e.g. SELECT <Project> FROM <Relations> WHERE <where>
 */
case class SelectQuery(project: QueryIRNode,
                       from: Seq[RelationOp],
                       where: Seq[QueryIRNode],
                       overrideAlias: Option[String],
                       ast: DatabaseAST[?]) extends RelationOp:
  val children = project +: (from ++ where)
  val name = overrideAlias.getOrElse({
    val latestVar = s"subquery${QueryIRTree.idCount}"
    QueryIRTree.idCount += 1
    latestVar
  })
  override def alias = name
  override def appendWhere(w: Seq[QueryIRNode], astOther: DatabaseAST[?]): RelationOp =
    SelectQuery(project, from, where ++ w, Some(alias), astOther)

  // TODO: define semantics of map(f1).map(f2), could collapse into map(f2(f1))?
  override def appendProject(p: QueryIRNode, astOther: DatabaseAST[?]): RelationOp =
    project match
      case s: SelectAllExpr => SelectQuery(p, from, where, None, ast)
      case _ => // Could collapse, for now use subquery
        SelectQuery(p, Seq(this), Seq(), None, astOther)

  /**
   * Merge with another select query to avoid extra nesting
   */
  override def appendSubquery(q: SelectQuery, astOther: DatabaseAST[?]): RelationOp =
    val newP = if (project.isInstanceOf[SelectAllExpr]) // TODO: differentiate between * and no map?
      q.project
    else if (q.project.isInstanceOf[SelectAllExpr])
      project
    else
      // TODO: define semantics of map(f1).map(f2), could collapse into map(f2(f1))?
      throw new Exception("Unimplemented: merging two subqueries with project stmts")

    val newF = from ++ q.from
    val newW = where ++ q.where
    SelectQuery(newP, newF, newW, None, astOther)

  override def appendFlag(f: SelectFlags): RelationOp =
    flags = flags + f
    this

  override def toSQLString(): String =
    val (open, close) = if flags.contains(SelectFlags.FinalLevel) then ("", "") else ("(", ")")
    val aliasStr = if flags.contains(SelectFlags.FinalLevel) || flags.contains(SelectFlags.ExprLevel) then "" else s" as $alias"
    val flagsStr = if flags.contains(SelectFlags.Distinct) then "DISTINCT " else ""
    val projectStr = project.toSQLString()
    val fromStr = from.map(f => f.toSQLString()).mkString("", ", ", "")
    val whereStr = if where.nonEmpty then
      s" WHERE ${if where.size == 1 then where.head.toSQLString() else where.map(f => f.toSQLString()).mkString("(", " AND ", ")")}" else
      ""
    s"${open}SELECT $flagsStr$projectStr FROM $fromStr$whereStr$close$aliasStr"

  override def toString: String = // for debugging
    s"SelectQuery(\n\talias=$alias,\n\tproject=$project,\n\tfrom=$from,\n\twhere=$where\n)"

/**
 * Query with ORDER BY clause
 */
case class OrderedQuery(query: RelationOp, sortFn: Seq[(QueryIRNode, Ord)], ast: DatabaseAST[?]) extends RelationOp:
  override val children: Seq[QueryIRNode] = query +: sortFn.map(_._1)
  override def alias = query.alias

  val orders: Seq[Ord] = sortFn.map(_._2)

  override def appendWhere(w: Seq[QueryIRNode], astOther: DatabaseAST[?]): RelationOp =
    // Does not trigger subquery, e.g. relation.sort(s).filter(f) => SELECT * FROM relation WHERE f ORDER BY s
    OrderedQuery(query.appendWhere(w, astOther), sortFn, ast)

  override def appendProject(p: QueryIRNode, astOther: DatabaseAST[?]): RelationOp =
    // Triggers a subquery, e.g. relation.sort(s).map(m) => SELECT m FROM (SELECT * from relation ORDER BY s).
    // Note relation.map(m).sort(s) => SELECT m FROM relation ORDER BY s
    SelectQuery(
      p,
      Seq(this),
      Seq(),
      None,
      astOther
    )

  override def appendSubquery(q: SelectQuery, astOther: DatabaseAST[?]): RelationOp =
    // Triggers subquery
    SelectQuery(
      SelectAllExpr(),
      Seq(this),
      Seq(),
      None,
      astOther
    )
  override def appendFlag(f: SelectFlags): RelationOp =
    f match
      case SelectFlags.Distinct =>
        query.appendFlag(f)
      case _ =>
        flags = flags + f
    this

  override def toSQLString(): String =
    val (open, close) = if flags.contains(SelectFlags.FinalLevel) then ("", "") else ("(", ")")
    val aliasStr = if flags.contains(SelectFlags.FinalLevel) || flags.contains(SelectFlags.ExprLevel) then "" else s" as $alias"
    s"$open${query.toSQLString()} ORDER BY ${sortFn.map(s =>
      val varStr = s._1 match // NOTE: special case orderBy alias since for now, don't bother prefixing, TODO: which prefix to use for multi-relation select?
        case v: SelectExpr => v.attrName
        case o => o.toSQLString()
      s"$varStr ${s._2.toString}"
      ).mkString("", ", ", "")}$close$aliasStr"

/**
 * Binary relation-level operation, e.g. union
 */
case class BinRelationOp(lhs: RelationOp, rhs: QueryIRNode, op: String, ast: Query[?]) extends RelationOp:
  override val children: Seq[QueryIRNode] = Seq(lhs, rhs)
  val latestVar = s"subquery${QueryIRTree.idCount}"
  QueryIRTree.idCount += 1
  override def alias = latestVar

  override def appendWhere(w: Seq[QueryIRNode], astOther: DatabaseAST[?]): RelationOp =
    SelectQuery(
      SelectAllExpr(),
      Seq(this),
      w,
      None,
      astOther
    )

  override def appendProject(p: QueryIRNode, astOther: DatabaseAST[?]): RelationOp =
    SelectQuery(
      p,
      Seq(this),
      Seq(),
      None,
      astOther
    )

  override def appendSubquery(q: SelectQuery, astOther: DatabaseAST[?]): RelationOp =
    SelectQuery(
      SelectAllExpr(),
      Seq(this),
      Seq(),
      None,
      astOther
    )

  override def appendFlag(f: SelectFlags): RelationOp =
    f match
      case SelectFlags.Distinct =>
        lhs.appendFlag(f)
        rhs match
          case r: RelationOp => r.appendFlag(f)
      case _ =>
        flags = flags + f
    this

  override def toSQLString(): String =
    val (open, close) = if flags.contains(SelectFlags.FinalLevel) then ("", "") else ("(", ")")
    val aliasStr = if flags.contains(SelectFlags.FinalLevel) || flags.contains(SelectFlags.ExprLevel) then "" else s" as $alias"
    s"${lhs.toSQLString()} $op ${rhs.toSQLString()}"

