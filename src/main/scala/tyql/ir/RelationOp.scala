package tyql

/**
 * Modifiers for query generation, e.g. queries at the expression level need surrounding parens.
 */
enum SelectFlags:
  case Distinct   // used by select queries
  case Final      // top-level final result, e.g. avoid extra parens or aliasing
  case ExprLevel  // expression-level relation operation

enum PositionFlag:
  case Project, Where, Subquery

type SymbolTable = Map[String, RelationOp]

/**
 * Relation-level operations, e.g. a table, union of tables, SELECT query, etc.
 */
trait RelationOp extends QueryIRNode:
  var flags: Set[SelectFlags] = Set.empty
  def alias: String

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

  def wrapString(inner: String): String =
    val (open, close) = if flags.contains(SelectFlags.Final) then ("", "") else ("(", ")")
    val aliasStr = if flags.contains(SelectFlags.Final) || flags.contains(SelectFlags.ExprLevel) then "" else s" as $alias"
    s"$open$inner$close$aliasStr"

/**
 * Simple table read.
 */
case class TableLeaf(tableName: String, ast: Table[?]) extends RelationOp with QueryIRLeaf:
  val name = s"$tableName${QueryIRTree.idCount}"
  QueryIRTree.idCount += 1
  override def alias = name
  override def toSQLString(): String =
    if (flags.contains(SelectFlags.Final))
      tableName
    else
      s"$tableName as $name"

  override def toString: String = s"TableLeaf($tableName as $name)"

  override def mergeWith(r: RelationOp, astOther: DatabaseAST[?]): RelationOp =
    r match
      case t: TableLeaf =>
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
      case t: TableLeaf =>
        SelectAllQuery(from :+ t, where, None, astOther)
      case q: SelectAllQuery =>
        val newF = from ++ q.from
        val newW = where ++ q.where
        SelectAllQuery(newF, newW, None, astOther)
      case q: SelectQuery =>
        val newF = from ++ q.from
        val newW = where ++ q.where
        SelectQuery(q.project, newF, newW, None, astOther)
      case r: RelationOp =>
        // default to subquery, some ops may want to override
        SelectAllQuery(Seq(this, r), Seq(), None, astOther)

  override def appendFlag(f: SelectFlags): RelationOp =
    flags = flags + f
    this

  override def toSQLString(): String =
    val flagsStr = if flags.contains(SelectFlags.Distinct) then "DISTINCT " else ""
    val fromStr = from.map(f => f.toSQLString()).mkString("", ", ", "")
    val whereStr = if where.nonEmpty then
      s" WHERE ${if where.size == 1 then where.head.toSQLString() else where.map(f => f.toSQLString()).mkString("(", " AND ", ")")}" else
      ""
    wrapString(
      s"SELECT $flagsStr* FROM $fromStr$whereStr"
    )

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
//    SelectQuery(project, from, where :+ w, Some(alias), astOther).appendFlags(flags)
    // Appending a where clause to something that already has a project triggers subquery
    SelectAllQuery(Seq(this), Seq(w), Some(alias), astOther)

  override def appendProject(p: QueryIRNode, astOther: DatabaseAST[?]): RelationOp =
    // TODO: define semantics of map(f1).map(f2), could collapse into map(f2(f1))? For now just trigger subquery
    SelectQuery(p, Seq(this), Seq(), None, astOther).appendFlags(flags)


  override def mergeWith(r: RelationOp, astOther: DatabaseAST[?]): RelationOp =
    r match
      case t: TableLeaf =>
        SelectAllQuery(Seq(this, t), Seq(), None, astOther)
      case q: SelectAllQuery =>
        SelectAllQuery(this +: q.from, q.where, None, astOther)
      case q: SelectQuery =>
        // TODO: define semantics of map(f1).map(f2), could collapse into map(f2(f1))? For now just trigger subquery
        SelectAllQuery(Seq(this, q), Seq(), None, astOther)
      case r: RelationOp =>
        // default to subquery, some ops may want to override
        SelectAllQuery(Seq(this, r), Seq(), None, astOther)

  override def appendFlag(f: SelectFlags): RelationOp =
    flags = flags + f
    this

  override def toSQLString(): String =
    val flagsStr = if flags.contains(SelectFlags.Distinct) then "DISTINCT " else ""
    val projectStr = project.toSQLString()
    val fromStr = from.map(f => f.toSQLString()).mkString("", ", ", "")
    val whereStr = if where.nonEmpty then
      s" WHERE ${if where.size == 1 then where.head.toSQLString() else where.map(f => f.toSQLString()).mkString("(", " AND ", ")")}" else
      ""
    wrapString(s"SELECT $flagsStr$projectStr FROM $fromStr$whereStr")

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
      case t: TableLeaf =>
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
        // hoist where, TODO: fix unreachable cases
        (query, o.query) match
          case q: (SelectAllQuery, SelectAllQuery) =>
            SelectAllQuery(
              Seq(
                OrderedQuery(SelectAllQuery(q._1.from, Seq(), Some(q._1.alias), q._1.ast).appendFlag(SelectFlags.Final), sortFn, ast),
                OrderedQuery(SelectAllQuery(q._2.from, Seq(), Some(q._2.alias), q._2.ast).appendFlag(SelectFlags.Final), o.sortFn, o.ast)
              ),
              q._1.where ++ q._2.where,
              None,
              astOther
            )
          case q: (RelationOp, SelectAllQuery) =>
            SelectAllQuery(
              Seq(
                this,
                OrderedQuery(SelectAllQuery(q._2.from, Seq(), Some(q._2.alias), q._2.ast).appendFlag(SelectFlags.Final), o.sortFn, o.ast)
              ),
              q._2.where,
              None,
              astOther
            )
          case q: (SelectAllQuery, RelationOp) =>
            SelectAllQuery(
              Seq(
                OrderedQuery(SelectAllQuery(q._1.from, Seq(), Some(q._1.alias), q._1.ast).appendFlag(SelectFlags.Final), sortFn, ast),
                o
              ),
              q._1.where,
              None,
              astOther
            )
          case _ =>
            SelectAllQuery(Seq(this, o), Seq(), None, astOther)
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

  override def toSQLString(): String =
    wrapString(s"${query.toSQLString()} ORDER BY ${sortFn.map(s =>
      val varStr = s._1 match // NOTE: special case orderBy alias since for now, don't bother prefixing, TODO: which prefix to use for multi-relation select?
        case v: SelectExpr => v.attrName
        case o => o.toSQLString()
      s"$varStr ${s._2.toString}"
    ).mkString("", ", ", "")}")

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
      case t: TableLeaf =>
        SelectAllQuery(
          Seq(this, t),
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
    flags = flags + f
    this

  override def toSQLString(): String =
    wrapString(children.map(_.toSQLString()).mkString(s" $op "))

case class MultiRecursiveRelationOp(aliases: Seq[String], query: Seq[RelationOp], finalQ: RelationOp, ast: DatabaseAST[?]) extends RelationOp:
  val alias = finalQ.alias
  override def appendWhere(w: WhereClause, astOther: DatabaseAST[?]): RelationOp =
    MultiRecursiveRelationOp(
      aliases, query, finalQ.appendWhere(w, astOther), ast
    ).appendFlags(flags)

  override def appendProject(p: QueryIRNode, astOther: DatabaseAST[?]): RelationOp =
    MultiRecursiveRelationOp(
      aliases, query, finalQ.appendProject(p, astOther), ast
    ).appendFlags(flags)

  override def mergeWith(r: RelationOp, astOther: DatabaseAST[?]): RelationOp =
    ???

  override def appendFlag(f: SelectFlags): RelationOp =
    flags = flags + f
    this

  override def toSQLString(): String =
    // NOTE: no parens or alias needed, since already defined
    val ctes = aliases.zip(query).map((a, q) => s"$a AS (${q.toSQLString()})").mkString(",\n")
    s"WITH RECURSIVE $ctes\n ${finalQ.toSQLString()}"


case class RecursiveRelationOp(alias: String, query: RelationOp, finalQ: RelationOp, ast: DatabaseAST[?]) extends RelationOp:
  override def appendWhere(w: WhereClause, astOther: DatabaseAST[?]): RelationOp =
    RecursiveRelationOp(
      alias, query, finalQ.appendWhere(w, astOther), ast
    ).appendFlags(flags)

  override def appendProject(p: QueryIRNode, astOther: DatabaseAST[?]): RelationOp =
    RecursiveRelationOp(
      alias, query, finalQ.appendProject(p, astOther), ast
    ).appendFlags(flags)

  override def mergeWith(r: RelationOp, astOther: DatabaseAST[?]): RelationOp =
    r match
      case t: TableLeaf =>
        SelectAllQuery(
          Seq(this, t),
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
    flags = flags + f
    this

  override def toSQLString(): String =
    // NOTE: no parens or alias needed, since already defined
    s"WITH RECURSIVE $alias AS (${query.toSQLString()}); ${finalQ.toSQLString()}"

/**
 * A recursive variable that points to a table or subquery.
 */
case class RecursiveIRVar(pointsToAlias: String, alias: String, ast: DatabaseAST[?]) extends RelationOp:
  override def toSQLString() = s"$pointsToAlias as $alias"
  override def toString: String = s"RVAR($alias->$pointsToAlias)"

  // TODO: for now reuse TableOp's methods
  override def mergeWith(r: RelationOp, astOther: DatabaseAST[?]): RelationOp =
    r match
      case t: TableLeaf =>
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
    SelectAllQuery(Seq(this), Seq(w), Some(alias), astOther)

  override def appendProject(p: QueryIRNode, astOther: DatabaseAST[?]): RelationOp =
    SelectQuery(p, Seq(this), Seq(), None, astOther)

  override def appendFlag(f: SelectFlags): RelationOp =
    val q = f match
      case SelectFlags.Distinct => // Distinct is special case because needs to be "hoisted" to enclosing SELECT, so alias is kept
        SelectAllQuery(Seq(this), Seq(), Some(alias), ast)
      case _ =>
        SelectAllQuery(Seq(this), Seq(), None, ast)

    q.flags = q.flags + f
    q
