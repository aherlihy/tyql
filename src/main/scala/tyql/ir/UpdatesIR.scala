package tyql
import tyql._

case class InsertQueryIR[T, Names <: Tuple]
  (table: Table[T], names: List[String], values: Seq[Seq[QueryIRNode]], val ast: UpdateToTheDB) extends QueryIRNode {
  override def computeSQL(using d: Dialect)(using cnf: Config)(ctx: SQLRenderingContext): Unit =
    ctx.sql.append("INSERT INTO ")
    ctx.sql.append(table.$name)
    ctx.sql.append(" (")
    ctx.sql.append(names.map(n => cnf.caseConvention.convert(n)).mkString(", "))
    ctx.sql.append(") VALUES ")
    var first = true
    for row <- values do
      if first then
        first = false
      else
        ctx.sql.append(", ")
      ctx.sql.append("(")
      var first2 = true
      for columnValue <- row do
        if first2 then
          first2 = false
        else
          ctx.sql.append(", ")
        columnValue.computeSQL(ctx)
      ctx.sql.append(")")
}

case class InsertFromSelectQueryIR[T](table: Table[T], query: RelationOp, names: List[String], val ast: UpdateToTheDB)
    extends QueryIRNode {
  override def computeSQL(using d: Dialect)(using cnf: Config)(ctx: SQLRenderingContext): Unit =
    ctx.sql.append("INSERT INTO ")
    ctx.sql.append(table.$name)
    ctx.sql.append(" (")
    ctx.sql.append(names.map(n => cnf.caseConvention.convert(n)).mkString(", "))
    ctx.sql.append(")\n")
    query.appendFlag(SelectFlags.Final)
    query.computeSQL(ctx)
}

case class DeleteQueryIR[T, S <: ExprShape]
  (
      table: Table[T],
      where: QueryIRNode,
      orderBys: Seq[(QueryIRNode, tyql.Ord)],
      limit: Option[QueryIRNode],
      val ast: UpdateToTheDB
  ) extends QueryIRNode {
  override def computeSQL(using d: Dialect)(using cnf: Config)(ctx: SQLRenderingContext): Unit =
    ctx.sql.append("DELETE FROM ")
    ctx.sql.append(table.$name)
    ctx.sql.append(" WHERE ")
    where.computeSQL(ctx)
    if orderBys.nonEmpty then
      ctx.sql.append(" ORDER BY ")
      var first = true
      for (expr, ord) <- orderBys do
        if first then
          first = false
        else
          ctx.sql.append(", ")
        expr.computeSQL(ctx)
        ctx.sql.append(" ")
        ctx.sql.append(ord.toString)
    if limit.isDefined then
      ctx.sql.append(" LIMIT ")
      limit.get.computeSQL(ctx)
}

case class UpdateQueryIR[T, S <: ExprShape]
  (
      table: Table[T],
      setNames: List[String],
      setExprs: Seq[QueryIRNode],
      where: Option[QueryIRNode],
      orderBys: Seq[(QueryIRNode, tyql.Ord)],
      limit: Option[QueryIRNode],
      val ast: UpdateToTheDB
  ) extends QueryIRNode {
  override def computeSQL(using d: Dialect)(using cnf: Config)(ctx: SQLRenderingContext): Unit =
    ctx.sql.append("UPDATE ")
    ctx.sql.append(table.$name)
    ctx.sql.append(" SET ")
    var first = true
    for (name, expr) <- setNames.zip(setExprs) do
      if first then
        first = false
      else
        ctx.sql.append(", ")
      ctx.sql.append(cnf.caseConvention.convert(name))
      ctx.sql.append(" = ")
      expr.computeSQL(ctx)
    if where.isDefined then
      ctx.sql.append(" WHERE ")
      where.get.computeSQL(ctx)
    if orderBys.nonEmpty then
      ctx.sql.append(" ORDER BY ")
      var first = true
      for (expr, ord) <- orderBys do
        if first then
          first = false
        else
          ctx.sql.append(", ")
        expr.computeSQL(ctx)
        ctx.sql.append(" ")
        ctx.sql.append(ord.toString)
    if limit.isDefined then
      ctx.sql.append(" LIMIT ")
      limit.get.computeSQL(ctx)
}
