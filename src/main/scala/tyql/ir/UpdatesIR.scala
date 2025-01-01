package tyql.ir
import tyql._

case class InsertQueryIR[T, Names <: Tuple](table: Table[T], names: List[String], values: Seq[Seq[QueryIRNode]], val ast: UpdateToTheDB) extends QueryIRNode {
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
