package tyql
/**
 * Logical query plan tree
 * Goal: move all type parameters into terms,
 */
trait QueryIRNode:
  val ast: DatabaseAST[?]
  val parent: QueryIRNode
  val children: Seq[QueryIRNode]
  def toSQLString: String

// Root node
case class QueryIRTree(ast: DatabaseAST[?]) extends QueryIRNode:
  val children = ast match
    case aggFlatMap: Aggregation.AggFlatMap[_, _] =>
      Seq(Select(ast, null))
    case flatMap: Query.FlatMap[_, _] =>
      Seq(Select(ast, null))
    case map: Query.Map[_, _] =>
      Seq(Select(ast, null))
    case _ => ???
  override val parent: QueryIRTree = null

  override def toSQLString: String = children.head.toSQLString


case class Select(ast: DatabaseAST[?], override val parent: QueryIRNode) extends QueryIRNode:
  val children = Seq()

  override def toSQLString: String = s"Select[${ast.tag}"