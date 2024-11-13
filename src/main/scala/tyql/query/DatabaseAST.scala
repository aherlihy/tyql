package tyql

/** Shared supertype of query and aggregation
  * @tparam Result
  */
trait DatabaseAST[Result](using val qTag: ResultTag[Result]):
  def toSQLString()(using d: Dialect)(using cnf: Config): String = toQueryIR.toSQLString()

  private var cached: java.util.concurrent.ConcurrentHashMap[Dialect, QueryIRNode] = null

  def toQueryIR(using d: Dialect): QueryIRNode =
    if cached == null then
      this.synchronized {
        if cached == null then
          cached = new java.util.concurrent.ConcurrentHashMap[Dialect, QueryIRNode]()
      }
    val q = cached.computeIfAbsent(
      d,
      dd => QueryIRTree.generateFullQuery(this, SymbolTable())(using dd)
    )
    q
