package tyql

/** The type of query references to database tables, TODO: put driver stuff here? */
case class Table[R]($name: String)(using ResultTag[R]) extends Query[R, BagResult]

// case class Database(tables: ) // need seq of tables
