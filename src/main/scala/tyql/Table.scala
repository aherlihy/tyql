package tyql

/** The type of query references to database tables, TODO: put driver stuff here? */
case class Table[R]($name: String) extends Query[R]

// case class Database(tables: ) // need seq of tables