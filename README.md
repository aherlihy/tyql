# Tyql

A Scala3 SQL query generator
- based on [named tuples](https://scala-lang.org/api/3.x/docs/docs/reference/experimental/named-tuples.html) and not macros nor higher-kinded types,
- checks query correctness at compile-time against selected backend,
- generates SQL at runtime,
- guides the user with nice error messages,
- is usable (feature coverage, speed).


### How do i use it?
First, import a dialect (`postgres`, `mysql`, `mariadb`, `duckdb`, `sqlite`, `h2`) like this
```scala
import tyql.Dialect.postgres.given
```
Then define your tables as case classes or named tuples
```scala
case class Person(id: Long, name: String)
val persons = Table[Person]()
val orders = Table[(orderid: Long, person: Long, notes: Option[String])]("order")
```
Compose queries like this
```scala
val q = for (p <- persons ; if p.id > 10L ; o <- orders.joinOn(o => p.id == o.person))
  yield (name = p.name, orderNumber = o.orderid, specialNote = notes.getOrElse("NONE"))
```
You will sometimes have to wrap literals in `lit`, especially booleans: `lit(true)`.

You can then examine the SQL
```scala
println(q.toSQLString())
```
or run it against the database and receive Scala-native data structures back
```
val conn = java.sql.DriverManager.getConnection("jdbc:postgresql://localhost:5433/testdb", "testuser", "testpass")
val db = tyql.DB(conn)
db.run(q)
/* List(
*   (name = "Adam", orderNumber = 12L, specialNote = "NONE"),
*   (name = "Eva", orderNumber = 3L, specialNote = "2nd floor")
* )
*/
```

#### How do I configure it?
```scala
given tyql.Config = new tyql.Config(tyql.CaseConvention.Underscores, tyql.ParameterStyle.EscapedInline) {}
```
For case convention (how will the Scala identifiers be translated into SQL) you can pick
```scala
enum CaseConvention:
  case Exact
  case Underscores // three_letter_word
  case PascalCase // ThreeLetterWord
  case CamelCase // threeLetterWord
  case CapitalUnderscores // THREE_LETTER_WORD
  case Joined // threeletterword
  case JoinedCapital // THREELETTERWORD
```
For parameter style you can pick `EscapedInline` (literals will be pasted inside the SQL) or `DriverParametrized` (the SQL will be `?`-parametrized and the JDBC will be provided with the values).

### How fast is it in practice?
We benchmarked against Quill (a prominent macro-based query generator) on a local MySQL instance.
Quill computes and renders the query entirely at compile-time, we therefore compare to Tyql with caching enabled.
We therefore are comparing the performance of the driver and fetching from cache.
In our tests
* time it takes to fetch 100k rows is almost identical (87-89µs),
* time it takes for a round trip of a small query is almost identical (95-97µs).

Tyql generates queries usually in between 7µs and 25µs (depending on query complexity).

### What about caching queries with changing inputs?
You can use `Var(thunk: => T)`. If you're using `?`-parametrization, the value will be fetched only inside `DB.run`, that is, when the parameters need to be passed to JDBC.
```scala
val q = persons.filter(p => p.age >= Var(getAge()))
db.run(q)
```
The query will be computed and rendered only once, no matter how many times it is run with different parameters.

### What about transactions and other driver-specific functionality?
We do not replace JDBC, only wrap its `Connection` with oue `DB`.
```scala
try {
  conn.setAutoCommit(false)
  val got = db.run(q1)
  // ...
  conn.commit()
} catch { case e: Exception =>
  conn.rollback()
} finally {
  conn.setAutoCommit(true)
  conn.close()
}
```
