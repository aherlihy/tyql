package tyql
package dialects

import scala.annotation.implicitNotFound

// POC of the DialectFeature gating mechanism.  The full set of marker traits
// lives on the `backend-specialization` branch.

/** Base trait for capability flags that a Dialect may or may not provide.
 *  Methods that require a given capability take a `(using DialectFeature.X)`
 *  parameter, which produces a compile-time error pointing at the missing
 *  capability when a user imports a dialect that does not support it. */
trait DialectFeature

object DialectFeature:

  @implicitNotFound(
    "This dialect does not support INSERT ... SELECT. " +
      "Import a dialect that does, e.g. `import tyql.dialects.postgresql.given`."
  )
  trait Insertable extends DialectFeature

  /** Keyed recursion (DuckDB's `WITH RECURSIVE ... USING KEY`), which makes the
   *  full accumulated relation available to the recursive case via the
   *  `recurring.` pseudo-table and is what lets a NON-LINEAR recursive query
   *  evaluate correctly. Required only when a recursive query is actually
   *  non-linear. */
  @implicitNotFound(
    "This backend cannot run a non-linear recursive query: it has no keyed recursion " +
      "(`USING KEY`). Either make the recursion linear, or target a backend that supports " +
      "it, e.g. `import tyql.dialects.duckdb.given`."
  )
  trait KeyedRecursion extends DialectFeature

  /** A `CYCLE` clause that bounds an otherwise non-terminating BAG-semantic
   *  recursive query (PostgreSQL/Oracle). Required only when a recursive query
   *  is actually bag-semantic. */
  @implicitNotFound(
    "This backend cannot run a bag-semantic recursive query safely: it has no `CYCLE` clause. " +
      "Either use set semantics (`.distinct`), or target a backend that supports it, " +
      "e.g. `import tyql.dialects.postgresql.given`."
  )
  trait CycleClause extends DialectFeature

  /** Aggregation inside the recursive step (DuckDB, research engines). The SQL
   *  standard prohibits it and most engines reject it. Required only when a
   *  recursive query actually aggregates in its recursive case. */
  @implicitNotFound(
    "This backend rejects aggregation inside a recursive query. " +
      "Target a backend that permits it, e.g. `import tyql.dialects.duckdb.given`."
  )
  trait RecursiveAggregation extends DialectFeature
