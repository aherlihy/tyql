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
