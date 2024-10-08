package tyql

import scala.annotation.{implicitNotFound, targetName}
import scala.compiletime.ops.int.S
import scala.util.NotGiven

/** The databases we support have some formal versioning schemas, usually semver (https://semver.org/).
 *  The special DB features are sometimes shared between the databases, but also can appear in a disjoint subset of releases of the same DB.
 *
 *  This suggests that we should have two hierarchies, one for the DB versions (`Dialect`) and the other for feature flags (`DialectFeature`)
 *  Compile-time implicit resolution allows us to assign any feature flag to an arbitrary subset of releases.
 *  The internals that need some feature-custom behavior can just statically dispatch on the flag with `using`.
 *
 *  This design is similar to how CMake handles feature flags. It also allows something interesting. If we define these features
 *    meticulously and add some optional expected performance flags, then the set of features will be the only specification of where
 *    a query can run. This would allow automatic switching between backends with compile-time proofs of full compatibility
 *    (of course as long as we make sure all the relevant constraints are in the feature flag set, or maybe allow end-users to define their own flags?).
 */


/** Type class that only allows integers Start..Stop */
infix type to[Start <: Int, Stop <: Int] = Tuple.Union[InclusiveRange[Start, Stop]]
private type InclusiveRange[Start <: Int, Stop <: Int] = InclusiveRangeImpl[Start, Stop, Start]
private type InclusiveRangeImpl[Start <: Int, Stop <: Int, Now <: Int] <: Tuple = Now match
  case S[Stop] => EmptyTuple
  case _ => Now *: InclusiveRangeImpl[Start, Stop, S[Now]]

// TODO no idea how to use <, <=, >=, > from `scala.compiletime.ops.int`, so I must implement this using implicit resolution
trait `<=`[A <: Int, B <: Int]
given [A <: Int]: `<=`[A, A] with {}
given [A <: Int, B <: Int](using `<=`[A, B]): `<=`[A, S[B]] with {}
// TODO define <= on semver tuples so we can do (3,2) <= (3,2,10) for nicer expressions

// TODO it's unclear how different inline `to` is from the inline `<=`.
//   Maybe both of these checks should be done using implicit resolution?
//   Now one check appears in the type parametrization and the other in the implicit parameter list.
//   Having all of them be implicit, I could probably write nicer expressions like (3,2) <= (A,B), but I could not use `&` and `|`...

type NonNegative[A <: Int] = 0 <= A

sealed trait Dialect
sealed class SQLite[A <: Int, B <: Int, C <: Int](using NonNegative[A], NonNegative[B], NonNegative[C]) extends Dialect // these NonNegative constraints seem to slow down the initial complication, unclear why, most of the numbers are very small
sealed class DuckDB[A <: Int, B <: Int, C <: Int](using NonNegative[A], NonNegative[B], NonNegative[C]) extends Dialect
sealed class PostgreSQL[A <: Int, B <: Int, C <: Int](using NonNegative[A], NonNegative[B], NonNegative[C]) extends Dialect

sealed trait DialectFeature
trait ExtendedMathFeature extends DialectFeature:
    def log(): Double

given [A <: 3, B <: Int & (13 `to` 14) | (19 `to` 30)](using dialect: SQLite[A, B, ?]): ExtendedMathFeature= new ExtendedMathFeature:
    override def log(): Double = 3.0
// You can easily do `|` in the list of constraints of a single type-number, but the following needs a @targetName due to double definition
@targetName("someOtherSQLite")
given [A <: 2, B <: 10](using dialect: SQLite[A, B, ?]): ExtendedMathFeature = new ExtendedMathFeature:
    override def log(): Double = 3.0
// TODO ?!?!?! the order SQLite[A, ?, ?], (7 `MyLte` A) vas (7 `MyLte` A), SQLite[A, ?, ?] is the difference between this working or not!
@targetName("someOtherSQLite2")
given [A <: Int](using SQLite[A, ?, ?], 7 <= A): ExtendedMathFeature = new ExtendedMathFeature:
  override def log(): Double = 3.0

// Practical use of the feature flags:
class Thing:
  def commonToAll(): Unit = println("commonToAll")
  def onlyExtendedMath()(using e: ExtendedMathFeature): Unit = println(e.log())
  // This is how you require that a feature flag is NOT present
  def onlySimpleMath()(using NotGiven[ExtendedMathFeature]): Unit = println("I cannot do logarithms")

extension (t: Thing)(using SQLite[?, ?, ?])
  def onlySqlite(): Unit = println("onlySqlite")


// Here's how to define a feature flag for a subset of supported databases
sealed trait OLTPDatabase extends DialectFeature
given (using SQLite[?, ?, ?] ): OLTPDatabase = new OLTPDatabase {}
given (using PostgreSQL[?, ?, ?] ): OLTPDatabase = new OLTPDatabase {}


// Verbose but exact control, this means "OLTPDatabase & ~(3,12,21)"
private sealed trait thatOneSpecialRelease extends DialectFeature
given (using SQLite[3,13,21]): thatOneSpecialRelease = new thatOneSpecialRelease {}
sealed trait someSpecialBehavior extends DialectFeature
given (using OLTPDatabase, NotGiven[thatOneSpecialRelease]): someSpecialBehavior = new someSpecialBehavior {}

extension (t: Thing)(using someSpecialBehavior)
  def processTransaction(): Unit = println("I can transact")

@main def dialectsPlayground(): Unit =
  given SQLite[3, 13, 22]()
  val t = Thing()
  t.commonToAll()
  t.onlySqlite()
  t.onlyExtendedMath()
//    t.onlySimpleMath()
  t.processTransaction()
