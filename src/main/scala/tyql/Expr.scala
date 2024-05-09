package tyql

import language.experimental.namedTuples
import NamedTuple.{NamedTuple, AnyNamedTuple}

/** The type of expressions in the query language */
trait Expr[Result] extends Selectable:
  /** This type is used to support selection with any of the field names
   *  defined by Fields.
   */
  type Fields = NamedTuple.Map[NamedTuple.From[Result], Expr]

 /** A selection of a field name defined by Fields is implemented by `selectDynamic`.
   *  The implementation will add a cast to the right Expr type corresponding
   *  to the field type.
   */
  def selectDynamic(fieldName: String) = Expr.Select(this, fieldName)

  /** Member methods to implement universal equality on Expr level. */
  def == (other: Expr[?]): Expr[Boolean] = Expr.Eq(this, other)
  def != (other: Expr[?]): Expr[Boolean] = Expr.Ne(this, other)

object Expr:

  /** Sample extension methods for individual types */
  extension (x: Expr[Int])
    def > (y: Expr[Int]): Expr[Boolean] = Gt(x, y)
    def > (y: Int): Expr[Boolean] = Gt(x, IntLit(y)) // TODO: shouldn't the implicit conversion handle this?
  extension (x: Expr[Boolean])
    def &&(y: Expr[Boolean]): Expr[Boolean] = And(x, y)
    def || (y: Expr[Boolean]): Expr[Boolean] = Or(x, y)

  // Note: All field names of constructors in the query language are prefixed with `$`
  // so that we don't accidentally pick a field name of a constructor class where we want
  // a name in the domain model instead.

  // Some sample constructors for Exprs
  case class Gt($x: Expr[Int], $y: Expr[Int]) extends Expr[Boolean]
  case class Plus(x: Expr[Int], y: Expr[Int]) extends Expr[Int]
  case class And($x: Expr[Boolean], $y: Expr[Boolean]) extends Expr[Boolean]
  case class Or($x: Expr[Boolean], $y: Expr[Boolean]) extends Expr[Boolean]

  // So far Select is weakly typed, so `selectDynamic` is easy to implement.
  // Todo: Make it strongly typed like the other cases
  case class Select[A]($x: Expr[A], $name: String) extends Expr

  case class Single[S <: String, A]($x: Expr[A]) extends Expr[NamedTuple[S *: EmptyTuple, A *: EmptyTuple]]

  case class Concat[A <: AnyNamedTuple, B <: AnyNamedTuple]($x: Expr[A], $y: Expr[B]) extends Expr[NamedTuple.Concat[A, B]]

  case class Project[A <: AnyNamedTuple]($a: A) extends Expr[NamedTuple.Map[A, StripExpr]]

  type StripExpr[E] = E match
    case Expr[b] => b
    // case _ => E // TODO: verify this won't backfire

  // Also weakly typed in the arguents since these two classes model universal equality */
  case class Eq($x: Expr[?], $y: Expr[?]) extends Expr[Boolean]
  case class Ne($x: Expr[?], $y: Expr[?]) extends Expr[Boolean]

  /** References are placeholders for parameters */
  private var refCount = 0

  case class Ref[A]($name: String = "") extends Expr[A]:
    private val id = refCount
    refCount += 1
    override def toString = s"ref$id(${$name})"

  /** Literals are type-specific, tailored to the types that the DB supports */
  case class IntLit($value: Int) extends Expr[Int]
  /** Scala values can be lifted into literals by conversions */
  given Conversion[Int, IntLit] = IntLit(_)

  case class StringLit($value: String) extends Expr[String]
  given Conversion[String, StringLit] = StringLit(_)

  /** The internal representation of a function `A => B`
   *  Query languages are ususally first-order, so Fun is not an Expr
   */
  case class Fun[A, B]($param: Ref[A], $f: B)

  type Pred[A] = Fun[A, Expr[Boolean]]

  /** Explicit conversion from
   *      (name_1: Expr[T_1], ..., name_n: Expr[T_n])
   *  to
   *      Expr[(name_1: T_1, ..., name_n: T_n)]
   */
  extension [A <: AnyNamedTuple](x: A) def toRow: Project[A] = Project(x)

  /** Same as _.toRow, as an implicit conversion */
  given [A <: AnyNamedTuple]: Conversion[A, Expr.Project[A]] = Expr.Project(_)

end Expr
