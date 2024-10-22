package tyql.bench
import scala.collection.mutable
import scalasql.{Table as ScalaSQLTable}

type constant = String | Int | Double
object FixedPointQuery {
  val database = mutable.Map[String, Seq[constant]]()
  @annotation.tailrec
  final def fix[P](bases: Seq[P], acc: Seq[P])(fns: Seq[P] => Seq[P]): Seq[P] =
    val next = fns(bases)
    if (next == bases)
      acc ++ bases
    else
      fix(next, acc ++ bases)(fns)

  final def dbFix[P[_[_]]]
    (bases: ScalaSQLTable[P], acc: ScalaSQLTable[P])
    (fns: () => Unit)
    (cmp: () => Boolean)
    (init: () => Unit)
  : ScalaSQLTable[P] =

    fns()

    val isEmpty = cmp()
    if (isEmpty)
      acc
    else
      init()
      dbFix(bases, acc)(fns)(cmp)(init)
}
