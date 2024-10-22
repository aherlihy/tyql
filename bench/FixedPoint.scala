package tyql.bench
import scala.collection.mutable

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
}
