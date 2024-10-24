package tyql.bench
import scala.collection.mutable
import scalasql.{Table as ScalaSQLTable, DbApi, query, Expr}
import scalasql.dialects.PostgresDialect.*

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

  final def scalaSQLFix[P[_[_]]]
    (bases: ScalaSQLTable[P], acc: ScalaSQLTable[P])
    (fns: ScalaSQLTable[P] => ScalaSQLTable[P])
    (cmp: ScalaSQLTable[P] => Boolean)
    (init: () => Unit)
  : ScalaSQLTable[P] =

    val delta = fns(bases)

    val isEmpty = cmp(delta)
    if (isEmpty)
      acc
    else
      init()
      scalaSQLFix(bases, acc)(fns)(cmp)(init)

//   final def semiNaive[P[_[_]]](db: DbApi,
//                                delta: ScalaSQLTable[P],
//                                derived: ScalaSQLTable[P],
//                                tmp: ScalaSQLTable[P])
//                               (initBase: ScalaSQLTable[P] => Unit)
//                               (initRecur: ScalaSQLTable[P] => ScalaSQLTable[P])
//                               (resetIter: () => Unit)
//   : Unit = { //ScalaSQLTable[P] =
//
//     db.run(delta.delete(_ => true))
//     db.run(derived.delete(_ => true))
//     db.run(tmp.delete(_ => true))
//
//     initBase(delta)
//     initBase(derived)
//
//     val cmp: ScalaSQLTable[P] => Boolean = delta => db.run(delta.select).isEmpty
//
//     tableFix(delta, derived)(initRecur)(cmp)(resetIter)
//     resetIter()
//   }

  final def scalaSQLSemiNaive[Q, T >: Tuple, P[_[_]]](db: DbApi,
                                                      delta: ScalaSQLTable[P],
                                                      derived: ScalaSQLTable[P],
                                                      tmp: ScalaSQLTable[P])
                                                     (toTuple: P[Expr] => Tuple)
                                                     (initBase: () => query.Select[T, Q])
                                                     (initRecur: ScalaSQLTable[P] => query.Select[T, Q])
  : Unit = { //ScalaSQLTable[P] =

    db.run(delta.delete(_ => true))
    db.run(derived.delete(_ => true))
    db.run(tmp.delete(_ => true))

    db.run(delta.insert.select(
      toTuple,
      initBase()
    ))
    db.run(derived.insert.select(
      toTuple,
      initBase()
    ))

    val cmp: ScalaSQLTable[P] => Boolean = delta => db.run(delta.select).isEmpty

    val fixFn: ScalaSQLTable[P] => ScalaSQLTable[P] = recur => {
      val query = initRecur(recur)

      db.run(tmp.delete(_ => true))
      db.run(tmp.insert.select( // need tmp because reads from delta
        toTuple,
        query
      ))
      db.run(delta.delete(_ => true))
      db.updateRaw(s"INSERT INTO ${ScalaSQLTable.name(delta)} (SELECT * FROM ${ScalaSQLTable.name(tmp)})")
      delta
    }

    val init: () => Unit = () => {
      db.updateRaw(s"INSERT INTO ${ScalaSQLTable.name(derived)} (SELECT * FROM ${ScalaSQLTable.name(delta)})")
    }

    scalaSQLFix(delta, derived)(fixFn)(cmp)(init)
    init()
  }

}
