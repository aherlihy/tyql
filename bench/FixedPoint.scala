package tyql.bench
import scala.collection.mutable
import scalasql.{Table as ScalaSQLTable, DbApi, query, Expr}
import scalasql.dialects.PostgresDialect.*

type constant = String | Int | Double
object FixedPointQuery {
  val database = mutable.Map[String, Seq[constant]]()
  @annotation.tailrec
  final def fix[P](set: Boolean)(bases: Seq[P], acc: Seq[P])(fns: Seq[P] => Seq[P]): Seq[P] =
    val next = fns(bases)
    if (next.toSet.subsetOf(acc.toSet))
      if (set) then (acc ++ bases).distinct else acc ++ bases
    else
      val res = if (set) then (acc ++ bases).distinct else acc ++ bases
      fix(set)(next, res)(fns)

  @annotation.tailrec
  final def multiFix[T <: Tuple, S <: Seq[?]](set: Boolean, targetIdx: Int = 0)(bases: T, acc: T)(fns: (T, T) => T): T =
    val next = fns(bases, acc)

    val nextA = next.toList.asInstanceOf[List[S]]
    val basesA = bases.toList.asInstanceOf[List[S]]
    val accA = acc.toList.asInstanceOf[List[S]]


    val cmp = nextA.zip(accA).map(
      (n, a) => n.toSet.subsetOf(a.toSet)
    ).forall(b => b)
    if (cmp)
      val combo = accA.zip(basesA).map((a: S, b: S) => if (set) then (a ++ b).distinct else a ++ b)
      val res = Tuple.fromArray(combo.toArray).asInstanceOf[T]
      res
    else
      val combo = accA.zip(basesA).map((a: S, b: S) => if (set) then (a ++ b).distinct else a ++ b)
      val res = Tuple.fromArray(combo.toArray).asInstanceOf[T]
      multiFix(set)(next, res)(fns)

  @annotation.tailrec
  final def scalaSQLFix[P[_[_]]]
    (bases: ScalaSQLTable[P], next: ScalaSQLTable[P], acc: ScalaSQLTable[P])
    (fns: (ScalaSQLTable[P], ScalaSQLTable[P]) => Unit)
    (cmp: (ScalaSQLTable[P], ScalaSQLTable[P]) => Boolean)
    (copyTo: (next: ScalaSQLTable[P], acc: ScalaSQLTable[P]) => ScalaSQLTable[P])
  : ScalaSQLTable[P] =

    fns(bases, next)

    val isEmpty = cmp(next, acc)
    if (isEmpty)
      acc
      copyTo(bases, acc)
    else
      val newNext = copyTo(bases, acc)
      val newBase = next
      scalaSQLFix(newBase, newNext, acc)(fns)(cmp)(copyTo)

  final def scalaSQLSemiNaive[Q, T >: Tuple, P[_[_]]](set: Boolean)
                                                     (db: DbApi, bases_db: ScalaSQLTable[P], next_db: ScalaSQLTable[P], acc_db: ScalaSQLTable[P])
                                                     (toTuple: P[Expr] => Tuple)
                                                     (initBase: () => query.Select[T, Q])
                                                     (initRecur: ScalaSQLTable[P] => query.Select[T, Q])
  : Unit = {

    db.run(bases_db.delete(_ => true))
    db.run(next_db.delete(_ => true))
    db.run(acc_db.delete(_ => true))

    db.run(bases_db.insert.select(
      toTuple,
      initBase()
    ))

    val cmp: (ScalaSQLTable[P], ScalaSQLTable[P]) => Boolean = (next, acc) =>
      val newly = next.select.asInstanceOf[query.Select[T, Q]].except(acc.select.asInstanceOf[query.Select[T, Q]])
      db.run(newly).isEmpty

    val fixFn: (ScalaSQLTable[P], ScalaSQLTable[P]) => Unit = (bases, next) => {
      val query = initRecur(bases)
      db.run(next.delete(_ => true))
      db.run(next.insert.select(
        toTuple,
        query
      ))
    }

    val copyTo: (ScalaSQLTable[P], ScalaSQLTable[P]) => ScalaSQLTable[P] = (bases, acc) => {
      if (set)
        val tmp = s"${ScalaSQLTable.name(bases)}_tmp"
        db.updateRaw(s"CREATE TABLE $tmp AS SELECT * FROM ${ScalaSQLTable.name(bases)} LIMIT 0")
        db.updateRaw(s"INSERT INTO $tmp (SELECT * FROM ${ScalaSQLTable.name(bases)} UNION SELECT * FROM ${ScalaSQLTable.name(acc)})")
        db.run(acc.delete(_ => true))
        db.updateRaw(s"INSERT INTO ${ScalaSQLTable.name(acc)} (SELECT * FROM $tmp)")
        db.updateRaw(s"DROP TABLE $tmp")
      else
        db.updateRaw(s"INSERT INTO ${ScalaSQLTable.name(acc)} (SELECT * FROM ${ScalaSQLTable.name(bases)})")
      bases
    }

    scalaSQLFix(bases_db, next_db, acc_db)(fixFn)(cmp)(copyTo)
  }

  // Mutually recursive relations
  @annotation.tailrec
  final def scalaSQLMultiFix[T]
    (bases: T, next: T, acc: T)
    (fns: (T, T) => Unit)
    (cmp: (T, T) => Boolean)
    (copyTo: (T, T) => T)
  : T = {
//    println(s"----- start multifix ------")
//    println(s"BASE=${ScalaSQLTable.name(bases.asInstanceOf[Tuple2[ScalaSQLTable[?], ScalaSQLTable[?]]]._1)}")
//    println(s"NEXT=${ScalaSQLTable.name(next.asInstanceOf[Tuple2[ScalaSQLTable[?], ScalaSQLTable[?]]]._1)}")
//    println(s"ACC=${ScalaSQLTable.name(acc.asInstanceOf[Tuple2[ScalaSQLTable[?], ScalaSQLTable[?]]]._1)}")
    fns(bases, next)

    val isEmpty = cmp(next, acc)
    if (isEmpty)
      copyTo(bases, acc)
      acc
    else
      val newNext = copyTo(bases, acc)
      val newBase = next
      scalaSQLMultiFix(newBase, newNext, acc)(fns)(cmp)(copyTo)
  }

  // this is silly but higher kinded types are mega painful to abstract
  final def scalaSQLSemiNaive2[Q1, Q2, T1 >: Tuple, T2 >: Tuple, P1[_[_]], P2[_[_]], Tables]
    (using Tables =:= (ScalaSQLTable[P1], ScalaSQLTable[P2]))
    (set: Boolean)
    (db: DbApi, bases_db: Tables, next_db: Tables, acc_db: Tables)
    (toTuple: (P1[Expr] => Tuple, P2[Expr] => Tuple))
    (initBase: () => (query.Select[T1, Q1], query.Select[T2, Q2]))
    (initRecur: Tables => (query.Select[T1, Q1], query.Select[T2, Q2]))
  : Unit = {

    db.run(bases_db._1.delete(_ => true))
    db.run(next_db._1.delete(_ => true))
    db.run(acc_db._1.delete(_ => true))

    db.run(bases_db._2.delete(_ => true))
    db.run(next_db._2.delete(_ => true))
    db.run(acc_db._2.delete(_ => true))

    val (base1, base2) = initBase()

    db.run(bases_db._1.insert.select(
      toTuple._1,
      base1
    ))
    db.run(bases_db._2.insert.select(
      toTuple._2,
      base2
    ))

    def printTable(t: Tables, name: String): Unit =
      println(s"${name}1(${ScalaSQLTable.name(t._1)})=${db.runRaw[(String)](s"SELECT * FROM ${ScalaSQLTable.name(t._1)}")}")
      println(s"${name}2(${ScalaSQLTable.name(t._2)})=${db.runRaw[(String, Int)](s"SELECT * FROM ${ScalaSQLTable.name(t._2)}")}")


    val cmp: (Tables, Tables) => Boolean = (next, acc) => {
      val (newDelta1, newDelta2) = (
        next._1.select.asInstanceOf[query.Select[T1, Q1]].except(acc._1.select.asInstanceOf[query.Select[T1, Q1]]),
        next._2.select.asInstanceOf[query.Select[T2, Q2]].except(acc._2.select.asInstanceOf[query.Select[T2, Q2]])
      )
      db.run(newDelta1).isEmpty && db.run(newDelta2).isEmpty
    }

    val fixFn: (Tables, Tables) => Unit = (base, next) => {
      val (query1, query2) = initRecur(base)
      db.run(next._1.delete(_ => true))
      db.run(next._2.delete(_ => true))
      db.run(next._1.insert.select(
        toTuple._1,
        query1
      ))
      db.run(next._2.insert.select(
        toTuple._2,
        query2
      ))
    }

    val copyInto: (Tables, Tables) => Tables = (base, acc) => {
      val tmp = (s"${ScalaSQLTable.name(base._1)}_tmp", s"${ScalaSQLTable.name(base._2)}_tmp")
      if (set)
        db.updateRaw(s"CREATE TABLE ${tmp._1} AS SELECT * FROM ${ScalaSQLTable.name(base._1)} LIMIT 0")
        db.updateRaw(s"INSERT INTO ${tmp._1} (SELECT * FROM ${ScalaSQLTable.name(base._1)} UNION SELECT * FROM ${ScalaSQLTable.name(acc._1)})")

        db.updateRaw(s"CREATE TABLE ${tmp._2} AS SELECT * FROM ${ScalaSQLTable.name(base._2)} LIMIT 0")
        db.updateRaw(s"INSERT INTO ${tmp._2} (SELECT * FROM ${ScalaSQLTable.name(base._2)} UNION SELECT * FROM ${ScalaSQLTable.name(acc._2)})")

        db.run(acc._1.delete(_ => true))
        db.run(acc._2.delete(_ => true))
        db.updateRaw(s"INSERT INTO ${ScalaSQLTable.name(acc._1)} (SELECT * FROM ${tmp._1})")
        db.updateRaw(s"INSERT INTO ${ScalaSQLTable.name(acc._2)} (SELECT * FROM ${tmp._2})")
        db.updateRaw(s"DROP TABLE ${tmp._1}")
        db.updateRaw(s"DROP TABLE ${tmp._2}")

      else
        db.updateRaw(s"INSERT INTO ${ScalaSQLTable.name(acc._1)} (SELECT * FROM ${ScalaSQLTable.name(base._1)})")
        db.updateRaw(s"INSERT INTO ${ScalaSQLTable.name(acc._2)} (SELECT * FROM ${ScalaSQLTable.name(base._2)})")
      base
    }

    scalaSQLMultiFix(bases_db, next_db, acc_db)(fixFn)(cmp)(copyInto)
  }

}
