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

  final def multiFix[T <: Tuple, S <: Seq[?]](set: Boolean)(bases: T, acc: T)(fns: T => T): T =
    val next = fns(bases)

    val nextA = next.toList.asInstanceOf[List[S]]
    val basesA = bases.toList.asInstanceOf[List[S]]
    val accA = acc.toList.asInstanceOf[List[S]]


    if (nextA.zip(accA).map(
      (n, a) => n.toSet.subsetOf(a.toSet)
    ).forall(b => b))
      val combo = accA.zip(basesA).map((a: S, b: S) => if (set) then (a ++ b).distinct else a ++ b)
      val res = Tuple.fromArray(combo.toArray).asInstanceOf[T]
      res
    else
      val combo = accA.zip(basesA).map((a: S, b: S) => if (set) then (a ++ b).distinct else a ++ b)
      val res = Tuple.fromArray(combo.toArray).asInstanceOf[T]
      multiFix(set)(next, res)(fns)

  final def scalaSQLFix[P[_[_]]]
    (bases: ScalaSQLTable[P], acc: ScalaSQLTable[P])
    (fns: ScalaSQLTable[P] => ScalaSQLTable[P])
    (cmp: (ScalaSQLTable[P], ScalaSQLTable[P]) => Boolean)
    (init: () => Unit)
  : ScalaSQLTable[P] =

    val delta = fns(bases)

    val isEmpty = cmp(delta, acc)
    if (isEmpty)
      acc
    else
      init()
      scalaSQLFix(bases, acc)(fns)(cmp)(init)

  final def scalaSQLSemiNaive[Q, T >: Tuple, P[_[_]]](set: Boolean)
                                                     (db: DbApi, delta: ScalaSQLTable[P], derived: ScalaSQLTable[P], tmp: ScalaSQLTable[P])
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

    val cmp: (ScalaSQLTable[P], ScalaSQLTable[P]) => Boolean = (currentDelta, acc) =>
      val newly = currentDelta.select.asInstanceOf[query.Select[T, Q]].except(acc.select.asInstanceOf[query.Select[T, Q]])
      db.run(newly).isEmpty

    val fixFn: ScalaSQLTable[P] => ScalaSQLTable[P] = recur => {
      val query = initRecur(recur)

      db.run(tmp.delete(_ => true))
      db.run(tmp.insert.select( // need tmp because reads from delta
        toTuple,
        query
      ))
      db.run(delta.delete(_ => true))
      val setStr = if (set) then " DISTINCT" else ""
      db.updateRaw(s"INSERT INTO ${ScalaSQLTable.name(delta)} (SELECT$setStr * FROM ${ScalaSQLTable.name(tmp)})")
      delta
    }

    val init: () => Unit = () => {
      if (set)
        db.run(tmp.delete(_ => true))
        db.updateRaw(s"INSERT INTO ${ScalaSQLTable.name(tmp)} (SELECT * FROM ${ScalaSQLTable.name(delta)} UNION SELECT * FROM ${ScalaSQLTable.name(derived)})")
        db.run(derived.delete(_ => true))
        db.updateRaw(s"INSERT INTO ${ScalaSQLTable.name(derived)} (SELECT * FROM ${ScalaSQLTable.name(tmp)})")
      else
        db.updateRaw(s"INSERT INTO ${ScalaSQLTable.name(derived)} (SELECT * FROM ${ScalaSQLTable.name(delta)})")
    }

    scalaSQLFix(delta, derived)(fixFn)(cmp)(init)
    init()
  }

  // Mutually recursive relations
  final def scalaSQLMultiFix[T](bases: T, acc: T, tmp: T)(fns: (T, T) => (T, T))(cmp: (T, T) => Boolean)(copyInto: (T, T, T) => T): T = {
//    println(s"BASE=${ScalaSQLTable.name(bases.asInstanceOf[Tuple2[ScalaSQLTable[?], ScalaSQLTable[?]]]._1)}")
//    println(s"TMP=${ScalaSQLTable.name(tmp.asInstanceOf[Tuple2[ScalaSQLTable[?], ScalaSQLTable[?]]]._1)}")
//    println(s"ACC=${ScalaSQLTable.name(acc.asInstanceOf[Tuple2[ScalaSQLTable[?], ScalaSQLTable[?]]]._1)}")
    val (currentDelta, lastDelta) = fns(bases, tmp)

    val isEmpty = cmp(currentDelta, acc)
    if (isEmpty)
      acc
    else
      val newBase = copyInto(currentDelta, acc, lastDelta)
      scalaSQLMultiFix(newBase, acc, lastDelta)(fns)(cmp)(copyInto)
  }

  var it = 0
  // this is silly but higher kinded types are mega painful to abstract
  final def scalaSQLSemiNaive2[Q1, Q2, T1 >: Tuple, T2 >: Tuple, P1[_[_]], P2[_[_]], Tables]
    (using Tables =:= (ScalaSQLTable[P1], ScalaSQLTable[P2]))
    (set: Boolean)
    (db: DbApi, init_delta: Tables, init_derived: Tables, init_tmp: Tables)
    (toTuple: (P1[Expr] => Tuple, P2[Expr] => Tuple))
    (initBase: () => (query.Select[T1, Q1], query.Select[T2, Q2]))
    (initRecur: Tables => (query.Select[T1, Q1], query.Select[T2, Q2]))
  : Unit =

    db.run(init_delta._1.delete(_ => true))
    db.run(init_derived._1.delete(_ => true))
    db.run(init_tmp._1.delete(_ => true))

    db.run(init_delta._2.delete(_ => true))
    db.run(init_derived._2.delete(_ => true))
    db.run(init_tmp._2.delete(_ => true))

    val (base1, base2) = initBase()

    db.run(init_delta._1.insert.select(
      toTuple._1,
      base1
    ))
    db.run(init_derived._1.insert.select(
      toTuple._1,
      base1
    ))
    db.run(init_delta._2.insert.select(
      toTuple._2,
      base2
    ))

    db.run(init_derived._2.insert.select(
      toTuple._2,
      base2
    ))

    def printTable(t: Tables, name: String): Unit =
      println(s"${name}1(${ScalaSQLTable.name(t._1)})=${db.runRaw[(String, String)](s"SELECT * FROM ${ScalaSQLTable.name(t._1)}")}")
      println(s"${name}2(${ScalaSQLTable.name(t._2)})=${db.runRaw[(String, String)](s"SELECT * FROM ${ScalaSQLTable.name(t._2)}")}")


    val cmp: (Tables, Tables) => Boolean = (currentDelta, acc) =>
//      println("-----CMP-----")
//      printTable(currentDelta, "currentDelta")
//      printTable(acc, "acc")

      val (newDelta1, newDelta2) = (
        currentDelta._1.select.asInstanceOf[query.Select[T1, Q1]].except(acc._1.select.asInstanceOf[query.Select[T1, Q1]]),
        currentDelta._2.select.asInstanceOf[query.Select[T2, Q2]].except(acc._2.select.asInstanceOf[query.Select[T2, Q2]])
      )
//      println(s"DIFF -->1:${db.run(newDelta1)}, 2:${db.run(newDelta1)}")
      db.run(newDelta1).isEmpty && db.run(newDelta2).isEmpty

    val fixFn: (Tables, Tables) => (Tables, Tables) = (recur, temp) => {
//      println(s"-----FIX $it-----")
//      println(s"recur=${ScalaSQLTable.name(recur._1)}, temp=${ScalaSQLTable.name(temp._1)}")
//      printTable(recur, "base")
      val (query1, query2) = initRecur(recur)

//      println(s"recur returns=${db.run(query1)}")
//      println(s"recur returns=${db.run(query2)}")

      db.run(temp._1.delete(_ => true))
      db.run(temp._2.delete(_ => true))
      db.run(temp._1.insert.select( // need tmp because reads from delta
        toTuple._1,
        query1
      ))
      db.run(temp._2.insert.select( // need tmp because reads from delta
        toTuple._2,
        query2
      ))
//      printTable(temp, "RES-FIX")

//      if (it > 1) System.exit(0)
      it += 1

      (temp, recur) // = newDelta, oldDelta
    }

    val copyInto: (Tables, Tables, Tables) => Tables = (currentDelta, acc, temp) => {
//      println("----CopyInto----")
//      println(s"currentDelta=${ScalaSQLTable.name(currentDelta._1)}, acc=${ScalaSQLTable.name(acc._1)}, temp=${ScalaSQLTable.name(temp._1)}")
      printTable(currentDelta, "currentDelta")
      printTable(acc, "acc")
      if (set)
        db.run(temp._1.delete(_ => true))
        db.run(temp._2.delete(_ => true))
        db.updateRaw(s"INSERT INTO ${ScalaSQLTable.name(temp._1)} (SELECT * FROM ${ScalaSQLTable.name(currentDelta._1)} UNION SELECT * FROM ${ScalaSQLTable.name(acc._1)})")
        db.updateRaw(s"INSERT INTO ${ScalaSQLTable.name(temp._2)} (SELECT * FROM ${ScalaSQLTable.name(currentDelta._2)} UNION SELECT * FROM ${ScalaSQLTable.name(acc._2)})")
        db.run(acc._1.delete(_ => true))
        db.run(acc._2.delete(_ => true))
        db.updateRaw(s"INSERT INTO ${ScalaSQLTable.name(acc._1)} (SELECT * FROM ${ScalaSQLTable.name(temp._1)})")
        db.updateRaw(s"INSERT INTO ${ScalaSQLTable.name(acc._2)} (SELECT * FROM ${ScalaSQLTable.name(temp._2)})")
      else
        db.updateRaw(s"INSERT INTO ${ScalaSQLTable.name(acc._1)} (SELECT * FROM ${ScalaSQLTable.name(currentDelta._1)})")
        db.updateRaw(s"INSERT INTO ${ScalaSQLTable.name(acc._2)} (SELECT * FROM ${ScalaSQLTable.name(currentDelta._2)})")
//      printTable(acc, "newAcc")
//      printTable(currentDelta, "newBase")
      currentDelta
    }

    scalaSQLMultiFix(init_delta, init_derived, init_tmp)(fixFn)(cmp)(copyInto)
//    copyInto()
}
