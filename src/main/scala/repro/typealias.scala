package repro

import language.experimental.namedTuples
import NamedTuple.{NamedTuple, AnyNamedTuple}

// Repros for bugs or questions
class ClassToMap[A]()
abstract class ClassToFind[Rows <: AnyNamedTuple]:
  def mapped: NamedTuple.Map[Rows, ClassToMap]

given TDB: ClassToFind[(t1: Int, t2: String)] with
  override def mapped = (
    t1 = ClassToMap[Int](),
    t2 = ClassToMap[String]()
  )

type TypeAlias = (t1: Int, t2: String)
class Repro1_Pass(using val testDB: ClassToFind[TypeAlias]) {
  def query() =
    testDB.mapped.t1
}
// Uncomment:
// class Repro1_Fail(using val testDB: ClassToFind[(t1: Int, t2: String)]) {
//   def query() =
//     testDB.mapped.t1 // fails to compile
// }
/* Fails with:
[error] 25 |    testDB.mapped.t1 // fails to compile
[error]    |    ^^^^^^^^^^^^^
[error]    |Found:    (x$proxy3 :
[error]    |  (Repro1_Fail.this.testDB.mapped :
[error]    |    => NamedTuple.Map[(t1 : Int, t2 : String), repro.ClassToMap])
[error]    | &
[error]    |  $proxy3.NamedTuple[
[error]    |    NamedTupleDecomposition.Names[
[error]    |      $proxy3.NamedTuple[(("t1" : String), ("t2" : String)), (Int, String)]],
[error]    |    Tuple.Map²[
[error]    |      NamedTupleDecomposition.DropNames[
[error]    |        $proxy3.NamedTuple[(("t1" : String), ("t2" : String)), (Int, String)]],
[error]    |      repro.ClassToMap]
[error]    |  ]
[error]    |)
[error]    |Required: (repro.ClassToMap[Int], repro.ClassToMap[String])
 */