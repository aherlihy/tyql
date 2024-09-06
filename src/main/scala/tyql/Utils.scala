package tyql

import scala.compiletime.ops.int.S

object Utils:
  type GenerateIndices[N <: Int, Size <: Int] <: Tuple = N match
    case Size => EmptyTuple
    case _ => N *: GenerateIndices[S[N], Size]

  type ZipWithIndex[T <: Tuple] = Tuple.Zip[T, GenerateIndices[0, Tuple.Size[T]]]

  extension [Base <: Tuple, F[_], G[_]](tuple: Tuple.Map[Base, F])
    /** Map a tuple `(F[A], F[B], ...)` to a tuple `(G[A], G[B], ...)`. */
    inline def naturalMap(f: [t] => F[t] => G[t]): Tuple.Map[Base, G] =
      scala.runtime.Tuples.map(tuple, f.asInstanceOf).asInstanceOf[Tuple.Map[Base, G]]