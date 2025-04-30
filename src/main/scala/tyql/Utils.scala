package tyql

import scala.compiletime.ops.int.S

object Utils:
  type GenerateIndices[N <: Int, Size <: Int] <: Tuple = N match
    case Size => EmptyTuple
    case _ => N *: GenerateIndices[S[N], Size]

  type ZipWithIndex[T <: Tuple] = Tuple.Zip[T, GenerateIndices[0, Tuple.Size[T]]]

  /**
   * Check for duplicates in a tuple by recursively checking if the body of the tuple contains the head
   * @tparam T tuple
   * returns Nothing if there are duplicates, otherwise the unmodified input T
   */
  type HasDuplicate[T <: Tuple] <: Tuple = T match
    case EmptyTuple => T
    case h *: t => Tuple.Contains[t, h] match
      case true => Nothing
      case false => h *: HasDuplicate[t]

  type NotContains[T <: Tuple, U] <: Boolean = Tuple.Contains[T, U] match
    case true => false
    case false => true
  
  type Except[T1 <: Tuple, T2 <: Tuple] = Tuple.Filter[T1, [t] =>> NotContains[T2, t]]

  extension [Base <: Tuple, F[_], G[_]](tuple: Tuple.Map[Base, F])
    /** Map a tuple `(F[A], F[B], ...)` to a tuple `(G[A], G[B], ...)`. */
    inline def naturalMap(f: [t] => F[t] => G[t]): Tuple.Map[Base, G] =
      scala.runtime.Tuples.map(tuple, f.asInstanceOf).asInstanceOf[Tuple.Map[Base, G]]