package tyql

import scala.compiletime.ops.int.S

object Utils:
  type GenerateIndices[N <: Int, Size <: Int] <: Tuple = N match
    case Size => EmptyTuple
    case _ => N *: GenerateIndices[S[N], Size]

  type ZipWithIndex[T <: Tuple] = Tuple.Zip[T, GenerateIndices[0, Tuple.Size[T]]]

  type HasDuplicate[T <: Tuple] <: Tuple = T match
    case EmptyTuple => T
    case h *: t => Tuple.Contains[t, h] match
      case true => Nothing
      case false => h *: HasDuplicate[t]

  /** Remove the first occurrence of `X` from `T`, if present. */
  type RemoveFirst[T <: Tuple, X] <: Tuple = T match
    case EmptyTuple => EmptyTuple
    case X *: t => t
    case h *: t => h *: RemoveFirst[t, X]

  /** Multiset difference `T - S`: remove from `T` one occurrence of each element of `S`. */
  type MultisetMinus[T <: Tuple, S <: Tuple] <: Tuple = S match
    case EmptyTuple => T
    case h *: t => MultisetMinus[RemoveFirst[T, h], t]

  /**
   * Dependency collection for `union`/`unionAll`: the multiset union (pointwise maximum)
   * of the two branches' dependency tuples `D1` and `D2`, computed as `D1 ++ (D2 - D1)`.
   *
   * The two branches of a union become separate Datalog rules, so a recursive reference
   * used once in each branch must be counted once, not twice: cross-branch sharing must
   * NOT introduce a linearity violation. A duplicate WITHIN either branch is preserved, so
   * a genuinely non-linear branch is still rejected. This differs from `Tuple.Concat` (used
   * for `flatMap`, which is a join and so sums the two dependency counts).
   */
  type UnionCollect[D1 <: Tuple, D2 <: Tuple] = Tuple.Concat[D1, MultisetMinus[D2, D1]]

  extension [Base <: Tuple, F[_], G[_]](tuple: Tuple.Map[Base, F])
    /** Map a tuple `(F[A], F[B], ...)` to a tuple `(G[A], G[B], ...)`. */
    inline def naturalMap(f: [t] => F[t] => G[t]): Tuple.Map[Base, G] =
      scala.runtime.Tuples.map(tuple, f.asInstanceOf).asInstanceOf[Tuple.Map[Base, G]]