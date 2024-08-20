package tyql

object Utils:
  extension [Base <: Tuple, F[_], G[_]](tuple: Tuple.Map[Base, F])
    /** Map a tuple `(F[A], F[B], ...)` to a tuple `(G[A], G[B], ...)`. */
    inline def naturalMap(f: [t] => F[t] => G[t]): Tuple.Map[Base, G] =
      scala.runtime.Tuples.map(tuple, f.asInstanceOf).asInstanceOf[Tuple.Map[Base, G]]