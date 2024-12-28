package tyql

object BooleanLiterals:
  trait UseTrueFalse extends Dialect:
    override def quoteBooleanLiteral(in: Boolean): String =
      if in then "TRUE" else "FALSE"
