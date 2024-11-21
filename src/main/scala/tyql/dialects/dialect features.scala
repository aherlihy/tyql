package tyql

trait DialectFeature

object DialectFeature:
  trait RandomFloat extends DialectFeature
  trait RandomUUID extends DialectFeature
  // TODO also refactor this one just like the above two are refactored to be late-binding
  trait RandomIntegerInInclusiveRange extends DialectFeature // TODO later change it to not use raw SQL maybe?

  trait ReversibleStrings extends DialectFeature
