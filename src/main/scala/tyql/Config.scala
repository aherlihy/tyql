package tyql

// Quill also allows you to chain a few of them and offers uppercase and lowercase options
// but I do think that is overengineered. Also, Quill handles identifier escaping here,
// which is just incorrect and asking for trouble.
// https://github.com/fwbrasil/quill/#naming-strategy

enum CaseConvention:
  case Exact
  case Underscores        // three_letter_word
  case PascalCase         // ThreeLetterWord
  case CamelCase          // threeLetterWord
  case CapitalUnderscores // THREE_LETTER_WORD
  case Joined             // threeletterword
  case JoinedCapital      // THREELETTERWORD

  def convert(name: String): String =
    val parts = CaseConvention.splitName(name)
    this match
      case Exact => name
      case Underscores => parts.mkString("_")
      case PascalCase => parts.map(_.capitalize).mkString
      case CamelCase => parts.head + parts.tail.map(_.capitalize).mkString
      case CapitalUnderscores => parts.map(_.toUpperCase).mkString("_")
      case Joined => parts.mkString
      case JoinedCapital => parts.map(_.toUpperCase).mkString

object CaseConvention:
  private def splitName(name: String): List[String] =
    name.split("(?=[A-Z])|[_\\s]").filter(_.nonEmpty).map(_.toLowerCase).toList

trait Config (
  val caseConvention: CaseConvention
)

object Config:
  given Config = new Config(CaseConvention.Exact) {}
