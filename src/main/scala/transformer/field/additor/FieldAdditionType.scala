package transformer.field.additor

object FieldAdditionType extends Enumeration {
  type FieldAdditionType = Value
  val CURRENT_TIMESTAMP = Value

  def fromString(s: String): FieldAdditionType = s match {
    case "CURRENT_TIMESTAMP" => CURRENT_TIMESTAMP
    case _ => throw new IllegalArgumentException(s"Invalid FieldAdditionType: $s")
  }
}

