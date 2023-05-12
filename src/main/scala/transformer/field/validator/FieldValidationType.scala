package transformer.field.validator

object FieldValidationType extends Enumeration {
  type FieldValidationType = Value
  val NOT_NULL = Value

  def fromString(s: String): FieldValidationType = s match {
    /**
     * Convierte una cadena en un tipo de validación de campo.
     * @param s Cadena que representa el tipo de validación de campo.
     * @return Tipo de validación de campo.
     * @throws IllegalArgumentException Si el tipo de validación de campo no es compatible.
     */
    case "NOT_NULL" => NOT_NULL
    case _ => throw new IllegalArgumentException(s"Tipo de validación ${s} no soportado")
  }
}
