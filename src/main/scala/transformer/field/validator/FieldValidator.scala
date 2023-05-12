package transformer.field.validator

import org.apache.spark.sql.DataFrame

trait FieldValidator {
  def validate(fieldName: String, df: DataFrame): DataFrame
}
