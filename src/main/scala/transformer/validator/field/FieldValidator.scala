package transformer.validator.field

import org.apache.spark.sql.DataFrame

trait FieldValidator {
  def validate(fieldName: String, df: DataFrame): DataFrame
}
