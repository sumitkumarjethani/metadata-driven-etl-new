package field.validator

import org.apache.spark.sql.DataFrame

abstract class FieldValidator() {
  def validate(fieldName: String, df: DataFrame): DataFrame
}
