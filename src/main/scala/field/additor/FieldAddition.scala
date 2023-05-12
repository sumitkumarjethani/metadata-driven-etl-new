package field.additor
import org.apache.spark.sql.DataFrame

trait FieldAddition {
  def add(df: DataFrame): DataFrame
}
