import org.apache.spark.sql.SparkSession

class SparkSessionWrapper(appName: String) {
  lazy val spark: SparkSession = {
    SparkSession.builder().master("local[*]").appName(appName).getOrCreate()
  }
}
