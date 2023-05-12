import etl.{Extractor, Loader, Validator, Transformer}
import parser.metadata.JsonMetadataParser
import reader.JsonReader
import org.apache.log4j.PropertyConfigurator
import org.apache.log4j.Logger
import org.apache.spark.sql.{SparkSession}

object Main {
  PropertyConfigurator.configure(getClass.getClassLoader.getResource("log4j.properties"))
  private val logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    try {
      if (args.length == 0) throw new Exception("Ruta de metadatos no proporcionada")

      // Leer la ruta desde los argumentos de la línea de comandos
      val path = args(0)
      val spark = new SparkSessionWrapper("MetadataDrivenEtl").spark

      // Creamos el JsonReader para los metadatos
      logger.info("Leyendo el fichero de metadatos...")
      val metadata = new JsonReader(path).readJson()

      logger.info("Convirtiendo los metadatos a DataFlow...")
      val dataFlow = new JsonMetadataParser().parse(metadata)

      logger.info("Leyendo los ficheros de entrada...")
      val extractor = new Extractor(spark)
      val sourcesMap = extractor.load(dataFlow.sources)

      logger.info("Realizando las validaciones necesarias...")
      val validator = new Validator()
      val sourcesMapValidated = validator.validate(dataFlow.transformations, sourcesMap)

      logger.info("Realizando las transformaciones necesarias...")
      val transformer = new Transformer()
      val transformedMap = transformer.transform(dataFlow.transformations, sourcesMapValidated)

      logger.info("Escribiendo los ficheros necesarios...")
      val loader = new Loader()
      loader.load(dataFlow.sinks, transformedMap)

      logger.info("ETL Terminada...")
      spark.stop()
    } catch {
      case e: Exception => logger.error("Se ha producido un error: " + e.getMessage()); sys.exit()
    }
  }
}
