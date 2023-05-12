ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

lazy val root = (project in file("."))
  .settings(
    name := "prueba-tecnica",
    libraryDependencies += "com.typesafe.play" %% "play-json" % "2.7.4",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.0",
    libraryDependencies += "org.apache.spark" %% "spark-core" % "3.2.0",
    libraryDependencies += "org.apache.logging.log4j" % "log4j-core" % "2.14.1"
  )
