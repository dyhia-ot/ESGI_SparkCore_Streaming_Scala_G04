

ThisBuild / scalaVersion := "2.12.17"
ThisBuild / organization := "com.curse"

val sparkVersion = "3.3.0"

// Définition du module "producer"
lazy val producer = (project in file("producer"))
  .settings(
    name := "producer",
    libraryDependencies ++= Seq(
      // Pour lire le fichier application.conf (HOCON)
      "com.typesafe" % "config" % "1.4.2",
      // Spark Core (contient sc.binaryFiles, SparkContext, et Hadoop FileUtil)
      "org.apache.spark" %% "spark-core" % sparkVersion
    ),

    // Ajout de ces 3 lignes pour régler le problème Java 17
    fork := true,
    javaOptions ++= Seq(
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED"
    ),
    //  LIGNE CLÉ : le programme tourne depuis la RACINE du projet
    Compile / run / baseDirectory := (LocalRootProject / baseDirectory).value
    
  )

// Définition du module "consumer"
lazy val consumer = (project in file("consumer"))
  .settings(
    name := "consumer",
    libraryDependencies ++= Seq(
      "com.typesafe" % "config" % "1.4.2",
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion    // ← Structured Streaming est dans spark-sql
    ),
    fork := true,
    javaOptions ++= Seq(
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED"
    ),
    Compile / run / baseDirectory := (LocalRootProject / baseDirectory).value
  )

// Projet racine (agrège tous les modules futurs)
lazy val root = (project in file("."))
  .aggregate(producer, consumer)
  .settings(
    name := "ESGI_SparkCore_Streaming_Scala_G04"
  )