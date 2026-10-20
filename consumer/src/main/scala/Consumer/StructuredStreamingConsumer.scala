package consumer

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._ 

object StructuredStreamingConsumer extends App {

  // ============================================================
  // 0. SPARK SESSION (l'API haut niveau, contrairement au Producer
  //    qui utilisait le SparkContext bas niveau)
  // ============================================================
  val spark = SparkSession.builder()
    .appName("FastFood Consumer")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")
  import spark.implicits._


  // ============================================================
  // 1. SCHÉMA EXPLICITE (obligatoire en streaming : pas d'inférence)
  //    Le format "binaryFile" produit exactement ces 4 colonnes :
  //    path, modificationTime, length, content
  // ============================================================
  val binaryFileSchema = StructType(Seq(
    StructField("path",             StringType),
    StructField("modificationTime", TimestampType),
    StructField("length",           LongType),
    StructField("content",          BinaryType)
  ))


  // ============================================================
  // 2. SOURCE : surveille le répertoire destination (FLÈCHE 3)
  //    Chaque nouveau fichier = 1 nouvelle ligne dans le flux.
  //    binaryFile donne : path, modificationTime, length, content
  // ============================================================
  val rawStream = spark.readStream
    .schema(binaryFileSchema)
    .format("binaryFile")
    .option("pathGlobFilter", "*.{jpg,jpeg,png}")   // images uniquement
    .load("data/destination")

  // ============================================================
  // 3. TRANSFORM : pour l'instant, on extrait juste les métadonnées.
  //   
  // ============================================================
  val results = rawStream.select(
    col("path"),                                   // chemin de l'image
    col("length").as("size_bytes"),                // taille
    col("modificationTime"),                       // date d'arrivée
    current_timestamp().as("processed_at")         // quand le Consumer l'a traitée
  )

  // ============================================================
  // 4. SINK : écriture continue en Parquet dans data/output (FLÈCHE 4)
  //    outputMode("append") = seulement les nouvelles lignes à chaque batch
  //    checkpoint = mémoire de la requête (obligatoire pour la tolérance aux pannes)
  // ============================================================
  val query = results.writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", "data/output")
    .option("checkpointLocation", "data/checkpoint")
    .start()

  println("=== CONSUMER DÉMARRÉ : surveillance de data/destination ===")

  query.awaitTermination()   // garde la requête vivante indéfiniment
}