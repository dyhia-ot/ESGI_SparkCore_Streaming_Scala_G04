package producer

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.hadoop.conf.Configuration
import com.typesafe.config.ConfigFactory

object FileProducer extends App {

  // ============================================================
  // 1. CHARGER LA CONFIGURATION
  // ============================================================
  val config         = ConfigFactory.load().getConfig("producer")
  val sourceDir      = config.getString("source.dir")
  val destDir        = config.getString("destination.dir")
  val stagingDir     = config.getString("staging.dir")
  val batchSize      = config.getInt("batch-size")
  val intervalMillis = config.getDuration("interval").toMillis
  val mode           = config.getString("mode")
  val recursive      = config.getBoolean("recursive")
  val loop           = config.getBoolean("loop")

  // ============================================================
  // 2. INITIALISER SPARK
  // ============================================================
  val sparkConf = new SparkConf().setAppName("FileProducer").setMaster("local[*]")
  val sc = new SparkContext(sparkConf)
  sc.setLogLevel("WARN")

  println(s"=== PRODUCTEUR SPARK DÉMARRÉ ===")
  println(s"Source: $sourceDir | Destination: $destDir | Batch: $batchSize | Intervalle: ${intervalMillis}ms")

  val fs0 = FileSystem.get(new Configuration())
  fs0.mkdirs(new Path(stagingDir))
  fs0.mkdirs(new Path(destDir))
  fs0.close()

  var running = true
  var cursor  = 0        //  CURSEUR : position dans la liste des images déjà envoyées

  // ============================================================
  // 3. BOUCLE PRINCIPALE
  // ============================================================
  while (running) {

    val base = new java.io.File(".").getCanonicalPath
    val globPattern = if (recursive) s"$base/$sourceDir/*/*" else s"$base/$sourceDir/*"
    println(s"Glob pattern : $globPattern")

    // --- 3a. Découvrir les images via Hadoop FS (pas de job Spark) ---
    val conf = new Configuration()
    val fs   = FileSystem.get(conf)
    val statuses = fs.globStatus(new Path(globPattern))
    val allPaths: Array[String] = Option(statuses).getOrElse(Array.empty)
      .map(_.getPath.toString)
      .filter(p => p.toLowerCase.endsWith(".jpg")
                || p.toLowerCase.endsWith(".jpeg")
                || p.toLowerCase.endsWith(".png"))
    fs.close()

    println(s"Nombre d'images trouvées : ${allPaths.length}")

    // --- 3b. Gestion du curseur : a-t-on tout envoyé ? ---
    if (allPaths.isEmpty) {
      if (loop) println("Aucun fichier, on attend le prochain cycle (loop actif)...")
      else { println("Aucun fichier et loop désactivé. Arrêt."); running = false }
    } else if (cursor >= allPaths.length) {
      // tout a été envoyé une fois
      if (loop) {
        println("=== Toutes les images ont été envoyées, nouveau cycle ===")
        cursor = 0
      } else {
        println("Toutes les images envoyées. Arrêt.")
        running = false
      }
    } else {
      // --- 3c. Prendre le PROCHAIN lot (images différentes à chaque fois) ---
      val batch = allPaths.slice(cursor, cursor + batchSize)
      cursor += batch.length
      println(s"\n--- Drop de ${batch.length} image(s) [${cursor}/${allPaths.length}] ---")

      // --- 3d. Copie en PARALLÈLE ---
      sc.parallelize(batch).foreachPartition { partition =>
        val c    = new Configuration()
        val fsys = FileSystem.get(c)
        fsys.setWriteChecksum(false)

        partition.foreach { srcPathStr =>
          try {
            val src     = new Path(srcPathStr)
            val staging = new Path(s"$stagingDir/${src.getName}")
            val dest    = new Path(s"$destDir/${src.getName}")
            val deleteSource = mode == "move"

            FileUtil.copy(fsys, src, fsys, staging, deleteSource, c)

            if (!fsys.rename(staging, dest))
              println(s"  [ERREUR] rename impossible : ${src.getName}")
            else
              println(s"  [${if (deleteSource) "MOVED" else "COPIED"}] ${src.getName}")
          } catch {
            case e: Exception => println(s"  [ERREUR] $srcPathStr : ${e.getMessage}")
          }
        }
        fsys.close()
      }

      println("Drop terminé.")
    }

    Thread.sleep(intervalMillis)
  }

  sc.stop()
  println("Producer arrêté.")
}