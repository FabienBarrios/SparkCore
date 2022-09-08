import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

object Main extends App {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession
        .builder()
        .appName("SparkProject")
        .master(master = "local[*]")
        .getOrCreate()

    spark.conf.set("spark.sql.shuffle.partitions", "5")

    val path: String = "Dataset/"

    val csv_2010_12_01: Dataset[Row] = spark
        .read
        .option("header", "true")
        .option("inferSchema", "true")
        .json(path + "data.json")


    csv_2010_12_01.printSchema()

    // PARTIE STREAMING

    val retailSchema = csv_2010_12_01.schema

    val retailStream = spark.readStream
        .schema(retailSchema)
        .option("maxFilesPerTrigger", 1)
        .format("csv")
        .option("header", "true")
        .load(path + "*.json")

    val average_per_year = retailStream.groupBy("created_at")
                                .mean("Avg_viewers")
                                .sort("created_at")
                                .show(false)

    val full = retailStream.groupBy("created_at")
        .agg(
            round(avg("public_metrics")).as("Avg_Hours_watched"),
            max("Hours_watched").as("Max_Hours_watched"),
            round(avg("Avg_viewers")).as("Avg_viewers"),
            max("Avg_viewers").as("Max_viewers"),
            max("Peak_viewers").as("Peak_viewers"),
            round(avg("Streams")).as("Avg_streams"),
            max("Streams").as("Peak_streams"),
            round(avg("Avg_channels")).as("Avg_channels"),
            max("Avg_channels").as("Max_channels"),
            max("Games_streamed").as("Max_Games_streamed_diff"),
        )
        .sort("year")
        .show(false)

//    val totalJourClientStream = retailStream
//        .selectExpr("author_id",
//            "(Capacité de la station - Nombre bornettes libres) as BorneRestante",
//            "Actualisation de la donnée")
//        .groupBy(col("Identifiant station"),
//            window(col("Actualisation de la donnée"),
//                "1 hour"))
//        .sum("BorneRestante")
//
//    totalJourClientStream.writeStream
//        .format("memory") // memory = store in-memory table
//        .queryName("purchase_stream") // name of the in-memory table
//        .outputMode("complete") // all the counts should be in table
//        .start()
//
//    for (i <- 1 to 50) {
//        spark.sql(
//            """
//              |SELECT * FROM purchase_stream
//              |ORDER BY 'sum(BorneRestante)' DESC
//              |""".stripMargin
//        ).show(false)
//
//        Thread.sleep(1000)
//    }

    System.in.read
    spark.stop()
}