import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{asc, col, upper}
//import org.apache.log4j.Logger
//import org.apache.log4j.Level

//import java.lang.Thread.sleep

object firstApp extends App {
  val spark = SparkSession.builder()
      .appName("IABD2 spark APP")
      .master("local[*]")
      .getOrCreate()

  //delete error in cosole
  spark.sparkContext.setLogLevel("ERROR")


  val myDF = spark.range(100)
    .toDF("col1")

  val dfPair = myDF.filter("col1%2 == 0")

  val dfdouble = myDF.select(col("col1")*2 as "col2")

  val df2 = myDF.withColumn("col2", col("col1")*2)
  //df2.printSchema()

  val df3 = df2.select("*")
  //df3.explain()

  //read a file
  val sum2015 = spark.read
      .option("header", "true")
      //permet de mettre les bon types au colonne (accorder le bon schema)
      .option("inferSchema", "true")
      .csv("/Users/fabienbarrios/Desktop/Cours/Spark/ProjetSpark/sparkProjet/2015-summary.csv")

  val sumMaj = sum2015.select(upper(col("DEST_COUNTRY_NAME")) as "DEST_COUNTRY_NAME", col("ORIGIN_COUNTRY_NAME"), col("count") as "nbr fly")

  //TP: Exo 1
  val sumDest = sum2015.filter(col("ORIGIN_COUNTRY_NAME") === col("DEST_COUNTRY_NAME"))
  //sumDest.show()
  //sumDest.explain()

  //TP: Exo 2
  val sortedDF = sumMaj.sort(asc("count"))
  //sortedDF.explain()

  //TP: Exo 3
  val flightData2010Json = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .json("/Users/fabienbarrios/Desktop/Cours/Spark/ProjetSpark/sparkProjet/2010-summary.json")
  //flightData2010Json.show()

  //TP: Exo 4
  val flightsParquet = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .parquet("/Users/fabienbarrios/Desktop/Cours/Spark/ProjetSpark/sparkProjet/Fichier.gz.parquet")
  //flightsParquet.show()

  //déclare un RDD
  val myRDD = spark.sparkContext.textFile("/Users/fabienbarrios/Desktop/Cours/Spark/ProjetSpark/sparkProjet/2015-summary.csv")

  //Le take converti le rdd en tableau num= le nbr de lignes
  val takerdd = myRDD.take(num = 3)
  //affiche les lignes du rdd
  //takerdd.foreach(println(_))

  val matrdd = myRDD.map(e => e.split(",")) // = val matrdd = myRDD.map(_.split(","))
  //matrdd.take(3).foreach(e => e.foreach(println(_)))

  val flatRDD = myRDD.flatMap(e => e.split(","))

  val filterrdd = myRDD.filter(_.startsWith("S"))
  filterrdd.take(3).foreach(_.foreach(print(_)))

  System.in.read()
}