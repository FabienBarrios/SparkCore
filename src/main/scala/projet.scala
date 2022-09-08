import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{asc, avg, col, max, round, upper}
//import org.apache.log4j.Logger
//import org.apache.log4j.Level

//import java.lang.Thread.sleep

object projet extends App {
    val spark = SparkSession.builder()
        .appName("IABD2 spark APP")
        .master("local[*]")
        .getOrCreate()

    //delete error in cosole
    spark.sparkContext.setLogLevel("ERROR")

    //read a file
    val dataset_origin = spark.read
        .option("header", "true")
        //permet de mettre les bon types au colonne (accorder le bon schema)
        .option("inferSchema", "true")
        .csv("/Users/fabienbarrios/Desktop/Cours/Spark/Dataset/Twitch_global_data.csv")

//    val average_per_year = dataset_origin.groupBy("year")
//                                .mean("Avg_viewers")
//                                .sort("year")
//                                .show(false)

    val full = dataset_origin.groupBy("year")
        .agg(
            round(avg("Hours_watched")).as("Avg_Hours_watched"),
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

    val classement_mois_plus_rentable = dataset_origin.groupBy("Month")
        .agg(
            round(avg("Hours_watched")).as("Avg_Hours_watched"),
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
        .sort("Avg_viewers")
        .show(false)

    val avant_covid = dataset_origin.groupBy("year", "month", "Games_streamed")
            .agg(
                round(avg("Hours_watched")).as("Avg_Hours_watched"),
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
            .filter(col("year") < 2020)
            .sort("year", "month")
            .show(false)

    val apres_covid = dataset_origin.groupBy("Year", "Month", "Games_streamed")
        .agg(
            round(avg("Hours_watched")).as("Avg_Hours_watched"),
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
        .filter(col("year") >= 2020)
        .sort("year", "month")
        .show(false)

    System.in.read()
}