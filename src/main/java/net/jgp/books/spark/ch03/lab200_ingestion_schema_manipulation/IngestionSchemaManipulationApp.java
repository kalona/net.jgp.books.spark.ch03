package net.jgp.books.spark.ch03.lab200_ingestion_schema_manipulation;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

import org.apache.spark.Partition;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.nio.MappedByteBuffer;
import java.util.Collections;
import java.util.Map;

/**
 * CSV ingestion in a dataframe and manipulation.
 * 
 * @author jgp
 */
public class IngestionSchemaManipulationApp {

    /**
     * main() is the entry point to the application.
     * This method initializes and starts the application.
     *
     * @param args Command line arguments for the application.
     */
    public static void main(String[] args) {
        IngestionSchemaManipulationApp app = new IngestionSchemaManipulationApp();
        app.start();
    }


  /**
   * The processing code.
   */
  private void start() {
    // Creates a session on a local master
    SparkSession spark = SparkSession.builder()
        .appName("Restaurants in Wake County, NC")
        .master("local")
        .getOrCreate();

    // Reads a CSV file with header, called
    // Restaurants_in_Wake_County_NC.csv,
    // stores it in a dataframe
    Dataset<Row> df = spark.read().format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load("data/Restaurants_in_Wake_County_NC.csv");

    System.out.println("*** Right after ingestion");
    df.show(5);
    df.printSchema();
    System.out.println("We have " + df.count() + " records.");

//    // Let's transform our dataframe
//    df = df.withColumn("county", lit("Wake"))
//        .withColumnRenamed("HSISID", "datasetId")
//        .withColumnRenamed("NAME", "name")
//        .withColumnRenamed("ADDRESS1", "address1")
//        .withColumnRenamed("ADDRESS2", "address2")
//        .withColumnRenamed("CITY", "city")
//        .withColumnRenamed("STATE", "state")
//        .withColumnRenamed("POSTALCODE", "zip")
//        .withColumnRenamed("PHONENUMBER", "tel")
//        .withColumnRenamed("RESTAURANTOPENDATE", "dateStart")
//        .withColumnRenamed("FACILITYTYPE", "type")
//        .withColumnRenamed("X", "geoX")
//        .withColumnRenamed("Y", "geoY")
//        .drop("OBJECTID")
//        .drop("PERMITID")
//        .drop("GEOCODESTATUS");

      // Alternative way to transform our dataframe using withColumnsRenamed method

      Map<String, String> renames = Collections.unmodifiableMap(Map.ofEntries(
              Map.entry("HSISID", "datasetId"),
              Map.entry("NAME", "name"),
              Map.entry("ADDRESS1", "address1"),
              Map.entry("ADDRESS2", "address2"),
              Map.entry("CITY", "city"),
              Map.entry("STATE", "state"),
              Map.entry("POSTALCODE", "zip"),
              Map.entry("PHONENUMBER", "tel"),
              Map.entry("RESTAURANTOPENDATE", "dateStart"),
              Map.entry("FACILITYTYPE", "type"),
              Map.entry("X", "geoX"),
              Map.entry("Y", "geoY")
      ));

      df = df.withColumn("county", lit("Wake"))
              .withColumnsRenamed(renames)
              .drop("OBJECTID")
              .drop("PERMITID")
              .drop("GEOCODESTATUS");



    // Creates a new column "id" as a concatenation of state, county
    // and datasetId
    df = df.withColumn("id", concat(
        df.col("state"),
        lit("_"),
        df.col("county"), lit("_"),
        df.col("datasetId")));

    // Shows at most 5 rows from the dataframe
    System.out.println("*** Dataframe transformed");
    df.show(5);

//    for book only
//    Dataset<Row> dfUsedForBook = df.drop("address2")
//        .drop("zip")
//        .drop("tel")
//        .drop("dateStart")
//        .drop("geoX")
//        .drop("geoY")
//        .drop("address1")
//        .drop("datasetId");
//    dfUsedForBook.show(5, 15);
//    end

    df.printSchema();

    System.out.println("*** Looking at partitions");
    Partition[] partitions = df.rdd().partitions();
    int partitionCount = partitions.length;
    System.out.println("Partition count before repartition: " +
        partitionCount);

    df = df.repartition(4);
    System.out.println("Partition count after repartition: " +
        df.rdd().partitions().length);
  }
}
