import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import javax.xml.crypto.Data;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;


public class DurhamParks {
    public static void main(String[] args) {
//        String filepath = "D:\\input_data\\city-parks-3.json";
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder()
                            .appName("Combine 2 datasets")
                            .master("local[*]")
                            .config("spark.sql.warehouse.dir", "file:///c:/tmp/")
                            .config("spark.driver.host", "127.0.0.1")
                            .getOrCreate();
        Dataset<Row> durhamDf = buildDurhamParksDataFrame(spark);
        durhamDf.printSchema();
        durhamDf.show(10);

    }
    public static Dataset<Row> buildDurhamParksDataFrame(SparkSession spark){

        Dataset<Row> df = spark.read().format("json")
                                .option("multiline", true)
                                .load("D:\\input_data\\city-parks-5.json");
        df = df.withColumn("parkId", concat(df.col("objectid"), lit("_"), df.col("cw_id")));
        return  df;
    }
}
