import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.*;


public class Etl1 {

    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder()
                            .appName("ETL-2")
                            .master("local[*]")
                            .config("spark.sql.warehouse.dir", "file:///c:/tmp/")
                            .config("spark.driver.host", "127.0.0.1")
                            .getOrCreate();
        Dataset<Row> dataset =spark.read()
                                .option("header", true)
                                .csv("D:\\input_data\\EDGAR-Log-20170329.csv");
        Dataset<Row> datasetFiltered = dataset.filter("code >= 500 AND code < 600");

        Dataset<Row> datasetHour = datasetFiltered.withColumn("extractedHour", hour((from_utc_timestamp(col("time"), "GMT"))))
                                        .groupBy("extractedHour").agg(count("ip").alias("countIp"))
                                        .sort("extractedHour");
//                                    .where("extractedHour > 0");
        datasetHour.show();
    }
}
