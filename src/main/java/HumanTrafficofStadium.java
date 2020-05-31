import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import java.nio.file.Paths;

public class HumanTrafficofStadium {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        String inputPath = Paths.get(".", "src", "main", "resources", "input_data", "stadium").toString();
        String stadiumPath = Paths.get(inputPath, "stadium.csv").toString();

        SparkSession spark = SparkSession.builder()
                .appName("HumanTraffic")
                .master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///c:/tmp/")
                .config("spark.driver.host", "127.0.0.1")
                .getOrCreate();
        getTraffic(spark, stadiumPath);
    }
    public static void getTraffic(SparkSession spark, String stadiumPath){
        Dataset<Row> dfTrips = spark.read().option("header", true)
                .option("multiline", true)
                .csv(stadiumPath);
        WindowSpec wind = Window.orderBy("id");
        Column next1Wind = functions.lead(dfTrips.col("people"), 1).over(wind);
        Column next2Wind = functions.lead(dfTrips.col("people"), 2).over(wind);
        dfTrips.withColumn("next1", next1Wind)
                .withColumn("next2", next2Wind)
                .filter(functions.col("people").gt(100).and((functions.col("next1").gt(100).or(functions.col("next1").isNull()))
                        .and(functions.col("next2").gt(100).or(functions.col("next2").isNull()))))
                .drop(functions.col("next1"))
                .drop(functions.col("next2"))
                .show();

    }



}
