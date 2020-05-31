import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.awt.*;
import java.nio.file.Paths;

import static org.apache.spark.sql.functions.*;

public class ExchangeSeats {
    public static void main(String[] args) {
        String inputPath = Paths.get(".", "src", "main", "resources", "input_data", "seats").toString();
        String seatsPath = Paths.get(inputPath, "seats.csv").toString();
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder()
                .appName("Combine 2 datasets")
                .master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///c:/tmp/")
                .config("spark.driver.host", "127.0.0.1")
                .getOrCreate();
        exchange(spark, seatsPath);
    }
    public static void exchange(SparkSession spark, String seatsPath){
        StructType schema = DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField("id",  DataTypes.IntegerType, true),
                DataTypes.createStructField("student", DataTypes.StringType, true)
        });
        Dataset<Row> dfTrips = spark.read().schema(schema)
                .option("header", true)
                .option("multiline", true)
                .csv(seatsPath);

        WindowSpec wind = Window.orderBy("id");
        Column leadCol = functions.lead(dfTrips.col("student"), 1).over(wind);
        Column lagCol = functions.lag(dfTrips.col("student"), 1).over(wind);
        Integer maxVal = ((Integer) dfTrips.agg(max("id")).first().get(0));


    System.out.println(maxVal);
    dfTrips.withColumn("student_new",
      when(dfTrips.col("id").cast(DataTypes.IntegerType).$percent(2).$eq$eq$eq(1).and(functions.not(functions.col("id").$eq$eq$eq(maxVal))), leadCol)
      .when(dfTrips.col("id").cast(DataTypes.IntegerType).$percent(2).$eq$eq$eq(0).and(functions.not(functions.col("id").$eq$eq$eq(maxVal))), lagCol)
      .otherwise(functions.col("student")))
      .show();
    }
}
