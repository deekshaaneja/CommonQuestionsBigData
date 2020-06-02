import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.nio.file.Paths;

public class NthHighestSalary {
    public static void main(String[] args) {
        String inputPath = Paths.get(".", "src", "main", "resources", "input_data", "company_employee").toString();
        String salaryPath = Paths.get(inputPath, "salary.csv").toString();

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder()
                .appName("ReportProcessing")
                .master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///c:/tmp/")
                .config("spark.driver.host", "127.0.0.1")
                .getOrCreate();
        findNthHighest(3, spark, salaryPath);
    }
    public static void findNthHighest(Integer n, SparkSession spark, String salaryPath){
        StructType gradeSchema = DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField("Id",  DataTypes.IntegerType, true),
                DataTypes.createStructField("Salary", DataTypes.LongType, true)
        });
        Dataset<Row> dfSalary = spark.read().schema(gradeSchema)
                .option("header", true)
                .option("multiline", true)
                .csv(salaryPath);
        WindowSpec wind = Window.orderBy(functions.col("Salary").desc());
        Column winCol = functions.dense_rank().over(wind);
        dfSalary.select(functions.col("Salary").alias("NthHighestSalary"), winCol.alias("rank"))
                .filter(functions.col("rank").$eq$eq$eq(n))
                .drop("rank")
                .show();

    }
}
