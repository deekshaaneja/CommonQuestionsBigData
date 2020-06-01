import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.immutable.Range;
import org.apache.spark.sql.expressions.*;

import java.nio.file.Paths;
/*You are given two tables: Students and Grades. Students contains three columns ID, Name and Marks.
+-----+--------+--------+
|Grade|Min_Mark|Max_Mark|
+-----+--------+--------+
|    1|       0|       9|
|    2|      10|      19|
|    3|      20|      29|
|    4|      30|      39|
|    5|      40|      49|
|    6|      50|      59|
|    7|      60|      69|
|    8|      70|      79|
|    9|      80|      89|
|   10|      90|     100|
+-----+--------+--------+

StudentMarks
+---+--------+-----+
| ID|    Name|Marks|
+---+--------+-----+
|  1|   Julia|   88|
|  2|Samantha|   68|
|  3|   Maria|   99|
|  4| Scarlet|   78|
|  5|  Ashley|   63|
|  6|    Jane|   81|
+---+--------+-----+

Find grades of all students
 */
public class Report {
    public static void main(String[] args) {
        String inputPath = Paths.get(".", "src", "main", "resources", "input_data", "the_report").toString();
        String gradesPath = Paths.get(inputPath, "grades.csv").toString();
        String reportPath = Paths.get(inputPath, "input.csv").toString();

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder()
                .appName("ReportProcessing")
                .master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///c:/tmp/")
                .config("spark.driver.host", "127.0.0.1")
                .getOrCreate();
        getReport(spark, reportPath, gradesPath);
    }


    public static  void getReport(SparkSession spark, String reportPath, String gradesPath){
        StructType gradeSchema = DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField("Grade",  DataTypes.IntegerType, true),
                DataTypes.createStructField("Min_Mark", DataTypes.IntegerType, true),
                DataTypes.createStructField("Max_Mark", DataTypes.IntegerType, true)
        });
        Dataset<Row> dfGrades = spark.read().schema(gradeSchema)
                .option("header", true)
                .option("multiline", true)
                .csv(gradesPath);

        StructType inputSchema = DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField("ID",  DataTypes.IntegerType, true),
                DataTypes.createStructField("Name", DataTypes.StringType, true),
                DataTypes.createStructField("Marks", DataTypes.IntegerType, true)
        });
        Dataset<Row> dfInput = spark.read().schema(inputSchema)
                .option("header", true)
                .option("multiline", true)
                .csv(reportPath);

//        dfInput.show();
//        dfGrades.show();
        dfInput.join(dfGrades, dfInput.col("Marks").between(dfGrades.col("Min_Mark"), dfGrades.col("Max_Mark")), "left")
                .drop(functions.col("Min_Mark")).drop(functions.col("Max_Mark")).show();

    }
}
