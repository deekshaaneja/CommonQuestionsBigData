import jdk.nashorn.internal.ir.SwitchNode;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import scala.Tuple1;
import scala.Tuple2;
import scala.collection.Seq;
import javax.xml.crypto.Data;
import org.apache.spark.sql.functions.*;

//import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.functions;

import java.util.ArrayList;


public class Practice {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder()
                .appName("Combine 2 datasets")
                .master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///c:/tmp/")
                .config("spark.driver.host", "127.0.0.1")
                .getOrCreate();
        Dataset<Row> rankDf = findRank(spark);
        rankDf.show();
        Dataset<Row> meanDf = findAvg(spark);
        meanDf.show();
        Dataset<Row> topN = findTopN(spark);
        topN.show();
        Dataset<Row> lagDf = findTimeLag(spark);
        lagDf.show();
        Dataset<Row> runningDf = findRunningTotal(spark);
        runningDf.show();

    }
    public static Dataset<Row> findRank(SparkSession spark){
        Dataset<Row> df = spark.read().format("csv")
                .option("multiline", true)
                .option("header", true)
                .load("C:\\Users\\admin\\Documents\\GitHub\\educate_problems\\pyspark\\input_data\\dataset.csv");
        WindowSpec wind = Window.partitionBy("depName").orderBy(df.col("salary").desc());
        Column depWind = functions.rank().over(wind);
//        Column avg = mean("salary").over(wind);
        Dataset<Row> rankedDf = df.withColumn("rank", depWind)
//                .withColumn("average", avg);
                .where("rank <= 2");
        return  rankedDf;
    }
   public static Dataset<Row> findAvg(SparkSession spark) {
       Dataset<Row> df = spark.read().format("csv")
               .option("multiline", true)
               .option("header", true)
               .load("C:\\Users\\admin\\Documents\\GitHub\\educate_problems\\pyspark\\input_data\\dataset.csv");
       WindowSpec wind = Window.partitionBy("depName");
       Column windCol = functions.mean("salary").over(wind);
       Dataset<Row> meanDf = df.withColumn("average", windCol);
       return meanDf;
   }
   public static Dataset<Row> findTopN(SparkSession spark){
       Dataset<Row> df = spark.read().format("csv")
               .option("multiline", true)
               .option("header", true)
               .load("C:\\Users\\admin\\Documents\\GitHub\\educate_problems\\pyspark\\input_data\\data_top_n.csv");
       WindowSpec wind = Window.partitionBy("category").orderBy(df.col("revenue").desc());
       Column windDr = functions.dense_rank().over(wind);
       Column windR = functions.rank().over(wind);
       Dataset<Row> topNDf = df.withColumn("rank", windR)
               .withColumn("dense rank", windDr);
       return topNDf;
   }
   public static Dataset<Row> findTimeLag(SparkSession spark){
       Dataset<Row> df = spark.read().format("csv")
               .option("multiline", true)
               .option("header", true)
               .load("C:\\Users\\admin\\Documents\\GitHub\\educate_problems\\pyspark\\input_data\\orders.csv");
       WindowSpec wind = Window.partitionBy("userId").orderBy("timestamp");
       Column windCol = df.col("timestamp").$minus(functions.lag(df.col("timestamp"), 1).over(wind));
       Dataset<Row> lagDf = df.withColumn("timeLag", windCol);
//       Dataset<Row> timeDiffDf = lagDf.withColumn("timeDiff", lagDf.col("timestamp").$minus(lagDf.col("timeLag")));
       return  lagDf;
   }
   public static Dataset<Row> findRunningTotal(SparkSession spark){
       Dataset<Row> df = spark.read().format("csv")
               .option("multiline", true)
               .option("header", true)
               .load("C:\\Users\\admin\\Documents\\GitHub\\educate_problems\\pyspark\\input_data\\running_total_data.csv");
       WindowSpec wind = Window.orderBy("orderQty");
       Column windCol = functions.sum(df.col("orderQty")).over(wind);
       Dataset<Row> runningDf = df.withColumn("running_total", windCol);
       return runningDf;
   }
}
