import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.reflect.ClassTag$;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.apache.spark.sql.*;
import java.util.HashSet;

public class WordCount {

    public static void main(String[] args) throws IOException {

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder()
                .appName("Combine 2 datasets")
                .master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///c:/tmp/")
                .config("spark.driver.host", "127.0.0.1")
                .getOrCreate();
        getTopWords(spark);
    }
    public static HashSet getStopWords() throws IOException {
        HashSet stopWords = new HashSet();
        for (String line : Files.readAllLines(Paths.get("C:\\Users\\admin\\Documents\\GitHub\\educate_problems\\pyspark\\input_data\\exclude.txt"))) {
            stopWords.add(line.trim());
        }
        return stopWords;
    }
    public static void getTopWords(SparkSession spark) throws IOException {
        Dataset<Row> df = spark.read().text("C:\\Users\\admin\\Documents\\GitHub\\educate_problems\\pyspark\\input_data\\testfile.txt");
        HashSet stopWords = getStopWords();
        Broadcast<HashSet<String>> broadCastVar = spark.sparkContext().broadcast(stopWords, ClassTag$.MODULE$.apply(HashSet.class));
//        Broadcast<HashSet<String>> brStop = spark.sparkContext().broadcast(stopWords, ClassTag$.MODULE$.apply(HashSet.class));
        Dataset<Row> splitDf = df.withColumn("valueSplit", functions.split(functions.lower(df.col("value")), "\\W+"))
                .filter(lineArr -> lineArr.length() > 0);
        Dataset<Row> wordsDf = splitDf.withColumn("word", functions.explode(splitDf.col("valueSplit")))
                            .withColumn("wordTrim", functions.trim(functions.col("word")));
        Dataset<Row> filteredWordsDf = wordsDf.filter(functions.not(wordsDf.col("wordTrim").isInCollection(broadCastVar.value())))
                            .filter(functions.length(wordsDf.col("wordTrim")).gt(0))
                            .withColumn("wordLen",functions.length(functions.col("wordTrim")))
                            .groupBy("wordTrim").count().alias("count")
                            .orderBy(functions.col("count").desc())
                            ;
        System.out.println(broadCastVar.value().contains("the"));
        filteredWordsDf.show();
    }
}
