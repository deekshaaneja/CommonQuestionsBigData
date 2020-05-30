import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;


public class Etl105 {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder()
                .appName("ETL-2")
                .master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///c:/tmp/")
                .config("spark.driver.host", "127.0.0.1")
                .getOrCreate();
//        Object schema = StructField("SMS", StringType());
        Dataset<Row> dataset =spark.read()
                .option("header", true)
                .json("D:\\input_data\\UbiqLog4UCI\\UbiqLog4UCI\\14_F\\log_1-2-2014.txt");
        dataset.show();

}}
