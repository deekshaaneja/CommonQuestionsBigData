import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import scala.reflect.ClassTag$;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

//Trips and Users with Spark.
// https://leetcode.com/problems/trips-and-users/

public class TripsAndUsers {
    public static void main(String[] args) {
        String inputPath = Paths.get(".", "src", "main", "resources", "input_data", "trips_and_users").toString();
        String tripsPath = Paths.get(inputPath, "trips.csv").toString();
        String usersPath = Paths.get(inputPath, "users.csv").toString();

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder()
                .appName("Combine 2 datasets")
                .master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///c:/tmp/")
                .config("spark.driver.host", "127.0.0.1")
                .getOrCreate();
        getCancellationRate(spark, tripsPath, usersPath);
    }
    public static void getCancellationRate(SparkSession spark, String tripsPath, String usersPath){
        Dataset<Row> dfTrips = spark.read().option("header", true)
                    .option("multiline", true)
                    .csv(tripsPath);
        Dataset<Row> dfUsers = spark
                .read().option("header", true)
                .option("multiline", true)
                .csv(usersPath);
        HashSet<String> setBannedUsers = new HashSet();
        List<Object> bannedUsers = dfUsers.filter(dfUsers.col("Banned").$eq$eq$eq("Yes")).select(functions.collect_list("Users_Id"))
                .first().getList(0);
        System.out.println(bannedUsers);

        for (int i = 0; i < bannedUsers.size(); i++) {
            setBannedUsers.add((String) bannedUsers.get(i));
        }

//        System.out.println(bannedUsers);
        Broadcast<HashSet> usersBroadcast = spark.sparkContext().broadcast(setBannedUsers, ClassTag$.MODULE$.apply(HashSet.class));

        Dataset<Row> dfTripsUser = dfTrips.filter(functions.not(dfTrips.col("Client_Id").isInCollection(usersBroadcast.value())))
                .filter(functions.not(dfTrips.col("Driver_Id").isInCollection(usersBroadcast.value())))
                .select(functions.col("Request_at").alias("Day"), functions.col("Status"));
        Dataset<Row> df = dfTripsUser.groupBy("Day").agg(functions.count("Day").alias("totalRides"),
                    functions.sum(functions.when(functions.col("Status").startsWith("cancelled"), 1).otherwise(0)).alias("cancelledRides"))
                .withColumn("Cancellation Rate", functions.round(functions.col("cancelledRides").divide(functions.col("totalRides")), 2))
                .drop("totalRides").drop("cancelledRides")
                .orderBy("Day");
//        dfTripsUser.show();
        df.show();


    }

}
