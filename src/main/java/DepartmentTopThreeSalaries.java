import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.*;
//import org.apache.spark.sql.types.*;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.*;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
/* https://leetcode.com/problems/department-top-three-salaries/
The Employee table holds all employees. Every employee has an Id, and there is also a column for the department Id.
The Department table holds all departments of the company.

/*
 */


public class DepartmentTopThreeSalaries {
    public static void main(String[] args) {
//        String cwd = Paths.get(".").toAbsolutePath().normalize().toString();
        String inputPath = Paths.get(".", "src", "main", "resources", "input_data", "employee").toString();
        String employeePath = Paths.get(inputPath, "employee.csv").toString();
        String departmentPath = Paths.get(inputPath, "department.csv").toString();
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder()
                .appName("HumanTraffic")
                .master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///c:/tmp/")
                .config("spark.driver.host", "127.0.0.1")
                .getOrCreate();
        getTopSalaries(employeePath, departmentPath, spark);
    }
    public static void getTopSalaries(String employeePath, String departmentPath, SparkSession spark)
    {
        StructType schemaEmployee = DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField("Id",  DataTypes.StringType, true),
                DataTypes.createStructField("Name", DataTypes.StringType, true),
                DataTypes.createStructField("Salary", DataTypes.LongType, true),
                DataTypes.createStructField("DepartmentId", DataTypes.StringType, true)

        });
        Dataset<Row> employeeDf = spark.read().option("header", true)
                .option("multiline", true)
                .schema(schemaEmployee)
                .csv(employeePath)
                .withColumnRenamed("Name", "Employee")
                .withColumn("DepartmentId", functions.trim(functions.col("DepartmentId")).cast(DataTypes.StringType));
        StructType schema = DataTypes.createStructType(new StructField[] {
                        DataTypes.createStructField("Id",  DataTypes.StringType, true),
                        DataTypes.createStructField("Name", DataTypes.StringType, true)
                });
        Dataset<Row> departmentDf = spark.read().option("header", true)
                .option("multiline", true)
                .schema(schema)
                .csv(departmentPath)
                .withColumnRenamed("Name", "Department")
                .withColumn("IdTrim", functions.trim(functions.col("Id")))
                .drop("Id").withColumnRenamed("IdTrim", "Id")
                ;

//        departmentDf.printSchema();
//        departmentDf.show();
//        employeeDf.show();
        Dataset<Row> employeeDfRes = employeeDf.join(departmentDf, employeeDf.col("DepartmentId").$eq$eq$eq(departmentDf.col("Id")), "right")
                .drop("Id")
                .drop("DepartmentId");
//                .show();

        WindowSpec wind = Window.partitionBy("Department").orderBy(functions.col("Salary").desc());
        Column windCol = functions.dense_rank().over(wind);

        employeeDfRes.select("Department", "Employee", "Salary")
                .withColumn("DeptRank", windCol)
                .filter("DeptRank <= 3")
                .drop("DeptRank").show();

    }
}
