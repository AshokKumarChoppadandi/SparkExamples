package com.bigdata.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.functions.col;

import java.util.ArrayList;
import java.util.List;

public class UpdatingFullTableUsingJavaDataframesAPI {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().master("local").appName("UpdatingFullTableUsingJavaDataframesAPI").getOrCreate();

        List<StructField> fields1 = new ArrayList<>();
        fields1.add(DataTypes.createStructField("eid", DataTypes.IntegerType, true));
        fields1.add(DataTypes.createStructField("ename", DataTypes.StringType, true));
        fields1.add(DataTypes.createStructField("esalary", DataTypes.DoubleType, true));
        fields1.add(DataTypes.createStructField("edept", DataTypes.IntegerType, true));
        StructType schema1 = DataTypes.createStructType(fields1);

        Dataset<Row> df1 = spark.read().schema(schema1).option("header", false).csv("src/main/resources/input/Employees.csv");

        List<StructField> fields2 = new ArrayList<>();
        fields2.add(DataTypes.createStructField("eid", DataTypes.IntegerType, true));
        fields2.add(DataTypes.createStructField("ename", DataTypes.StringType, true));
        fields2.add(DataTypes.createStructField("esalary", DataTypes.DoubleType, true));
        fields2.add(DataTypes.createStructField("edept", DataTypes.IntegerType, true));
        StructType schema2 = DataTypes.createStructType(fields2);

        Dataset<Row> df2 = spark.read().schema(schema2).option("header", false).csv("src/main/resources/input/UpdatedEmployees.csv");

        Dataset<Row> joinDf = df1.alias("emp1").join(df2.alias("emp2"), df1.col("eid").$eq$eq$eq(df2.col("eid")), "full_outer");

        Dataset<Row> newRecords = joinDf.filter(col("emp1.eid").isNull()).select("emp2.*");
        Dataset<Row> oldRecords = joinDf.filter(col("emp2.eid").isNull()).select("emp1.*");
        Dataset<Row> updatedRecords = joinDf.filter(col("emp2.eid").isNotNull()).select("emp2.*");


        newRecords.show();

        oldRecords.show();

        updatedRecords.show();

        Dataset<Row> finalEmployees = oldRecords.union(updatedRecords).union(newRecords);

        finalEmployees.show();

        finalEmployees.repartition(1).write().mode(SaveMode.Overwrite).csv("src/main/resources/output/FinalEmployees");


    }
}
