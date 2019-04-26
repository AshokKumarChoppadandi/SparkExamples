package com.sparkTutorial.sparkSql.join;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class UkMakerSpaces {

    public static void main(String[] args) throws Exception {

        Logger.getLogger("org").setLevel(Level.ERROR);

        /*
        SparkSession session = SparkSession.builder().appName("UkMakerSpaces").master("local[*]").getOrCreate();

        Dataset<Row> makerSpace = session.read().option("header", "true").csv("in/uk-makerspaces-identifiable-data.csv");

        Dataset<Row> postCode = session.read().option("header", "true").csv("in/uk-postcode.csv")
                .withColumn("PostCode", concat_ws("", col("PostCode"), lit(" ")));

        System.out.println("=== Print 20 records of makerspace table ===");
        makerSpace.show();

        System.out.println("=== Print 20 records of postcode table ===");
        postCode.show();

        Dataset<Row> joined = makerSpace.join(postCode,
                makerSpace.col("Postcode").startsWith(postCode.col("Postcode")), "left_outer");

        System.out.println("=== Group by Region ===");
        joined.groupBy("Region").count().show(200);



        */

        SparkSession spark = SparkSession.builder().appName("UkMakerSpaces").master("local").getOrCreate();

        Dataset<Row> makerSpace = spark.read().option("header", "true").csv("in/uk-makerspaces-identifiable-data.csv");
        Dataset<Row> postCode = spark.read().option("header", "true").csv("in/uk-postcode.csv");

        //makerSpace.show(5);
        //postCode.show(5);
        Dataset<Row> postCode2 = postCode.withColumn("newPostCode", concat_ws("", col("Postcode"), lit(" ")));
        //postCode2.show(5);

        Dataset<Row> joinData = makerSpace.join(postCode2, makerSpace.col("Postcode").startsWith(postCode2.col("newPostCode")), "left_outer");
        Dataset<Row> countsPerRegion = joinData.groupBy(postCode2.col("Region")).count();
        countsPerRegion.orderBy(col("count").desc()).show(5);


        Dataset<Row> makerSpace21 = spark.read().option("header", "true").csv("in/uk-makerspaces-identifiable-data.csv");
        Dataset<Row> postCode21 = spark.read().option("header", "true").csv("in/uk-postcode.csv");

        Dataset<Row> makerSpace22 = makerSpace21.withColumn("newPostCode", concat_ws("", split(col("Postcode"), " ").getItem(0), lit(" ")));
        Dataset<Row> postCode22 = postCode21.withColumn("newPostCode", concat_ws("", col("Postcode"), lit(" ")));

        //Dataset<Row> joinedDf = makerSpace22.join(postCode22, makerSpace22.col("newPostCode").$eq$eq$eq(postCode22.col("newPostCode")), "left_outer");
        Dataset<Row> joinedDf = makerSpace22.join(postCode22, makerSpace22.col("newPostCode").$eq$eq$eq(postCode22.col("newPostCode")), "inner");
        joinedDf.show(5);
        RelationalGroupedDataset groupedDataset = joinedDf.select(postCode22.col("Region"), lit(1).as("one")).groupBy(col("Region"));
        Dataset<Row> countsPerRegion2 = groupedDataset.agg(count("one").as("CountsPerRegion"));
        countsPerRegion2.orderBy(col("CountsPerRegion").desc()).show(5);

        makerSpace22.createOrReplaceTempView("table1");
        postCode22.createOrReplaceTempView("table2");

        Dataset<Row> result = spark.sql(
                "select b.Region, count(1) as total_counts from table1 a join table2 b on (a.newPostCode == b.newPostCode) group by b.Region order by total_counts desc"
        );
        result.show(5);
    }
}