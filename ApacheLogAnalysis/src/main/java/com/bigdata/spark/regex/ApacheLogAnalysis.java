package com.bigdata.spark.regex;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ApacheLogAnalysis {
    public static void main(String[] args) {

        // To enable / write the lambda functions in Java
        // 1. File -> Project Structure -> Projec -> Project Language Level -> 8 - Lambdas, type annotations etc.
        // 2. File -> Project Structure -> Modules -> Sources -> Language Level -> 8 - Lambdas, type annotations etc.
        // 3. Set the byte code generation to Java 8
        // File -> Settings -> Build, Execution, Deployment -> Compiler -> Java Compiler -> Target bytecode version -> 8
        // Save all the above changes before running the program.

        SparkConf conf = new SparkConf().setMaster("local").setAppName("ApacheLogAnalysis");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        SparkSession spark = SparkSession.builder().appName("ApacheLogAnalysis").master("local").getOrCreate();
        String inputPath = "src/main/resources/input/apache_logs.log";

        String logPattern = "^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\S+)\\s?(\\S+)?\\s?(\\S+)?\" (\\d{3}|-) (\\d+|-)\\s?\"?([^\"]*)\"?\\s?\"?([^\"]*)?\"?$";
        final Pattern pattern = Pattern.compile(logPattern);

        JavaRDD<String> logs = jsc.textFile(inputPath);
        JavaRDD<Row> parsedLogs = logs.map(log -> {
           Matcher matcher = pattern.matcher(log);
           if(matcher.find()) {
               return RowFactory.create(
                       matcher.group(1),
                       matcher.group(2),
                       matcher.group(3),
                       matcher.group(4),
                       matcher.group(5),
                       matcher.group(6),
                       matcher.group(7),
                       Integer.parseInt(matcher.group(8)),
                       Integer.parseInt(matcher.group(9)),
                       matcher.group(10),
                       matcher.group(11)
               );
           } else {
               return null;
           }
        });

        JavaRDD<Row> filteredLogs = parsedLogs.filter(log -> log != null);
        filteredLogs.collect().forEach(x -> System.out.println(x));

        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("ip_address", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("col2", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("col3", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("request_time", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("request_type", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("request_info", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("protocal", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("response_code", DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("bytes_transferred", DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("col10", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("message", DataTypes.StringType, true));

        StructType schema = DataTypes.createStructType(fields);
        Dataset<Row> logsDataset = spark.createDataFrame(filteredLogs, schema);
        logsDataset.show();
    }
}
