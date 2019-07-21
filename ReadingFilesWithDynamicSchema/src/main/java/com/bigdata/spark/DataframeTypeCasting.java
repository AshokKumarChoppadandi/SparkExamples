package com.bigdata.spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DataframeTypeCasting {
    public static void main(String[] args) throws IOException {
        String applName = "DataframeTypeCasting";
        SparkSession spark = SparkSession.builder().appName(applName).master("local").getOrCreate();

        // Creating the Source Data / Table Schema
        List<StructField> inputFields = new ArrayList<>();
        inputFields.add(DataTypes.createStructField("id", DataTypes.StringType, true));
        inputFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        inputFields.add(DataTypes.createStructField("salary", DataTypes.StringType, true));
        inputFields.add(DataTypes.createStructField("dept", DataTypes.StringType, true));

        // Getting the Schema of the Table Dynamically
        //Dataset<Row> table = spark.sql("select * from db.table where 1 = 2");

        StructType inputSchema = DataTypes.createStructType(inputFields);

        // Reading the Data from a File using the Input Schema
        Dataset<Row> inputData = spark.read().schema(inputSchema).csv("src/main/resources/test1/employees.csv");
        String tempTable = "tempTable_" + applName;
        inputData.createOrReplaceTempView(tempTable);
        inputData.printSchema();
        inputData.show();

        // Creating the Output Data / Table Schema
        // Procedure 1 : Creating the Schema in Program
        List<StructField> outputFields = new ArrayList<>();
        outputFields.add(DataTypes.createStructField("id", DataTypes.IntegerType, true));
        outputFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        outputFields.add(DataTypes.createStructField("salary", DataTypes.DoubleType, true));
        outputFields.add(DataTypes.createStructField("dept", DataTypes.LongType, true));
        StructType outputSchema = DataTypes.createStructType(outputFields);

        // Procedure 2 : Creating the Schema from a File
        String line = new String(Files.readAllBytes(Paths.get("src/main/resources/test1/schema.txt")), StandardCharsets.UTF_8);
        StructType ouptutSchema2 = getSchemaFromString(line);

        // Procedure 3 : Creating the Schema from a physical table
        String dbName = "database";
        String tableName = "table";
        Dataset<Row> emptyDf = spark.sql("select * from " + dbName + "." + tableName + " where 1 = 2");
        StructType outputSchema3 = emptyDf.schema();

        // Getting the fields from Schema
        StructField[] fieldsTemp = outputSchema.fields();

        // Procedure 1: Converting the DataFrame from Input Schema to Output Schema
        List<String> castFields = new ArrayList<>();
        for (StructField field : fieldsTemp) {
            String fieldName = field.name();
            String fieldType = field.dataType().typeName();
            String castField = "cast(" + fieldName + " as " + fieldType + ") as " + fieldName;
            castFields.add(castField);
        }
        String finalQuery = "select " + String.join(", ", castFields) + " from " + tempTable;
        System.out.println("Final Query ::: \n" + finalQuery);

        Dataset<Row> convertedDf = spark.sql(finalQuery);
        convertedDf.printSchema();
        convertedDf.show();

        // Procedure 2: Converting the JavaRDD from Input Schema to Output Schema
        List<Tuple2<Integer, String>> fieldAndTypeTuple = new ArrayList<>();
        for (int i = 0; i < fieldsTemp.length; i++) {
            int fieldName = i;
            String fieldType = fieldsTemp[i].dataType().typeName();
            Tuple2<Integer, String> tuple = new Tuple2<>(Integer.valueOf(fieldName), fieldType);
            fieldAndTypeTuple.add(tuple);
        }
        JavaRDD<Row> outputData = inputData.toJavaRDD().map(row -> {
           ArrayList<Object> tempArray = new ArrayList<>();
           for(Tuple2<Integer, String> tuple : fieldAndTypeTuple) {
               tempArray.add(convertData(row.get(tuple._1), tuple._2));
           }
           return RowFactory.create(tempArray.toArray());
        });

        outputData.collect().forEach(x -> System.out.println(x));

        Dataset<Row> df = spark.createDataFrame(outputData, outputSchema);
        df.printSchema();
        df.show();

    }

    private static StructType getSchemaFromString(String str) {
        final ArrayList<StructField> buffer = new ArrayList<>();
        List<String> columnsWithTypes = Arrays.asList(str.split(","));

        columnsWithTypes.forEach(
                column -> {
                    String[] colNameAndType = column.split(":");
                    switch (colNameAndType[1].toLowerCase()) {
                        case "integer":
                            buffer.add(DataTypes.createStructField(colNameAndType[1], DataTypes.IntegerType, true));
                            break;
                        case "string":
                            buffer.add(DataTypes.createStructField(colNameAndType[1], DataTypes.StringType, true));
                            break;
                        case "double":
                            buffer.add(DataTypes.createStructField(colNameAndType[1], DataTypes.DoubleType, true));
                            break;
                        case "long":
                            buffer.add(DataTypes.createStructField(colNameAndType[1], DataTypes.LongType, true));
                            break;
                        case "float":
                            buffer.add(DataTypes.createStructField(colNameAndType[1], DataTypes.FloatType, true));
                            break;
                        default:
                            buffer.add(DataTypes.createStructField(colNameAndType[1], DataTypes.IntegerType, true));
                            break;
                    }
                });
        return DataTypes.createStructType(buffer);
    }

    private static Object convertData(Object o, String returnType) {
        Object obj;
        switch (returnType) {
            case "integer":
                obj = Integer.parseInt((String) o);
                break;
            case "double":
                obj = Double.parseDouble((String) o);
                break;
            case "long":
                obj = Long.parseLong((String) o);
                break;
            case "string":
                obj = (String) o;
                break;
            default :
                obj = (String) o;
                break;
        }
        return obj;
    }
}
