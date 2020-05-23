package com.sparkTutorial.dataset;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsFunction;
import org.apache.spark.sql.*;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class TotalPurchaseProblem {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().appName("TotalPurchaseProblem").master("local").getOrCreate();

        Dataset<String> dataset1 = sparkSession.read().textFile("src/main/resources/input/purchases.txt");
        String header = dataset1.first();

        Dataset<String> dataset2 = dataset1.filter((FilterFunction<String>)  x -> !x.equals(header));
        dataset2.show();
        Dataset<Order> dataset3 = dataset2.map((MapFunction<String, Order>) x -> {
            String[] fields = x.split(",");
            List<String> itemsList = Arrays.asList(fields[fields.length - 2].split(":"));
            List<String> priceList = Arrays.asList(fields[fields.length - 1].split(":"));
            List<Double> productsListConverted = priceList.stream().map(Double::valueOf).collect(Collectors.toList());

            return new Order(
                    Integer.valueOf(fields[0]),
                    Long.valueOf(fields[1]),
                    fields[2],
                    itemsList,
                    productsListConverted
            );
        }, Encoders.bean(Order.class));

        Dataset<Tuple2<Long, Double>> dataset4 = dataset3.flatMap((FlatMapFunction<Order, Tuple2<Long, Double>>) order -> {
            Long invoiceId = order.getInvoiceId();
            List<Double> priceList = order.getPriceList();

            List<Tuple2<Long, Double>> list = new ArrayList<>();

            for (Double price : priceList) {
                list.add(new Tuple2<>(invoiceId, price));
            }

            return list.iterator();
        }, Encoders.tuple(Encoders.LONG(), Encoders.DOUBLE()));

        Dataset<InvoiceAndOrder> dataset41 = dataset3.flatMap((FlatMapFunction<Order, InvoiceAndOrder>) order -> {
            Long invoiceId = order.getInvoiceId();
            List<Double> priceList = order.getPriceList();

            List<InvoiceAndOrder> list = new ArrayList<>();

            for (Double price : priceList) {
                list.add(new InvoiceAndOrder(invoiceId, price));
            }

            return list.iterator();
        }, Encoders.bean(InvoiceAndOrder.class));

        KeyValueGroupedDataset<Long, Tuple2<Long, Double>> dataset5 = dataset4.groupByKey((MapFunction<Tuple2<Long, Double>, Long>) Tuple2::_1, Encoders.LONG());
        KeyValueGroupedDataset<Long, InvoiceAndOrder> dataset51 = dataset41.groupByKey((MapFunction<InvoiceAndOrder, Long>) InvoiceAndOrder::getInvoiceId, Encoders.LONG());

        Dataset<Tuple2<Long, Double>> dataset6 = dataset5.mapGroups(
                (MapGroupsFunction<Long, Tuple2<Long, Double>, Tuple2<Long, Double>>) (key, values) -> {
                    Double total = 0.0;
                    while(values.hasNext()) {
                        total += values.next()._2();
                    }

                    return new Tuple2<>(key, total);
                },
                Encoders.tuple(Encoders.LONG(), Encoders.DOUBLE())
        );

        Dataset<InvoiceAndOrder> dataset61 = dataset51.mapGroups((MapGroupsFunction<Long, InvoiceAndOrder, InvoiceAndOrder>) (key, values) -> {
            Double total = 0.0;
            while (values.hasNext()) {
                total += values.next().getPrice();
            }

            return new InvoiceAndOrder(key, total);
        }, Encoders.bean(InvoiceAndOrder.class));

        dataset6.show();
        dataset61.show();
    }
}
