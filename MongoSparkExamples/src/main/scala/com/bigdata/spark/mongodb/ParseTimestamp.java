package com.bigdata.spark.mongodb;

import java.text.SimpleDateFormat;
import java.util.Date;

public class ParseTimestamp {
    public static void main(String[] args) {
        String timestamp1 = "Thu May 21 2020 11:46:28 GMT-0400 (EDT)";

        String format = "EEE MMM dd yyyy HH:mm:ss 'GMT'Z (z)";
        SimpleDateFormat dateFormat = new SimpleDateFormat(format);
        Date date = new Date();

        String result = dateFormat.format(date);
        System.out.println("Result :: " + result);

    }
}
