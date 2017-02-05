package com.Samples.Spark.Basics;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;

/**
 * Created by Amit Reddy on 2/4/2017.
 */
public class SimpleApp {

    public static void main(String[] args){
        String logFile = "C:\\bdt\\spark\\bin\\AmitTest.txt";
        SparkConf conf = new SparkConf().setAppName("Simple App").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> logData = sc.textFile(logFile).cache();

        long numAs = logData.filter(new Function<String, Boolean>() {
        public Boolean call(String s) { return s.contains("A");}
        }).count();

        long numBs = logData.filter(new Function<String, Boolean>() {
            public Boolean call(String s) { return s.contains("B");}
        }).count();

        System.out.println("Lines with A:" + numAs + ", lines with b: " + numBs);

        sc.stop();
    }
}
