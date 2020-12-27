package com.subhransu;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.*;

public class CryptoStreamAnalysis {


    public static void main(String[] args) throws InterruptedException {
        String topicName;
        String bootstrapServer;
        String groupId;

        //all the window and slid durations of all 3 analyses
        //time is in minutes
        final Integer BATCH_DURATION = 1;

        final Integer WINDOW_DURATION_1 = 10;
        final Integer SLIDE_DURATION_1 = 5;
        final Integer WINDOW_DURATION_2 = 10;
        final Integer SLIDE_DURATION_2 = 5;
        final Integer WINDOW_DURATION_3 = 10;
        final Integer SLIDE_DURATION_3 = 10;

        //parsing the arguments into local variables, if all arguments are not present return Exception
        if (args.length < 3)
            throw new RuntimeException("Not Enough arguments. Provide <bootstrapServer> <topicName> <groupId>");
        else {
            bootstrapServer = args[0];
            topicName = args[1];
            groupId = args[2];
        }

        //the spark conf object, the master will be set to yarn in commandline
        SparkConf sparkConf = new SparkConf().setAppName("CryptoStreamAnalysis").setMaster("local[*]");

        //properties to connect to the kafka broker
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", bootstrapServer);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", groupId);
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", true);

        //list of topics to subscribe to
        Collection<String> topics = Arrays.asList(topicName);

        //creating JavaStreamingContext object using the spark conf defined earlier with a batch duration of 1 min
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.minutes(BATCH_DURATION));

        //setting the logging level to warn to remove unnecessary logs
        Logger.getRootLogger().setLevel(Level.WARN);

        //creating a DStream using KafkaUtils and the kafka connection properties
        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                );

        //as we need only the value of the kafka stream we map the value into a new DStream
        JavaDStream<String> stringJsonStream = stream.map(record -> record.value());

        //the DStream of JSON String is mapped into object of StockInfo
        JavaDStream<StockInfo> stockInfoJavaDStream = stringJsonStream.map(jsonLine -> {
            ObjectMapper mapper = new ObjectMapper();
            TypeReference<StockInfo> mapType = new TypeReference<StockInfo>() {
            };
            return mapper.readValue(jsonLine, mapType);
        });

        //the resulting DStream of StockInfo is cached as it is to be used by all 3 analyses
        //same records cannot be read by executor 3 times as they belong to same consumer group
        stockInfoJavaDStream.cache();

        //the data in 1 min batch is printed on the console
        stockInfoJavaDStream.print();


        //>> Analysis 1 - Moving Closing Average of all stocks in last 10 mins printed every 5 minutes

        //creates a DStream of stockName and closing value from the parsed object
        JavaPairDStream<String, Double> stockAndClosingValue =
                stockInfoJavaDStream.mapToPair(stockInfo ->
                        new Tuple2<>(stockInfo.getSymbol(), stockInfo.getPriceData().getClose()));

        //creates a DStream based on window duration and slide duration that adds all closing prices in last 10 mins
        // and slides every 5 mins
        JavaPairDStream<String, Double> stockAndAggregatedClosingValue = stockAndClosingValue
                .reduceByKeyAndWindow(new Function2<Double, Double, Double>() {
                    @Override
                    public Double call(Double closingValue1, Double closingValue2) throws Exception {
                        return closingValue1 + closingValue2;
                    }
                }, Durations.minutes(WINDOW_DURATION_1), Durations.minutes(SLIDE_DURATION_1));

        //calculates the average closing value of a sock in last 10 mins by dividing the total sum by 10
        JavaPairDStream<String, Double> stockAndAverageClosingValue = stockAndAggregatedClosingValue
                .mapValues(new Function<Double, Double>() {
                    @Override
                    public Double call(Double aggregatedClosingValue) throws Exception {
                        return aggregatedClosingValue / 10;
                    }
                });

        //prints rdd containing stockName and avg closing price to console
        stockAndAverageClosingValue.toJavaDStream().foreachRDD(new VoidFunction<JavaRDD<Tuple2<String, Double>>>() {
            @Override
            public void call(JavaRDD<Tuple2<String, Double>> tuple2JavaRDD) throws Exception {
                tuple2JavaRDD.foreach(new VoidFunction<Tuple2<String, Double>>() {
                    @Override
                    public void call(Tuple2<String, Double> tuple2) throws Exception {
                        System.out.println("Moving Average Closing price of " + tuple2._1 + " is " + tuple2._2);
                    }
                });
            }
        });


        //>> Analysis 2 - Stock with maximum profit in last 10 min window printed every 5 minutes

        //creates a DStream of stockName and PriceData from the parsed object
        JavaPairDStream<String, PriceData> stockAndPriceData = stockInfoJavaDStream.mapToPair(stock ->
                new Tuple2<>(stock.getSymbol(), stock.getPriceData()));

        //creates a DStream based on window duration and slide duration,
        // that adds all closing prices and opening prices  in last 10 mins
        // and slides every 5 mins
        JavaPairDStream<String, PriceData> stockAndAggregatedPriceData = stockAndPriceData
                .reduceByKeyAndWindow(new Function2<PriceData, PriceData, PriceData>() {
                    @Override
                    public PriceData call(PriceData priceData1, PriceData priceData2) throws Exception {
                        PriceData aggregatedPriceData = new PriceData();
                        aggregatedPriceData.setOpen(priceData1.getOpen() + priceData2.getOpen());
                        aggregatedPriceData.setClose(priceData1.getClose() + priceData2.getClose());
                        return aggregatedPriceData;
                    }
                }, Durations.minutes(WINDOW_DURATION_2), Durations.minutes(SLIDE_DURATION_2));

        //creates a new DStream by calculating the difference between summed closing and opening prices
        //resulting in the profitvalue for each stock in the last 10 min window
        JavaPairDStream<String, Double> stockAndProfit = stockAndAggregatedPriceData
                .mapValues(new Function<PriceData, Double>() {
                    @Override
                    public Double call(PriceData priceData) throws Exception {
                        return (priceData.getClose() - priceData.getOpen());
                    }
                });

        //finds the stock with maximum profit by doing a reduce operation across all rdd and resulting in 1 rdd
        JavaPairDStream<String, Double> stockAndMaxProfit = stockAndProfit.reduce((stock1, stock2) -> {
            if (stock1._2 < stock2._2)
                return stock2;
            else return stock1;
        }).mapToPair(stringDoubleTuple2 -> new Tuple2<>(stringDoubleTuple2._1, stringDoubleTuple2._2));

        //the stock with maximum profit is printed
        stockAndMaxProfit.toJavaDStream().foreachRDD(new VoidFunction<JavaRDD<Tuple2<String, Double>>>() {
            @Override
            public void call(JavaRDD<Tuple2<String, Double>> tuple2JavaRDD) throws Exception {
                tuple2JavaRDD.foreach(new VoidFunction<Tuple2<String, Double>>() {
                    @Override
                    public void call(Tuple2<String, Double> stockNameAndProfit) throws Exception {
                        System.out.println("Stock with highest profit in last interval is " + stockNameAndProfit._1 + " with a profit of " + stockNameAndProfit._2);
                    }
                });
            }
        });

        //>> Analysis 3 - Stock traded in maximum volumes in last 10 min window printed every 10 minutes

        //creates a DStream of stockName and PriceData from the parsed object
        JavaPairDStream<String, PriceData> stockAndVolumeData = stockInfoJavaDStream.mapToPair(stock ->
                new Tuple2<>(stock.getSymbol(), stock.getPriceData()));

        //creates a DStream based on window duration and slide duration,
        // that adds the absolute value of volumes of each stock in last 10 mins
        // and slides every 10 mins
        JavaPairDStream<String, PriceData> stockAndAggregatedVolumeDataInPriceData = stockAndPriceData
                .reduceByKeyAndWindow(new Function2<PriceData, PriceData, PriceData>() {
                    @Override
                    public PriceData call(PriceData priceData1, PriceData priceData2) throws Exception {
                        PriceData aggregatedPriceData = new PriceData();
                        aggregatedPriceData.setVolume(Math.abs(priceData1.getVolume()) + Math.abs(priceData2.getVolume()));
                        return aggregatedPriceData;
                    }
                }, Durations.minutes(WINDOW_DURATION_3), Durations.minutes(SLIDE_DURATION_3));

        //creates a new dstream of stockName and aggregated absolute volume for that stock
        JavaPairDStream<String, Double> stockAndAggregatedVolumeData2 = stockAndAggregatedVolumeDataInPriceData
                .mapToPair(stock -> new Tuple2<String,Double>(stock._1, stock._2.getVolume()));

        //creates a new dstream containing the stock with maximum volume in all rdds
        JavaPairDStream<String,Double> stockAndMaxVolume = stockAndAggregatedVolumeData2
                .reduce((stock1, stock2) -> {
                    if (stock1._2 < stock2._2)
                        return stock2;
                    else return stock1;
                }).mapToPair(stockNameAndVolume -> new Tuple2<>(stockNameAndVolume._1, stockNameAndVolume._2));

        //prints the stockName which has been traded in maximum volume
        stockAndMaxVolume.toJavaDStream().foreachRDD(new VoidFunction<JavaRDD<Tuple2<String, Double>>>() {
            @Override
            public void call(JavaRDD<Tuple2<String, Double>> stockNameAndMaxVolumeTupleJavaRDD) throws Exception {
                stockNameAndMaxVolumeTupleJavaRDD.foreach(new VoidFunction<Tuple2<String, Double>>() {
                    @Override
                    public void call(Tuple2<String, Double> stockNameAndMaxVolumeTuple) throws Exception {
                        System.out.println("Stock with highest volume trade in last interval is " + stockNameAndMaxVolumeTuple._1 + " , with volume " + stockNameAndMaxVolumeTuple._2);
                    }
                });
            }
        });

        //starts the streaming program
        jssc.start();
        //as it is an infintely running program, it waits for termination signal from user
        jssc.awaitTermination();
        //upon receiving the termination signal stops the streaming program
        jssc.close();

    }
}
