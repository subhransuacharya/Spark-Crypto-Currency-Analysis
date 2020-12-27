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

        //time is in minutes
        final Integer BATCH_DURATION = 1;

        final Integer WINDOW_DURATION_1 = 10;
        final Integer SLIDE_DURATION_1 = 5;
        final Integer WINDOW_DURATION_2 = 10;
        final Integer SLIDE_DURATION_2 = 5;
        final Integer WINDOW_DURATION_3 = 10;
        final Integer SLIDE_DURATION_3 = 10;

//        final Integer WINDOW_DURATION_1 = 10;
//        final Integer SLIDE_DURATION_1 = 1;
//        final Integer WINDOW_DURATION_2 = 10;
//        final Integer SLIDE_DURATION_2 = 1;
//        final Integer WINDOW_DURATION_3 = 10;
//        final Integer SLIDE_DURATION_3 = 1;


        if (args.length < 3)
            throw new RuntimeException("Not Enough arguments. Provide <bootstrapServer> <topicName> <groupId>");
        else {
            bootstrapServer = args[0];
            topicName = args[1];
            groupId = args[2];
        }

        SparkConf sparkConf = new SparkConf().setAppName("CryptoStreamAnalysis").setMaster("local[*]");


        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", bootstrapServer);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", groupId);
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", true);

        Collection<String> topics = Arrays.asList(topicName);
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.minutes(BATCH_DURATION));
        Logger.getRootLogger().setLevel(Level.WARN);
        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                );


        JavaDStream<String> stringJsonStream = stream.map(record -> record.value());

        JavaDStream<StockInfo> stockInfoJavaDStream = stringJsonStream.map(jsonLine -> {
            ObjectMapper mapper = new ObjectMapper();
            TypeReference<StockInfo> mapType = new TypeReference<StockInfo>() {
            };
            return mapper.readValue(jsonLine, mapType);
        });
        stockInfoJavaDStream.cache();
        stockInfoJavaDStream.print();
//        >>                                    1

        JavaPairDStream<String, Double> stockAndClosingValue =
                stockInfoJavaDStream.mapToPair(stockInfo ->
                        new Tuple2<>(stockInfo.getSymbol(), stockInfo.getPriceData().getClose()));

        JavaPairDStream<String, Double> stockAndAggregatedClosingValue = stockAndClosingValue
                .reduceByKeyAndWindow(new Function2<Double, Double, Double>() {
                    @Override
                    public Double call(Double closeValue1, Double closeValue2) throws Exception {
                        return closeValue1 + closeValue2;
                    }
                }, Durations.minutes(WINDOW_DURATION_1), Durations.minutes(SLIDE_DURATION_1));

        JavaPairDStream<String, Double> stockAndAverageClosingValue = stockAndAggregatedClosingValue
                .mapValues(new Function<Double, Double>() {
                    @Override
                    public Double call(Double arg0) throws Exception {
                        return arg0 / 10;
                    }
                })
                .repartition(1);

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


//      >>                                 2
        JavaPairDStream<String, PriceData> stockAndPriceData = stockInfoJavaDStream.mapToPair(stock ->
                new Tuple2<>(stock.getSymbol(), stock.getPriceData()));

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

        JavaPairDStream<String, Double> stockAndProfit = stockAndAggregatedPriceData
                .mapValues(new Function<PriceData, Double>() {
                    @Override
                    public Double call(PriceData priceData) throws Exception {
                        return (priceData.getClose() - priceData.getOpen());
                    }
                });

        JavaPairDStream<String, Double> stockAndMaxProfit = stockAndProfit.reduce((stock1, stock2) -> {
            if (stock1._2 < stock2._2)
                return stock2;
            else return stock1;
        }).mapToPair(stringDoubleTuple2 -> new Tuple2<>(stringDoubleTuple2._1, stringDoubleTuple2._2))
                .repartition(1);

        stockAndMaxProfit.toJavaDStream().foreachRDD(new VoidFunction<JavaRDD<Tuple2<String, Double>>>() {
            @Override
            public void call(JavaRDD<Tuple2<String, Double>> tuple2JavaRDD) throws Exception {
                tuple2JavaRDD.foreach(new VoidFunction<Tuple2<String, Double>>() {
                    @Override
                    public void call(Tuple2<String, Double> tuple2) throws Exception {
                        System.out.println("Stock with highest profit in last interval is " + tuple2._1 + " with a profit of " + tuple2._2);
                    }
                });
            }
        });

//        >>                                            3
        JavaPairDStream<String, PriceData> stockAndVolumeData = stockInfoJavaDStream.mapToPair(stock ->
                new Tuple2<>(stock.getSymbol(), stock.getPriceData()));

        JavaPairDStream<String, PriceData> stockAndAggregatedVolumeDataInPriceData = stockAndPriceData
                .reduceByKeyAndWindow(new Function2<PriceData, PriceData, PriceData>() {
                    @Override
                    public PriceData call(PriceData priceData1, PriceData priceData2) throws Exception {
                        PriceData aggregatedPriceData = new PriceData();
                        aggregatedPriceData.setVolume(Math.abs(priceData1.getVolume()) + Math.abs(priceData2.getVolume()));
                        return aggregatedPriceData;
                    }
                }, Durations.minutes(WINDOW_DURATION_3), Durations.minutes(SLIDE_DURATION_3));

        JavaPairDStream<String, Double> stockAndAggregatedVolumeData2 = stockAndAggregatedVolumeDataInPriceData
                .mapToPair(stock -> new Tuple2<String,Double>(stock._1, stock._2.getVolume()));

        JavaPairDStream<String,Double> stockAndMaxVolume = stockAndAggregatedVolumeData2
                .reduce((stock1, stock2) -> {
                    if (stock1._2 < stock2._2)
                        return stock2;
                    else return stock1;
                }).mapToPair(stringDoubleTuple2 -> new Tuple2<>(stringDoubleTuple2._1, stringDoubleTuple2._2))
                .repartition(1);

        stockAndMaxVolume.toJavaDStream().foreachRDD(new VoidFunction<JavaRDD<Tuple2<String, Double>>>() {
            @Override
            public void call(JavaRDD<Tuple2<String, Double>> tuple2JavaRDD) throws Exception {
                tuple2JavaRDD.foreach(new VoidFunction<Tuple2<String, Double>>() {
                    @Override
                    public void call(Tuple2<String, Double> tuple2) throws Exception {
                        System.out.println("Stock with highest volume trade in last interval is " + tuple2._1 + " , with volume " + tuple2._2);
                    }
                });
            }
        });


        jssc.start();
        jssc.awaitTermination();
        jssc.close();

    }
}
