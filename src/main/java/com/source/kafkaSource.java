package com.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class kafkaSource{
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstraps.servers","hadoop:9092");
        properties.setProperty("group.id","consumer-group");
        properties.setProperty("key.deserializer","org.apche.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer","org.apche.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset","latest");


        DataStreamSource<String> kafkastream = env.addSource(new FlinkKafkaConsumer<String>("clicks", new SimpleStringSchema(), properties));

        kafkastream.print();

        env.execute();

    }
}
