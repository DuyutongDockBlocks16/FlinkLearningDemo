package com.sink;

import com.source.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import javax.print.DocFlavor;
import java.util.Properties;

public class sinktoKafkaTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        Properties properties = new Properties();
        properties.setProperty("bootstraps.servers","hadoop102:9092");
        // 用kafka读取数据
        DataStreamSource<String> clicks = env.addSource(new FlinkKafkaConsumer<String>("clicks", new SimpleStringSchema(), properties));

        // 用kafka生产数据

        SingleOutputStreamOperator<String> result = clicks.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {

                String[] elments = value.split(",");
                return new Event(elments[0].trim(), elments[1].trim(), Long.valueOf(elments[2].trim())).toString();
            }
        });

        result.addSink(new FlinkKafkaProducer<String>("hadoop102:9092","events", new SimpleStringSchema()));

        env.execute();

    }
}
