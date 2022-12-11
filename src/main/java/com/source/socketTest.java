package com.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class socketTest {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

//        从socket读取流

        DataStreamSource<String> hadoop102 = env.socketTextStream("DESKTOP-VEQF9SF", 7777);

        hadoop102.print();

        env.execute();



    }

}
