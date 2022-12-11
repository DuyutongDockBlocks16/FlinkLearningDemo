package com.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

public class boundedSourceTest {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


    //        从文件中读取数据
        DataStreamSource<String> rawData = env.readTextFile("input/clicks.txt");
        rawData.print("rawData");

    //        从集合中读取数据
        ArrayList<Integer> nums = new ArrayList<>();
        nums.add(1);
        nums.add(2);
        DataStreamSource<Integer> numStream = env.fromCollection(nums);
        numStream.print("numStream");

    //        泛型为POJO的集合
        ArrayList<Event> events = new ArrayList<>();
        events.add(new Event("Mary","./home",1000L));
        DataStreamSource<Event> eventDataStreamSource = env.fromCollection(events);
        eventDataStreamSource.print("eventDataStreamSource");

    //        从元素读取读取
        DataStreamSource<Event> mary = env.fromElements(
                new Event("Mary", "./home", 1000L)
        );

        mary.print("fromElements");

        env.execute();


    }


}
