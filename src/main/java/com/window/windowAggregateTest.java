package com.window;

import com.source.Event;
import com.source.clickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.sql.Timestamp;

public class windowAggregateTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setAutoWatermarkInterval(100);

        SingleOutputStreamOperator<Event> stream = env.addSource(new clickSource())
                        .assignTimestampsAndWatermarks(WatermarkStrategy.
                        <Event>forMonotonousTimestamps().
                        withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.timestamp;
                    }
                }))
                ;

        stream.keyBy(data->data.user).
                window(TumblingEventTimeWindows.of(Time.seconds(10))).
                aggregate(new AggregateFunction<Event, Tuple3<String, Long,Integer>, String>() {

                    @Override
                    public Tuple3<String, Long, Integer> createAccumulator() {
                        return Tuple3.of("",0L,0);
                    }

                    @Override
                    public Tuple3<String, Long, Integer> add(Event value, Tuple3<String, Long, Integer> accumulator) {
                        return Tuple3.of(value.user, accumulator.f1+value.timestamp, accumulator.f2+1);
                    }

                    @Override
                    public String getResult(Tuple3<String, Long, Integer> accumulator) {
                        Timestamp timestamp = new Timestamp(accumulator.f1/accumulator.f2);
                        return accumulator.f0+timestamp.toString();
                    }

                    @Override
                    public Tuple3<String, Long, Integer> merge(Tuple3<String, Long, Integer> a, Tuple3<String, Long, Integer> b) {
                        return Tuple3.of(a.f0,a.f1+b.f1,a.f2+b.f2);
                    }
                }).
                print();

        env.execute();
    }
}
