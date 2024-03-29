package com.wc;


import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class BoundedStreamWordCount {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);

        DataStreamSource<String> LineDataStream = env.readTextFile("input/words.txt");

        SingleOutputStreamOperator<String> map = LineDataStream.map(Line -> Line+"1");
        map.print();

//        SingleOutputStreamOperator<Tuple2<String, Long>> tuple2SingleOutputStreamOperator = LineDataStream.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
//            String[] words = line.split(" ");
//            for (String word : words) {
//                out.collect(Tuple2.of(word, 1L));
//            }
//        }).
//                returns(Types.TUPLE(Types.STRING,Types.LONG));
//
//        KeyedStream<Tuple2<String, Long>, String> tuple2StringKeyedStream = tuple2SingleOutputStreamOperator.keyBy(data -> data.f0);
//
//        SingleOutputStreamOperator<Tuple2<String, Long>> sum = tuple2StringKeyedStream.sum(1);
//
//        sum.print();

        env.execute();

    }
}
