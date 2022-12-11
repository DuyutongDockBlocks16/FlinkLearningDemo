package com.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;

import javax.sound.sampled.Line;

import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

public class BatchWordCount {
    public static void main(String[] args) throws Exception {
        // 1.创建一个执行环境
        ExecutionEnvironment env=ExecutionEnvironment.getExecutionEnvironment();

        // 1.从文件中读取数据
        DataSource<String> LineDataSource = env.readTextFile("input/words.txt");

        FlatMapOperator<String, Tuple2<String, Long>> WordAndOneTuple = LineDataSource.flatMap((String Line, Collector<Tuple2<String, Long>> out) -> {
            String[] words = Line.split(" ");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));

        UnsortedGrouping<Tuple2<String, Long>> WordAndOneGroup = WordAndOneTuple.groupBy(0);

        AggregateOperator<Tuple2<String, Long>> sum = WordAndOneGroup.sum(1);

        sum.print();

    }
}
