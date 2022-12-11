package com.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.Random;

public class customSourceTest {
    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//      非并行自定义source
//        DataStreamSource<Event> customStreamSource = env.addSource(new clickSource());
//
//        customStreamSource.print();

//        并行自定义source
        DataStreamSource<Integer> eventDataStreamSource = env.addSource(new myParallelclickSource()).setParallelism(2);
        eventDataStreamSource.print();


        env.execute();

    }

//实现自定义的并行source方法
    public static class myParallelclickSource implements ParallelSourceFunction<Integer>{

        private boolean running = true;
        private Random random = new Random();


        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            while(running){
                ctx.collect(random.nextInt());
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

}

