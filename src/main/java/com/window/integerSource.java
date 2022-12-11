package com.window;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

public class integerSource implements SourceFunction<Tuple2<Long, Integer>> {

    private boolean running = true;

    @Override
    public void run(SourceContext<Tuple2<Long, Integer>> ctx) throws Exception {
        Random random = new Random();



        while (running) {
            int num = random.nextInt(9);

            long timestamp = Calendar.getInstance().getTimeInMillis();

            ctx.collect(Tuple2.of(timestamp, num));

            Thread.sleep(1000L);

        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
