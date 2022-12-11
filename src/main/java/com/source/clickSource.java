package com.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Random;


public class clickSource implements SourceFunction<Event> {

    private boolean running = true;

    @Override
    public void run(SourceContext<Event> ctx) throws Exception {
        Random random = new Random();
        String[] users = {"Du","Mary","Bob","jack"};
        String[] urls = {"./1","./2"};

        while (running){
            String user = users[random.nextInt(users.length)];
            String url = urls[random.nextInt(urls.length)];

            long timestamp = Calendar.getInstance().getTimeInMillis();

            ctx.collect(new Event(user,url,timestamp));

            Thread.sleep(1000L);


        }


    }

    @Override
    public void cancel() {
        running = false;
    }
}
