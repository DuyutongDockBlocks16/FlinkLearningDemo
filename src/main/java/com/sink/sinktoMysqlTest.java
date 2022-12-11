package com.sink;

import com.source.Event;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class sinktoMysqlTest {
    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Event> stream = env.fromElements(
                new Event("bob", "./home", 1000L),
                new Event("bob", "./home", 1000L),
                new Event("bob", "./home", 1000L),
                new Event("bob", "./home", 1000L),
                new Event("bob", "./home", 1000L),
                new Event("bob", "./home", 1000L),
                new Event("bob", "./home", 1000L),
                new Event("bob", "./home", 1000L)
        );

        stream.addSink(JdbcSink.sink(
                "INSERT INTO clicks (user,url) VALUES (?, ?)",
                ((statement, event)-> {
                    statement.setString(1, event.user);
                    statement.setString(2, event.url);
                }),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder().
                        withUrl("jdbc:mysql://localhost:3306/test").
                        withDriverName("com.mysql.jdbc.Driver").
                        withUsername("root").
                        withPassword("root").build()
        ));

        env.execute();

    }
}
