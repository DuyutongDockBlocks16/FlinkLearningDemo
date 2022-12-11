package com.window;

import java.sql.Timestamp;

public class UvCountPOJO {
    public Long windowStart;
    public Long windowEnd;
    public Long count;
    public String url;

    public UvCountPOJO(){}

    public UvCountPOJO(Long windowStart, Long windowEnd, Long count, String url) {
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.count = count;
        this.url = url;
    }

    @Override
    public String toString(){
        return "UvCount{" +
                "url='" + url + '\'' +
                ", count='" + count + '\'' +
                ", windowStart=" + new Timestamp(windowStart)  +
                ", windowStart=" + new Timestamp(windowEnd)  +
                '}';
    }
}
