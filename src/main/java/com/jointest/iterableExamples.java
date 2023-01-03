package com.jointest;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class iterableExamples {

    public static void main(String[] args) {

// 创建一个hashmap
        Map<String, String> map = new HashMap<String, String>();

        map.put("S1","lisan");
        map.put("S2","duyutong");

//        创建一个Iterable对象
        Iterable iter = new Iterable() {
//          将map中的元素给到Iterable对象，如果要是想取key则写，map.keySet().iterator()
            @Override
            public Iterator iterator() {
                return map.values().iterator();
            }
        };

//      Iterable的遍历，首先将Iterable转化为迭代器iterator

        Iterator iterator = iter.iterator();

//        用这种方式遍历iterator，即可实现Iterable
        while(iterator.hasNext()){
            System.out.println(iterator.next());
        }


    }
}
