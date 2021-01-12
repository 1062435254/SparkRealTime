package com.atguigu.realtime.gmallpublisher;

import com.atguigu.realtime.gmallpublisher.service.Query;

import java.time.LocalDate;

public class Test {

    public static void main(String[] args) {
        System.out.println(LocalDate.parse("2020-01-08").plusDays(-1).toString());
    }
}
