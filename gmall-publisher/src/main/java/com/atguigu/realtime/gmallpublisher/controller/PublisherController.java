package com.atguigu.realtime.gmallpublisher.controller;

import com.alibaba.fastjson.JSON;
import com.atguigu.realtime.gmallpublisher.service.PublishService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
public class PublisherController {

    @Autowired
    public PublishService service;
    @GetMapping("/realtime-total")
    public String realtimeTotal(String date) throws IOException {
        System.out.println("-----------------");
        long total = service.getDau(date);

        List<Map<String,Object>> result = new ArrayList<> ();
        Map<String,Object> map1 = new HashMap<>();
        map1.put("id", "dau");
        map1.put("name", "新增日活");
        map1.put("value", total);
        Map<String,Object> map2 = new HashMap<>();
        map2.put("id", "new_mid");
        map2.put("name", "新增设备");
        map2.put("value", 233);
        result.add(map1);
        result.add(map2);


        return JSON.toJSONString(result);

    }

    @GetMapping("/realtime-hour")
    public String realtime(String id,String date) throws IOException {
        if("dau".equals(id)){
            Map<String, Long> nowDay = service.getHourDau(date);
            Map<String, Long> yesDay = service.getHourDau(LocalDate.parse(date).plusDays(-1).toString());

            Map<String,Map<String,Long>> result = new HashMap<>();
            result.put("today",nowDay);
            result.put("yesterday",yesDay);


            return JSON.toJSONString(result);
        }
        else{
            return null;
        }
    }
}
