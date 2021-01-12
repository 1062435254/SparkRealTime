package com.atguigu.realtime.gmallpublisher.service;

import java.io.IOException;
import java.util.Map;

public interface PublishService {
    //http://localhost:8070/realtime-total?date=2020-10-01
    //http://localhost:8070/realtime-hour?id=dau&date=2020-10-01
    long getDau(String date) throws IOException;

    Map<String, Long> getHourDau(String date) throws IOException;
}
