package com.atguigu.realtime.gmallpublisher.service;

import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Service
public class PublisherServiceImp implements PublishService {

    @Autowired
    JestClient es;

    @Override
    public long getDau(String date) throws IOException {
        //查询语句
        Search search = new Search.Builder(Query.str1())
                .addIndex("gmall_dau_info_" + date)
                .addType("_doc")
                .build();

        //提交search
        SearchResult result = es.execute(search);
        Long total = result.getTotal();
        System.out.println(total);
        return total;
    }

    @Override
    public Map<String, Long> getHourDau(String date) throws IOException {
        Search search = new Search.Builder(Query.str2())
                .addIndex("gmall_dau_info_" + date)
                .addType("_doc")
                .build();
        SearchResult result = es.execute(search);
        TermsAggregation agg = result.getAggregations()
                .getTermsAggregation("groupby_hour");
        HashMap<String, Long> map = new HashMap<>();
        if (agg != null) {
            for (TermsAggregation.Entry bucket : agg.getBuckets()) {
                String key = bucket.getKey();
                Long count = bucket.getCount();
                map.put(key,count);
            }
        }

        return map;
    }


}
