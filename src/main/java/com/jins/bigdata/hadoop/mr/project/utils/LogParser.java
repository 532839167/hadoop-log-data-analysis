package com.jins.bigdata.hadoop.mr.project.utils;


import org.apache.commons.lang.StringUtils;

import java.util.HashMap;
import java.util.Map;

public class LogParser {

    public Map<String, String> parse(String log) {
        Map<String, String> info = new HashMap<>();
        IPParser ipParser = IPParser.getInstance();

        // 判断log是否为空
        if (StringUtils.isNotBlank(log)) {
            String[] splits = log.split("\001");

            // 每条 ip 第 14 段是 ip 地址
            String ip = splits[13];

            // 解析ip
            String country = "";
            String province = "";
            String city = "";
            IPParser.RegionInfo regionInfo = ipParser.analyseIp(ip);

            if (regionInfo != null) {
                country = regionInfo.getCountry();
                province = regionInfo.getProvince();
                city = regionInfo.getCity();
            }

            info.put("ip", ip);
            info.put("country", country);
            info.put("province", province);
            info.put("city", city);

            String url = splits[1];
            info.put("url", url);

            String time = splits[17];
            info.put("time", time);
        }

        return info;
    }


    public Map<String, String> parseV2(String log) {
        Map<String, String> info = new HashMap<>();
        IPParser ipParser = IPParser.getInstance();

        // 判断log是否为空
        if (StringUtils.isNotBlank(log)) {
            String[] splits = log.split("\t");


            String ip = splits[0];
            String country = splits[1];;
            String province = splits[2];;
            String city = splits[3];

            info.put("ip", ip);
            info.put("country", country);
            info.put("province", province);
            info.put("city", city);

            String url = splits[4];
            info.put("url", url);

            String time = splits[5];
            info.put("time", time);

            String pageId = splits[6];
            info.put("pageId", pageId);
        }

        return info;
    }
}
