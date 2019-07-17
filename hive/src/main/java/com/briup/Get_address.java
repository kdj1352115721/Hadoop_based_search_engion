package com.briup;

import org.apache.hadoop.hive.ql.exec.UDF;

//给定手机号，返回归属地
public class Get_address extends UDF {
    public static String evaluate(String number){
        switch (number.substring(0,3)){
            case "138":
                return "北京";
            case "139":
                return "上海";
            case "152":
                return "深圳";
                default:
                    return "未知";
        }
    }
}
