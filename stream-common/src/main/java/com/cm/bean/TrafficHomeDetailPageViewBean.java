package com.cm.bean;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @Package com.cm.bean.TrafficHomeDetailPageViewBean
 * @Author chen.ming
 * @Date 2025/5/2 10:09
 * @description: 1
 */
@Data
@AllArgsConstructor
public class TrafficHomeDetailPageViewBean {
    // 窗口起始时间
    String stt;
    // 窗口结束时间
    String edt;
    // 当天日期
    String curDate;
    // 首页独立访客数
    Long homeUvCt;
    // 商品详情页独立访客数
    Long goodDetailUvCt;
    // 时间戳
    @JSONField(serialize = false)
    Long ts;
}
