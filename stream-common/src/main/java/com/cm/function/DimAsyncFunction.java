package com.cm.function;

import com.alibaba.fastjson.JSONObject;
import com.cm.bean.DimJoinFunction;
import com.cm.constant.Constant;
import com.cm.util.HBaseUtil;
import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.hadoop.hbase.client.AsyncConnection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.concurrent.CompletableFuture;

/**
 * @Package com.cm.function.DimAsyncFunction
 * @Author chen.ming
 * @Date 2025/5/2 14:35
 * @description: 异步hbase
 */
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> implements DimJoinFunction<T> {
    private AsyncConnection hbaseAsyncConn;

    @Override
    public void open(Configuration parameters) throws Exception {
        hbaseAsyncConn = HBaseUtil.getHBaseAsyncConnection();
    }

    @Override
    public void close() throws Exception {
        HBaseUtil.closeAsyncHbaseConnection(hbaseAsyncConn);
    }

    @Override
    public void asyncInvoke(T obj, ResultFuture<T> resultFuture) throws Exception {
        // 创建异步编排对象  执行线程任务，有返回值
        CompletableFuture.supplyAsync(
                new Supplier<JSONObject>() {
                    @Override
                    public JSONObject get() {
                        // 根据当前流中对象获取要关联的维度的主键
                        String key = getRowKey(obj);
                        // 直接从 HBase 中获取维度数据
                        return HBaseUtil.readDimAsync(hbaseAsyncConn, Constant.HBASE_NAMESPACE, getTableName(), key);
                    }
                }
        ).thenApplyAsync(
                new Function<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject apply(JSONObject dimJsonObj) {
                        if (dimJsonObj != null) {
                            System.out.println("~~~从HBase中找到了" + getTableName() + "表的" + getRowKey(obj) + "数据~~~");
                        } else {
                            System.out.println("~~~没有找到" + getTableName() + "表的" + getRowKey(obj) + "数据~~~");
                        }
                        return dimJsonObj;
                    }
                }
        ).thenAcceptAsync(
                new Consumer<JSONObject>() {
                    @Override
                    public void accept(JSONObject dimJsonObj) {
                        if (dimJsonObj != null) {
                            // 将维度对象相关的维度属性补充到流中对象上
                            addDims(obj, dimJsonObj);
                        }
                        // 获取数据库交互的结果并发送给ResultFuture的回调函数，将关联后的数据传递到下游
                        resultFuture.complete(Collections.singleton(obj));
                    }
                }
        );
    }

    // 抽象方法，需要子类实现
}
