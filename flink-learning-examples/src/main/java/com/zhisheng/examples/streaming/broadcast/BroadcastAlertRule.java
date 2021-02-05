package com.zhisheng.examples.streaming.broadcast;

import com.zhisheng.common.utils.ExecutionEnvUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.List;

/**
 * Desc: 集合变量管广播的情况下 读取该集合的数据后就会 task 就会 finished
 * Created by zhisheng on 2019/10/17 下午4:28
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
@Slf4j
public class BroadcastAlertRule {
    final static MapStateDescriptor<String, String> ALERT_RULE = new MapStateDescriptor<>(
            "alert_rule",
            BasicTypeInfo.STRING_TYPE_INFO,
            BasicTypeInfo.STRING_TYPE_INFO);


    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
        List<String> strings = Arrays.asList("A", "B", "C");
        /**
         * 在使用广播流时，需要使用MapStateDescriptor声明所进行广播的数据样式
         * env的connect方法：是属于broadcast的连接方式
         * 主要包含两种两种连接方式 union和connect
         * 其中union类似于mysql的union all,可以将两个同格式的数据集进行拼接且不去重，但是条件为数据格式要一致。
         *
         * connect则可以不限制数据格式，但是只能进行两个数据流的连接。
         *
         *
         * 一般来说，union的方式更多的使用在数据的合流，connect的方式就适合在使用广播流的方式来广播数据配置
         *这个例子中使用的最简单的方式来进行数据广播，即仅使用广播流来广播固定数据。
         *
         * 在具体的BroadcastProcessFunction中，需要实现如下几个方法
         * processElement：用于处理合并流的方法，由于是处理合并流的逻辑，因此广播流不允许改动
         * processBroadcastElement: 用于处理广播流的内部内容，因此广播流可以变动
         */
        env.socketTextStream("127.0.0.1", 9200)
                .connect(env.fromCollection(strings).broadcast(ALERT_RULE))
                .process(new BroadcastProcessFunction<String, String, String>() {
                    @Override
                    public void processElement(String value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                        ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(ALERT_RULE);
                        if (broadcastState.contains(value)) {
                            out.collect(value);
                        }
                    }

                    @Override
                    public void processBroadcastElement(String value, Context ctx, Collector<String> out) throws Exception {
                        BroadcastState<String, String> broadcastState = ctx.getBroadcastState(ALERT_RULE);
                        broadcastState.put(value, value);
                    }
                })
                .print();

        env.execute();
    }
}
