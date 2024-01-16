package com.pan.flink.jobs.userclean.function;

import com.pan.flink.jobs.userclean.pojo.People;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author panjb
 */
public class BestNameSelector extends KeyedProcessFunction<String, People, People> {

    private final OutputTag<String> filterTag = new OutputTag<>("filterTag", Types.STRING);
    private transient ValueState<Tuple2<String, String>> currPeople;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Tuple2<String, String>> descriptor = new ValueStateDescriptor<>("peopleState",
                TypeInformation.of(new TypeHint<Tuple2<String, String>>() {
                }));
        this.currPeople = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(People people, Context ctx, Collector<People> out) throws Exception {
        Tuple2<String, String> curr = currPeople.value();
        if (curr == null) {
            out.collect(people);
            currPeople.update(Tuple2.of(people.getName(), people.getId()));
        } else {
            if (people.getName().length() > curr.f0.length()) {
                // 上一个输出的数据需要被过滤
                ctx.output(filterTag, curr.f1);
                out.collect(people);
                currPeople.update(Tuple2.of(people.getName(), people.getId()));
            }
        }
    }

}
