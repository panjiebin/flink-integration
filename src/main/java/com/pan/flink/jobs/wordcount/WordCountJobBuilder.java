package com.pan.flink.jobs.wordcount;

import com.pan.flink.framework.annotation.Component;
import com.pan.flink.framework.job.BaseFlinkJobBuilder;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Optional;

/**
 * @author panjb
 */
@Component("wordCount")
public class WordCountJobBuilder extends BaseFlinkJobBuilder {

    @Override
    protected void doBuild(StreamExecutionEnvironment env, ParameterTool config) {
        // just build real job
        String host = config.get("host");
        int port = config.getInt("port");
        DataStreamSource<String> source = env.socketTextStream(host, port);
        source.flatMap(new WordSplitter())
                .name("word-splitter")
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(t -> t.f0)
                .process(new WordCounter())
                .print();
    }

    @Override
    protected String getJobName() {
        return "word-count-job";
    }

    private static class WordSplitter implements FlatMapFunction<String, Tuple2<String, Long>> {
        @Override
        public void flatMap(String line, Collector<Tuple2<String, Long>> collector) throws Exception {
            String[] words = line.split("\\s+");
            for (String word : words) {
                collector.collect(Tuple2.of(word, 1L));
            }
        }
    }

    private static class WordCounter extends KeyedProcessFunction<String, Tuple2<String, Long>, Tuple2<String, Long>> {

        private transient MapState<String, Long> counts;

        @Override
        public void open(Configuration parameters) throws Exception {
            MapStateDescriptor<String, Long> descriptor = new MapStateDescriptor<>("wordCount", Types.STRING, Types.LONG);
            counts = getRuntimeContext().getMapState(descriptor);
        }

        @Override
        public void processElement(Tuple2<String, Long> value, Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {
            long count = Optional.ofNullable(counts.get(value.f0)).orElse(0L);
            counts.put(value.f0, count + value.f1);
            out.collect(Tuple2.of(value.f0, counts.get(value.f0)));
        }
    }
}
