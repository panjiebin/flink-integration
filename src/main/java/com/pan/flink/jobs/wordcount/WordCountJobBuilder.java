package com.pan.flink.jobs.wordcount;

import com.pan.flink.framework.annotation.Component;
import com.pan.flink.framework.job.BaseFlinkJobBuilder;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

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
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(t -> t.f0)
                .sum(1)
                .print();
    }

    @Override
    protected String getJobName() {
        return "word-count-job";
    }

    private static class WordSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] words = line.split("\\s+");
            for (String word : words) {
                collector.collect(Tuple2.of(word, 1));
            }
        }
    }
}
