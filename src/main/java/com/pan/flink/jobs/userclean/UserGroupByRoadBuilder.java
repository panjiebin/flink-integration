package com.pan.flink.jobs.userclean;

import com.pan.flink.framework.annotation.Component;
import com.pan.flink.framework.job.BaseFlinkJobBuilder;
import com.pan.flink.jobs.userclean.function.PeopleFilter;
import com.pan.flink.jobs.userclean.function.PeopleMapFunction2;
import com.pan.flink.jobs.userclean.function.PeopleStringEncoder;
import com.pan.flink.jobs.userclean.function.RoadBucketAssigner;
import com.pan.flink.jobs.userclean.pojo.People;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.util.Collector;

import java.io.File;
import java.time.Duration;

/**
 * @author panjb
 */
@Component("roadGroup")
public class UserGroupByRoadBuilder extends BaseFlinkJobBuilder {

    @Override
    protected void doBuild(StreamExecutionEnvironment env, ParameterTool config) {
        env.setParallelism(8);
        FileSource<String> source = FileSource.forRecordStreamFormat(new TextLineInputFormat(),
                Path.fromLocalFile(new File("D:/test/result/merge"))
        ).build();
        DataStreamSource<String> sourceStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "csv-file-source");
        SingleOutputStreamOperator<People> peopleStream = sourceStream.map(new PeopleMapFunction2())
                .filter(new PeopleFilter())
                .keyBy(People::getRoad)
                .process(new KeyedProcessFunction<String, People, People>() {
                    @Override
                    public void processElement(People value, Context ctx, Collector<People> out) throws Exception {
                        out.collect(value);
                    }
                });
        DefaultRollingPolicy<People, String> rollingPolicy = DefaultRollingPolicy.builder()
                .withRolloverInterval(Duration.ofMinutes(15L))
                .withInactivityInterval(Duration.ofMinutes(5L))
                .withMaxPartSize(MemorySize.ofMebiBytes(125L))
                .build();
        FileSink<People> fileSink = FileSink.forRowFormat(new Path("D:/test/merge"), new PeopleStringEncoder())
                .withRollingPolicy(rollingPolicy)
                .withBucketAssigner(new RoadBucketAssigner())
                .withOutputFileConfig(OutputFileConfig.builder().withPartSuffix(".csv").build())
                .build();
        peopleStream.sinkTo(fileSink);
    }

    @Override
    protected String getJobName() {
        return "road-group-job";
    }
}
