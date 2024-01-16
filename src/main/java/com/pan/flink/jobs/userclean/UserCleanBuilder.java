package com.pan.flink.jobs.userclean;

import com.pan.flink.framework.annotation.Component;
import com.pan.flink.framework.function.BeanToStringFunction;
import com.pan.flink.framework.job.BaseFlinkJobBuilder;
import com.pan.flink.jobs.userclean.function.BestNameSelector;
import com.pan.flink.jobs.userclean.function.PeopleFilterFunction;
import com.pan.flink.jobs.userclean.function.PeopleMapFunction;
import com.pan.flink.jobs.userclean.pojo.People;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.util.OutputTag;

import java.io.File;
import java.time.Duration;

/**
 * @author panjb
 */
@Component("userClean")
public class UserCleanBuilder extends BaseFlinkJobBuilder {

    @Override
    protected void doBuild(StreamExecutionEnvironment env, ParameterTool config) {
        env.setParallelism(8);
        FileSource<String> source = FileSource.forRecordStreamFormat(new TextLineInputFormat(),
                Path.fromLocalFile(new File("D:/test/test2"))
        ).build();
        DataStreamSource<String> sourceStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "csv-file-source");
        OutputTag<String> filterTag = new OutputTag<>("filterTag", Types.STRING);

        SingleOutputStreamOperator<People> peopleDataStream = sourceStream.map(new PeopleMapFunction())
                .filter(new PeopleFilterFunction())
                .keyBy(People::getPhone)
                .process(new BestNameSelector());
        peopleDataStream.map(new BeanToStringFunction<>()).sinkTo(this.buildFileSink("D:/test/result/merge"));
        peopleDataStream.getSideOutput(filterTag).sinkTo(this.buildFileSink("D:/test/result/filter")).setParallelism(1);

    }

    private FileSink<String> buildFileSink(String path) {
        DefaultRollingPolicy<String, String> rollingPolicy = DefaultRollingPolicy.builder()
                .withRolloverInterval(Duration.ofMinutes(15L))
                .withInactivityInterval(Duration.ofMinutes(5L))
                .withMaxPartSize(MemorySize.ofMebiBytes(125L))
                .build();
        return FileSink.forRowFormat(new Path(path), new SimpleStringEncoder<String>("utf-8"))
                .withRollingPolicy(rollingPolicy)
                .withOutputFileConfig(OutputFileConfig.builder().withPartSuffix(".csv").build())
                .build();
    }


    @Override
    protected String getJobName() {
        return "user-clean-job";
    }
}
