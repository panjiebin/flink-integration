package com.pan.flink.framework.job;

import com.pan.flink.framework.Executable;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * The flink job
 *
 * @author panjb
 */
public class FlinkJob implements Job, Executable {

    private final String name;
    private final StreamExecutionEnvironment environment;

    public FlinkJob(String name, StreamExecutionEnvironment environment) {
        this.name = name;
        this.environment = environment;
    }

    @Override
    public void execute() throws Exception {
        this.environment.execute(name);
    }

    @Override
    public String getName() {
        return this.name;
    }
}
