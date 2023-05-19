package com.pan.flink.job;

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
