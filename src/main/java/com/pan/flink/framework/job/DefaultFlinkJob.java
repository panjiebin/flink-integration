package com.pan.flink.framework.job;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * The flink job
 *
 * @author panjb
 */
public class DefaultFlinkJob implements FlinkJob {

    private final String name;
    private final StreamExecutionEnvironment environment;

    public DefaultFlinkJob(String name, StreamExecutionEnvironment environment) {
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
