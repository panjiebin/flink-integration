package com.pan.flink.framework.job;

import org.apache.flink.api.java.utils.ParameterTool;

/**
 * Basic flink job builder
 *
 * @author panjb
 */
public abstract class BaseFlinkJobBuilder extends AbstractFlinkJobBuilder<ParameterTool> {

    @Override
    protected ParameterTool convertConfig(ParameterTool jobConfig) {
        return jobConfig;
    }

    @Override
    protected ParameterTool createConfig() {
        return null;
    }
}
