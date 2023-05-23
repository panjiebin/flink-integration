package com.pan.flink.framework.job;

import org.apache.flink.api.java.utils.ParameterTool;

import java.io.Serializable;

/**
 * @author panjb
 */
public interface JobConfigLoader extends Serializable {

    /**
     * load configuration
     * @param jobName job name
     * @return job configuration
     * @throws Exception Exception
     */
    ParameterTool load(String jobName) throws Exception;

}
