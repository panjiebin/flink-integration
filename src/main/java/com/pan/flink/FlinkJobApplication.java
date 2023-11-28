package com.pan.flink;

import com.pan.flink.framework.Constants;
import com.pan.flink.framework.job.JobSubmitter;
import org.apache.flink.api.java.utils.ParameterTool;

/**
 * flink jobs submission entry
 * @author panjb
 */
public class FlinkJobApplication {


    public static void main(String[] args) {
        ParameterTool config = ParameterTool.fromArgs(args);
        String jobPackage = config.get(Constants.CONF_JOB_PKG, "com.pan.flink.jobs");
        JobSubmitter.submit(jobPackage, args);
    }
}
