package com.pan.flink;

import com.pan.flink.framework.job.JobSubmitter;
import org.apache.flink.api.java.utils.ParameterTool;

/**
 * flink jobs submission entry
 * @author panjb
 */
public class FlinkJobApplication {

    public static final String JOB_PACKAGE = "job.package";

    public static void main(String[] args) {
        ParameterTool config = ParameterTool.fromArgs(args);
        String jobPackage = config.get(JOB_PACKAGE, "com.pan.flink.jobs");
        JobSubmitter.submit(jobPackage, args);
    }
}
