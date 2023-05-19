package com.pan.flink.job;

import com.pan.flink.common.CommonRegistry;
import com.pan.flink.common.Registry;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Scans and builds executable flink jobs in the specified package name.
 *
 * <p>The flink job builder must implement the {@link FlinkJobJobBuilder}.
 * By default, all jobs are submitted.You can specify one or more jobs to be submitted
 * using the {@link #SUBMITTED_JOBS} parameter, separated by commas (,)
 *
 * @author panjb
 */
public class JobSubmitter {

    private final static Logger logger = LoggerFactory.getLogger(JobSubmitter.class);

    private static final String SUBMITTED_JOBS = "jobs";

    private JobSubmitter() {
    }

    public static void run(String jobPath, String[] args) {
        try {
            Registry<FlinkJobJobBuilder> registry = new CommonRegistry<>(jobPath, FlinkJobJobBuilder.class);
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            String jobs = parameterTool.get(SUBMITTED_JOBS);
            if (logger.isInfoEnabled()) {
                logger.info("Submitted flink jobs: [{}]", jobs);
            }
            if (StringUtils.isBlank(jobs)) {
                List<FlinkJobJobBuilder> builders = registry.getAll();
                for (FlinkJobJobBuilder builder : builders) {
                    builder.build(args).execute();
                }
            } else {
                String[] submittedJobs = jobs.split(",");
                for (String job : submittedJobs) {
                    FlinkJobJobBuilder builder = registry.get(job);
                    if (builder != null) {
                        builder.build(args).execute();
                    } else {
                        if (logger.isWarnEnabled()) {
                            logger.warn("Could not found flink job builder [{}]", job);
                        }
                    }
                }
            }
        } catch (Exception e) {
            if (logger.isErrorEnabled()) {
                logger.error("submit flink job error:", e);
            }
        }
    }
}
