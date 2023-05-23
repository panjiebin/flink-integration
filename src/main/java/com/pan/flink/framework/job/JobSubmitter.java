package com.pan.flink.framework.job;

import com.pan.flink.framework.common.CommonRegistry;
import com.pan.flink.framework.Registry;
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

    public static void submit(String jobPath, String[] args) {
        try {
            Registry<FlinkJobJobBuilder> registry = new CommonRegistry<>(jobPath, FlinkJobJobBuilder.class);
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            String jobs = parameterTool.get(SUBMITTED_JOBS);
            if (StringUtils.isBlank(jobs)) {
                List<FlinkJobJobBuilder> builders = registry.getAll();
                for (FlinkJobJobBuilder builder : builders) {
                    FlinkJob job = builder.build(args);
                    job.execute();
                    if (logger.isInfoEnabled()) {
                        logger.info("Submitted flink jobs: [{}]", job.getName());
                    }
                }
            } else {
                String[] submittedJobs = jobs.split(",");
                for (String builderName : submittedJobs) {
                    FlinkJobJobBuilder builder = registry.get(builderName);
                    if (builder != null) {
                        FlinkJob job = builder.build(args);
                        job.execute();
                        if (logger.isInfoEnabled()) {
                            logger.info("Submitted flink jobs: [{}]", job.getName());
                        }
                    } else {
                        if (logger.isWarnEnabled()) {
                            logger.warn("Could not found flink job builder [{}]", builderName);
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            if (logger.isErrorEnabled()) {
                logger.error("submit flink job error:", e);
            }
        }
    }
}
