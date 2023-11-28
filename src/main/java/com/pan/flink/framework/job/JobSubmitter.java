package com.pan.flink.framework.job;

import com.pan.flink.framework.CommonRegistry;
import com.pan.flink.framework.Constants;
import com.pan.flink.framework.Registry;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Scans and builds executable flink jobs in the specified package name.
 *
 * <p>The flink job builder must implement the {@link FlinkJobBuilder}.
 * By default, all jobs are submitted.You can specify one or more jobs to be submitted
 * using the {@link Constants#CONF_SUBMITTED_JOBS} parameter, separated by commas (,)
 *
 * @author panjb
 */
public class JobSubmitter {

    private final static Logger logger = LoggerFactory.getLogger(JobSubmitter.class);

    private JobSubmitter() {
    }

    public static void submit(String jobPath, String[] args) {
        try {
            Registry<FlinkJobBuilder> registry = new CommonRegistry<>(jobPath, FlinkJobBuilder.class);
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            String jobs = parameterTool.get(Constants.CONF_SUBMITTED_JOBS);
            if (StringUtils.isBlank(jobs)) {
                if (logger.isInfoEnabled()) {
                    logger.info("No job specified to submit.");
                }
                return;
            }
            if (Constants.VAL_SUBMITTED_JOBS_ALL.equals(jobs)) {
                List<FlinkJobBuilder> builders = registry.getAll();
                for (FlinkJobBuilder builder : builders) {
                    buildAndExecJob(args, builder);
                }
            } else {
                String[] submittedJobs = jobs.split(",");
                for (String builderName : submittedJobs) {
                    FlinkJobBuilder builder = registry.get(builderName);
                    if (builder != null) {
                        buildAndExecJob(args, builder);
                    } else {
                        if (logger.isWarnEnabled()) {
                            logger.warn("Could not found flink job builder [{}]", builderName);
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

    private static void buildAndExecJob(String[] args, FlinkJobBuilder builder) throws Exception {
        FlinkJob job = builder.build(args);
        job.execute();
        if (logger.isInfoEnabled()) {
            logger.info("Submitted flink jobs: [{}]", job.getName());
        }
    }
}
