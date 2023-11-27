package com.pan.flink.framework.job;

import com.pan.flink.framework.Constants;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Basic flink job builder
 *
 * @author panjb
 */
public abstract class BaseFlinkJobBuilder extends AbstractFlinkJobBuilder<ParameterTool> {

    private final static Logger logger = LoggerFactory.getLogger(BaseFlinkJobBuilder.class);

    @Override
    protected ParameterTool convertConfig(ParameterTool jobConfig) {
        return jobConfig;
    }

    @Override
    protected ParameterTool createConfig() {
        return null;
    }

    @Override
    protected void configCheckpoint(StreamExecutionEnvironment env, ParameterTool conf) {
        boolean enable = conf.getBoolean(Constants.CONF_CHK_ENABLE, true);
        if (enable) {
            long interval = conf.getLong(Constants.CONF_CHK_INTERVAL, 5000L);
            String chkDir = conf.get(Constants.CONF_CHK_STORAGE_DIR);
            env.enableCheckpointing(interval, CheckpointingMode.EXACTLY_ONCE);
            if (StringUtils.isNotBlank(chkDir)) {
                env.getCheckpointConfig().setCheckpointStorage(chkDir);
            } else {
                if (logger.isWarnEnabled()) {
                    logger.warn("The checkpoint status backend storage path is empty.");
                }
            }
            env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10000L);
            env.getCheckpointConfig().setCheckpointTimeout(60000L);
            env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
            env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
            env.getCheckpointConfig().setTolerableCheckpointFailureNumber(1);
        }
    }
}
