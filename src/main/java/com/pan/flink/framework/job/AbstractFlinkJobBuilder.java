package com.pan.flink.framework.job;

import com.pan.flink.framework.ConfigLoader;
import com.pan.flink.utils.ClasspathConfigFileLoader;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;

/**
 * @author panjb
 */
public abstract class AbstractFlinkJobBuilder implements FlinkJobJobBuilder {

    private final ConfigLoader configLoader;

    public AbstractFlinkJobBuilder(ConfigLoader configLoader) {
        this.configLoader = configLoader;
    }

    @Override
    public final FlinkJob build(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // load config
        // Configuration priority: command line parameters > job config > config.properties
        ParameterTool commonConfig = loadCommonConfig();
        ParameterTool config = configLoader.load(this.getJobName());
        config = commonConfig.mergeWith(config);
        if (args != null) {
            config = config.mergeWith(ParameterTool.fromArgs(args));
        }
        if (isGlobalConfig()) {
            // 配置全局生效，在算子里面可以直接获取配置
            env.getConfig().setGlobalJobParameters(config);
        }
        this.doBuild(env, config);
        return new FlinkJob(this.getJobName(), env);
    }

    private ParameterTool loadCommonConfig() throws IOException {
        return ClasspathConfigFileLoader.load("config.properties");
    }

    /**
     * real build job
     * @param env {@link StreamExecutionEnvironment}
     * @param config {@link ParameterTool} config
     */
    protected abstract void doBuild(StreamExecutionEnvironment env, ParameterTool config);


    /**
     * get job name
     * @return job name
     */
    protected abstract String getJobName();

    protected boolean isGlobalConfig() {
        return true;
    }
}
