package com.pan.flink.framework.job;

import com.pan.flink.framework.Constants;
import com.pan.flink.framework.DefaultFileConfigLoader;
import com.pan.flink.utils.ClasspathConfigFileLoader;
import com.pan.flink.framework.ConfigPropertyParser;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Basic flink job builder
 *
 * @param <T> Type of job configuration
 * @author panjb
 */
public abstract class AbstractFlinkJobBuilder<T> implements FlinkJobBuilder {

    private final static Logger logger = LoggerFactory.getLogger(AbstractFlinkJobBuilder.class);

    @Override
    public final FlinkJob build(String[] args) throws Exception {
        // load config
        ParameterTool jobConfig = this.loadJobConfig(args);
        StreamExecutionEnvironment env = this.getExecutionEnvironment(jobConfig);
        if (this.isGlobalConfig()) {
            // 配置全局生效，在算子里面可以直接获取配置
            env.getConfig().setGlobalJobParameters(jobConfig);
        }
        T config = this.convertConfig(jobConfig);
        this.configCheckpoint(env, config);
        this.doBuild(env, config);
        return new DefaultFlinkJob(this.getJobName(), env);
    }

    private StreamExecutionEnvironment getExecutionEnvironment(ParameterTool jobConfig) {
        boolean localWeb = jobConfig.getBoolean(Constants.CONF_MODE_LOCAL, false);
        if (localWeb) {
            Configuration configuration = new Configuration();
            String port = jobConfig.get(Constants.CONF_REST_PORT, "8081");
            configuration.setString(RestOptions.BIND_PORT, port);
            return StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        }
        return StreamExecutionEnvironment.getExecutionEnvironment();
    }

    /**
     * configure checkpoint
     * @param env {@link StreamExecutionEnvironment}
     * @param conf job config
     */
    protected abstract void configCheckpoint(StreamExecutionEnvironment env, T conf);

    /**
     * real build job
     * @param env {@link StreamExecutionEnvironment}
     * @param config {@link ParameterTool} config
     */
    protected abstract void doBuild(StreamExecutionEnvironment env, T config);

    /**
     * Create a job configuration bean
     * @return job configuration bean
     */
    protected abstract T createConfig();

    /**
     * get job name
     * @return job name
     */
    protected abstract String getJobName();

    protected boolean isGlobalConfig() {
        return true;
    }

    /**
     * loads job configuration
     * <p>
     * The configuration consists of 3 parts:
     * <ul>
     *     <li>1. command line parameters</li>
     *     <li>2. job config</li>
     *     <li>3. classpath: config.properties</li>
     * </ul>
     * Configuration priority: command line parameters > job config > config.properties
     * @param args command line parameters
     * @return {@link ParameterTool} job configuration
     * @throws Exception Exception
     */
    private ParameterTool loadJobConfig(String[] args) throws Exception {
        ParameterTool config = ClasspathConfigFileLoader.load("config.properties");
        JobConfigLoader jobConfigLoader = this.getJobConfigLoader();
        if (jobConfigLoader != null) {
            ParameterTool jobConfig = jobConfigLoader.load(this.getJobName());
            if (jobConfig != null) {
                config = null == config ? jobConfig : config.mergeWith(jobConfig);
            }
        } else {
            if (logger.isWarnEnabled()) {
                logger.warn("Could not found a job config loader.");
            }
        }
        config = null == config ? ParameterTool.fromArgs(args): config.mergeWith(ParameterTool.fromArgs(args));
        return config;
    }

    protected T convertConfig(ParameterTool jobConfig) {
        T config = this.createConfig();
        ConfigPropertyParser.parseConfig(config, jobConfig);
        return config;
    }

    protected JobConfigLoader getJobConfigLoader() {
        return new DefaultFileConfigLoader();
    }
}
