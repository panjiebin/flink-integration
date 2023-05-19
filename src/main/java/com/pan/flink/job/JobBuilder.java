package com.pan.flink.job;


/**
 * A common job builder
 * @param <T>
 * @author panjb
 */
public interface JobBuilder<T extends Job> {

    /**
     * build job
     * @param args CLI args
     * @return job
     * @throws Exception exception
     */
    T build(String[] args) throws Exception;

}
