package com.pan.flink.jobs.userclean.function;

import com.pan.flink.jobs.userclean.pojo.People;
import org.apache.flink.api.common.functions.FilterFunction;

/**
 * @author panjb
 */
public class PeopleFilterFunction implements FilterFunction<People> {

    @Override
    public boolean filter(People value) throws Exception {
        return !value.getAddress().contains("医院");
    }
}
