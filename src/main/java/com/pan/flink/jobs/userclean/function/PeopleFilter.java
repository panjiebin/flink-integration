package com.pan.flink.jobs.userclean.function;

import com.pan.flink.jobs.userclean.pojo.People;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * @author panjb
 */
public class PeopleFilter extends RichFilterFunction<People> {

    private transient Set<String> ids;

    @Override
    public void open(Configuration parameters) throws Exception {
        ParameterTool config = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        String filterPath = Optional.of(config.get("filter.path")).get();
        this.ids = new HashSet<>();
        File file = new File(filterPath);
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(file));
            String line;
            while ((line = br.readLine()) != null) {
                if (StringUtils.isNotBlank(line)) {
                    ids.add(line);
                }
            }
        } finally {
            if (br != null) {
                br.close();
            }
        }
    }

    @Override
    public boolean filter(People value) throws Exception {
        return value != null && !ids.contains(value.getId());
    }
}
