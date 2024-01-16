package com.pan.flink.jobs.userclean.function;

import com.pan.flink.jobs.userclean.pojo.People;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.UUID;

/**
 * @author panjb
 */
public class PeopleMapFunction implements MapFunction<String, People> {
    @Override
    public People map(String value) throws Exception {
        String uuid = UUID.randomUUID().toString().replace("-", "");
        String[] info = value.split(",");
        String name = info[0];
        if (StringUtils.isNotBlank(name)) {
            if (name.matches(".*[a-zA-z]+.*")) {
                name = "";
            } else {
                name = name.replace("\"", "").trim();
            }
        } else {
            name = "";
        }
        People people = new People(uuid);
        people.setName(name);
        if (info.length == 3) {
            people.setPhone(trim(info[1]));
            people.setAddress(trim(info[2]));
        } else {
            people.setIdCard(trim(info[1]));
            people.setPhone(trim(info[2]));
            people.setAddress(trim(info[3]));
        }
        if (StringUtils.isBlank(people.getPhone())) {
            people.setPhone(uuid);
        }
        return people;
    }

    private String trim(String content) {
        return StringUtils.isBlank(content) ? content : content.trim();
    }
}
