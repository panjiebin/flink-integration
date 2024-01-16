package com.pan.flink.jobs.userclean.function;

import com.pan.flink.jobs.userclean.pojo.People;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @author panjb
 */
public class PeopleMapFunction2 implements MapFunction<String, People> {

    @Override
    public People map(String value) throws Exception {
        try {
            String[] info = value.split(",");
            People people = new People(info[1], info[2], info[3], info[4]);
            people.setId(info[0]);
            String address = people.getAddress();
            int roadIndex = address.indexOf("路");
            if (roadIndex >= 2) {
                people.setRoad(address.substring(roadIndex - 2, roadIndex + 1));
            } else {
                int areaIndex = address.indexOf("区");
                if (areaIndex >= 2) {
                    people.setRoad(address.substring(areaIndex - 2, areaIndex + 1));
                } else {
                    people.setRoad("unknown");
                }
            }
            // 还原空手机号
            if (people.getPhone().length() == 32) {
                people.setPhone("");
            }
            return people;
        } catch (Exception e) {
            return null;
        }
    }
}
