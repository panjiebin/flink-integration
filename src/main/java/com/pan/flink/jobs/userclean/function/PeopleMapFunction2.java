package com.pan.flink.jobs.userclean.function;

import com.pan.flink.jobs.userclean.pojo.People;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @author panjb
 */
public class PeopleMapFunction2 implements MapFunction<String, People> {

    public static void main(String[] args) throws Exception {
        new PeopleMapFunction2().map("fe93aff6f16d484da465ba2ee58855b6,李笑怡,null,13061702422,上海市普陀区宁夏路353弄2号201室");
    }
    @Override
    public People map(String value) throws Exception {
        String[] info = value.split(",");
        People people = new People(info[1], info[2], info[3], info[4]);
        people.setId(info[0]);
        String address = people.getAddress();
        int roadIndex = address.indexOf("路");
        if (roadIndex != -1) {
            people.setRoad(address.substring(roadIndex - 2, roadIndex + 1));
        } else {
            int areaIndex = address.indexOf("区");
            people.setRoad(address.substring(areaIndex - 2, areaIndex + 1));
        }
        return people;
    }
}
