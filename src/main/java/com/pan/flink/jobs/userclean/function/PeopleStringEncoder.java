package com.pan.flink.jobs.userclean.function;

import com.pan.flink.jobs.userclean.pojo.People;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;

import java.io.IOException;
import java.io.OutputStream;

/**
 * @author panjb
 */
public class PeopleStringEncoder implements Encoder<People> {
    private final Encoder<String> delegator = new SimpleStringEncoder<>("utf-8");

    @Override
    public void encode(People element, OutputStream stream) throws IOException {
        String s = element.getName() + "," + element.getIdCard() + "," + element.getPhone() + "," + element.getAddress();
        delegator.encode(s, stream);
    }
}
