package com.pan.flink.jobs.userclean.function;

import cn.hutool.crypto.digest.DigestUtil;
import com.pan.flink.jobs.userclean.pojo.People;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;

/**
 * @author panjb
 */
public class RoadBucketAssigner implements BucketAssigner<People, String> {
    @Override
    public String getBucketId(People people, Context context) {
        return DigestUtil.md5Hex(people.getRoad());
    }

    @Override
    public SimpleVersionedSerializer<String> getSerializer() {
        return SimpleVersionedStringSerializer.INSTANCE;
    }
}
