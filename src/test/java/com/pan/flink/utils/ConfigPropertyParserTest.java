package com.pan.flink.utils;

import com.pan.flink.framework.annotation.ConfigProperty;
import com.pan.flink.framework.ConfigPropertyParser;
import org.apache.flink.api.java.utils.ParameterTool;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;

public class ConfigPropertyParserTest {

    private ParameterTool parameterTool;

    @Before
    public void setUp() throws Exception {
        InputStream is = ConfigPropertyParserTest.class.getClassLoader().getResourceAsStream("test.properties");
        parameterTool = ParameterTool.fromPropertiesFile(is);
    }

    @Test
    public void parseConfig() {
        TestConfig config = new TestConfig();
        ConfigPropertyParser.parseConfig(config, parameterTool);
        Assert.assertEquals("127.0.0.1:9092", config.getServers());
    }

    private static class TestConfig {
        @ConfigProperty("kafka.bootstrap.servers")
        private String servers;
        @ConfigProperty("kafka.group.id")
        private String groupId;
        @ConfigProperty("kafka.topic.test")
        private String testTopic;
        @ConfigProperty("kafka.topic.test.partition")
        private int testTopicPartition;
        @ConfigProperty("kafka.topic.alarm")
        private String alarmTopic;
        @ConfigProperty("kafka.topic.alarm.partition")
        private int alarmTopicPartition;
        @ConfigProperty("watermark.delay.time")
        private int watermarkDelayTime;

        public String getServers() {
            return servers;
        }

        public void setServers(String servers) {
            this.servers = servers;
        }

        public String getGroupId() {
            return groupId;
        }

        public void setGroupId(String groupId) {
            this.groupId = groupId;
        }

        public String getTestTopic() {
            return testTopic;
        }

        public void setTestTopic(String testTopic) {
            this.testTopic = testTopic;
        }

        public int getTestTopicPartition() {
            return testTopicPartition;
        }

        public void setTestTopicPartition(int testTopicPartition) {
            this.testTopicPartition = testTopicPartition;
        }

        public String getAlarmTopic() {
            return alarmTopic;
        }

        public void setAlarmTopic(String alarmTopic) {
            this.alarmTopic = alarmTopic;
        }

        public int getAlarmTopicPartition() {
            return alarmTopicPartition;
        }

        public void setAlarmTopicPartition(int alarmTopicPartition) {
            this.alarmTopicPartition = alarmTopicPartition;
        }

        public int getWatermarkDelayTime() {
            return watermarkDelayTime;
        }

        public void setWatermarkDelayTime(int watermarkDelayTime) {
            this.watermarkDelayTime = watermarkDelayTime;
        }
    }

}