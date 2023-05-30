# flink-integration
flink集成框架

## 项目目标
更好的管理flink job代码

## 如何开发flink job
1. 在`com.pan.flink.jobs`创建一个job包
2. 创建一个job builder，继承 `com.pan.flink.framework.job.BaseFlinkJobBuilder`或者`com.pan.flink.framework.job.AbstractFlinkJobBuilder`
3. 使用注解 `Component`标识job builder
> BaseFlinkJobBuilder 和 AbstractFlinkJobBuilder 区别只是使用的配置类不一样  
> BaseFlinkJobBuilder：配置类是ParameterTool  
> AbstractFlinkJobBuilder：配置可以自定义配置类Bean，会自动ParameterTool -> 自定义配置类Bean 转换  

### 示例
```java
@Component("wordCount")
public class WordCountJobBuilder extends BaseFlinkJobBuilder {

    @Override
    protected void doBuild(StreamExecutionEnvironment env, ParameterTool config) {
        // just build real job
        String host = config.get("host");
        int port = config.getInt("port");
        DataStreamSource<String> source = env.socketTextStream(host, port);
        source.flatMap(new WordSplitter())
                .name("word-splitter")
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(t -> t.f0)
                .sum(1)
                .print();
    }

    @Override
    protected String getJobName() {
        return "word-count-job";
    }

    private static class WordSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] words = line.split("\\s+");
            for (String word : words) {
                collector.collect(Tuple2.of(word, 1));
            }
        }
    }
}
```
## 打包并提交作业
### 打包
```shell
mvn -DskipTests=true clean package -P dev
```
例如得到：flink-integration-0.0.1.jar

### 提交job
#### 关于配置
项目可以读取的配置有三个地方：
1. 资源目录下的 `config.properties`，主要用于配置多个job共用的一些配置
2. 资源目录下每个job的配置文件，配置文件名称规范：`jobName.properties`，比如 `word-count-job.properties`
3. 命令行参数  
**三者配置可以互相覆盖，优先级由低到高**

#### 提交job
比如提交到standalone模式的flink集群
```shell
./bin/flink run -m localhost:8081 flink-integration-0.0.1.jar --jobs wordCount --job.package com.pan.flink.jobs
```
> jobs：指定要提交的job，对应的Component定义的名称，可以多个，用逗号隔开，不指定执行包下面的所有job  
> job.package：指定job builder的包，不指定，默认是com.pan.flink.jobs  
> 命令行参数可以指定任何参数，优先级最高