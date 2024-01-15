# flink-integration
flink集成框架

# Flink Job 开发
Flink Job程序结构有很多共同之处，项目尝试对这些相似之处进行封装，以简化job开发和减少代码重复

## Job开发说明
1. flink job代码统一写到`com.pan.flink.jobs`包下，为每个job创建一个单独的package，比如`com.pan.flink.jobs.wordcount`
2. 编写一个FlinkJobBuilder实现类，继承 `com.pan.flink.framework.job.BaseFlinkJobBuilder`或者`com.pan.flink.framework.job.AbstractFlinkJobBuilder`
3. 使用注解 `Component`标识job builder，执行Job时会用到
4. 主要实现`doBuild()`和`getJobName()`两个方法
   1) `doBuild()`：根据实际业务编写数据处理逻辑（DataFlows）,注意不要执行`StreamExecutionEnvironment.execute()`，项目会统一调用
   2) `getJobName()`：返回作业名称，有2个用处：一是作为执行的作业名称，二是根据jobName获取配置
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
              .returns(Types.TUPLE(Types.STRING, Types.LONG))
              .keyBy(t -> t.f0)
              .process(new WordCounter())
              .print();
   }

   @Override
   protected String getJobName() {
      return "word-count-job";
   }

   private static class WordSplitter implements FlatMapFunction<String, Tuple2<String, Long>> {
      @Override
      public void flatMap(String line, Collector<Tuple2<String, Long>> collector) throws Exception {
         String[] words = line.split("\\s+");
         for (String word : words) {
            collector.collect(Tuple2.of(word, 1L));
         }
      }
   }

   private static class WordCounter extends KeyedProcessFunction<String, Tuple2<String, Long>, Tuple2<String, Long>> {

      private transient MapState<String, Long> counts;

      @Override
      public void open(Configuration parameters) throws Exception {
         MapStateDescriptor<String, Long> descriptor = new MapStateDescriptor<>("wordCount", Types.STRING, Types.LONG);
         counts = getRuntimeContext().getMapState(descriptor);
      }

      @Override
      public void processElement(Tuple2<String, Long> value, Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {
         long count = Optional.ofNullable(counts.get(value.f0)).orElse(0L);
         counts.put(value.f0, count + value.f1);
         out.collect(Tuple2.of(value.f0, counts.get(value.f0)));
      }
   }
}
```
## Job配置
项目支持读取的配置有3种方式：
1) `config.properties`：项目的公共配置，各个job共享，优先级最低
2) `jobName.properties`：每个job的独有配置，可以从classpath下读取，也可以外部（配置中心）读取
    > 根据需要实现`com.pan.flink.framework.job.JobConfigLoader`，默认从classpath下读取
3) 命令行参数，优先级最高  
3种配置方式，可以相互覆盖，优先级：命令行参数 > jobName.properties > config.properties

## 部署&执行Job
`com.pan.flink.FlinkJobApplication`是项目Job执行入口，通过`--jobs wordCount`指定要提交的Job。`wordCount`是Job的唯一标识，即`@Component`配置的值。
对于session模式，可以一次性提交多个Job，用逗号隔开

## 启动本地Web UI
添加配置，IDE正常启动Job后，可以再本地访问Flink web UI
```properties
local=true
rest.port=8081
```
浏览器访问：http://localhost:8081

# Flink on YARN 部署命令

## Application Mode

### 1 启动
```shell
./bin/flink run-application -t yarn-application \
-Dyarn.application.name=word-count-job \
-Djobmanager.memory.process.size=1G \
-Dtaskmanager.memory.process.size=2G \
-Dtaskmanager.numberOfTaskSlots=1 \
hdfs:///flink-jobs/flink-integration-0.0.1.jar \
--jobs wordCount 
```
> 更多参数配置，参见官网：https://nightlies.apache.org/flink/flink-docs-stable/ops/config.html

### 2 取消并生成savepoint
```shell
./bin/flink cancel -t yarn-application -Dyarn.application.id=application_XXXX_YY <jobId>
```
### 3 从savepoint恢复任务
```shell
./bin/flink run-application -t yarn-application \
-s hdfs:///flink/savepoints/savepoint-<jobId> \
-Dyarn.application.name=word-count-job \
-Djobmanager.memory.process.size=1G \
-Dtaskmanager.memory.process.size=2G \
-Dtaskmanager.numberOfTaskSlots=1 \
hdfs:///flink-jobs/flink-integration-0.0.1.jar \
--jobs wordCount 
```
>除了可以从savepoint恢复，也可以从最近一次checkpoint恢复  
> 只需配置: `-s <checkpointPath>`