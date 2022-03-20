package com.example.demo.admin;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class AdminSample {
    public final static String TOPIC_NAME = "kafka_topic";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // AdminClient adminClient = AdminSample.adminClient();
        // System.out.println(adminClient);

        // createTopic();

        topicLists();

        // delTopic();

        // topicLists();

        describeTopic();

        // alterConfig();
        // describeConfig();


        // incrPartition(2);
    }

    /**
     * 设置adminClient
     * @return
     */
    public static AdminClient adminClient() {
        Properties properties = new Properties();
        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "39.99.152.217:9092");
        AdminClient adminClient = AdminClient.create(properties);
        return adminClient;
    }

    /**
     * 创建topic实例
     */
    public static void createTopic() {
        AdminClient adminClient = adminClient();

        //
        short rs =  1;
        NewTopic newTopic = new NewTopic(TOPIC_NAME, 1, rs);

        CreateTopicsResult topics = adminClient.createTopics(Arrays.asList(newTopic));
        System.out.println("CreateTopicsResult" + topics.toString());
    }

    /**
     * 获取topic列表
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public static void topicLists() throws ExecutionException, InterruptedException {
        AdminClient adminClient = adminClient();

        // 是否查看options
        ListTopicsOptions options = new ListTopicsOptions();
        options.listInternal(true);
        // ListTopicsResult listTopicsResult = adminClient.listTopics();
        ListTopicsResult listTopicsResult = adminClient.listTopics(options);

        Set<String> strings = listTopicsResult.names().get();
        strings.stream().forEach(System.out::println);
        Collection<TopicListing> topicListings = listTopicsResult.listings().get();
        topicListings.stream().forEach((topicList) -> {
            System.out.println(topicList);
        });

    }

    /**
     * 删除topic
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public static void delTopic() throws ExecutionException, InterruptedException {
        AdminClient adminClient = adminClient();
        DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(Arrays.asList(TOPIC_NAME));
        deleteTopicsResult.all().get();
    }

    /**
     * 查看描述信息
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public static void describeTopic() throws ExecutionException, InterruptedException {
        AdminClient adminClient = adminClient();
        DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Arrays.asList("comment"));
        Map<String, TopicDescription> stringTopicDescriptionMap = describeTopicsResult.all().get();
        Set<Map.Entry<String, TopicDescription>> entries = stringTopicDescriptionMap.entrySet();

        entries.stream().forEach((entry) -> {
            System.out.println("name: " + entry.getKey() + ", " + "value: " + entry.getValue());
        });
    }

    /**
     * 查看config
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public static void describeConfig() throws ExecutionException, InterruptedException {
        AdminClient adminClient = adminClient();

        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME);
        DescribeConfigsResult describeConfigsResult = adminClient.describeConfigs(Arrays.asList(configResource));
        Map<ConfigResource, Config> configResourceConfigMap = describeConfigsResult.all().get();
        Set<Map.Entry<ConfigResource, Config>> entries = configResourceConfigMap.entrySet();
        entries.stream().forEach((entry) -> {
            System.out.println("name: " + entry.getKey() + ", " + "value: " + entry.getValue());
        });
    //    name: ConfigResource(type=TOPIC, name='kafka_topic'),
        //    value: Config(entries=[ConfigEntry(name=compression.type, value=producer, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
        //    ConfigEntry(name=leader.replication.throttled.replicas, value=, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
        //    ConfigEntry(name=min.insync.replicas, value=1, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
        //    ConfigEntry(name=message.downconversion.enable, value=true, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
        //    ConfigEntry(name=segment.jitter.ms, value=0, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
        //    ConfigEntry(name=cleanup.policy, value=delete, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
        //    ConfigEntry(name=flush.ms, value=9223372036854775807, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
        //    ConfigEntry(name=follower.replication.throttled.replicas, value=, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
        //    ConfigEntry(name=segment.bytes, value=1073741824, source=STATIC_BROKER_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
        //    ConfigEntry(name=retention.ms, value=604800000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
        //    ConfigEntry(name=flush.messages, value=9223372036854775807, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
        //    ConfigEntry(name=message.format.version, value=2.8-IV1, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
        //    ConfigEntry(name=max.compaction.lag.ms, value=9223372036854775807, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=file.delete.delay.ms, value=60000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=max.message.bytes, value=1048588, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=min.compaction.lag.ms, value=0, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=message.timestamp.type, value=CreateTime, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
        //    ConfigEntry(name=preallocate, value=false, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=index.interval.bytes, value=4096, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=min.cleanable.dirty.ratio, value=0.5, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=unclean.leader.election.enable, value=false, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=retention.bytes, value=-1, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=delete.retention.ms, value=86400000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=segment.ms, value=604800000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=message.timestamp.difference.max.ms, value=9223372036854775807, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=segment.index.bytes, value=10485760, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])])

    }

    /**
     * 修改config信息
     */
    public static void alterConfig() {
        AdminClient adminClient = adminClient();
        Map<ConfigResource, Config> configMap = new HashMap<>();
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME);
        Config config = new Config(Arrays.asList(new ConfigEntry("preallocate", "true")));
        configMap.put(configResource, config);
        adminClient.alterConfigs(configMap);
    }

    /**
     * 修改partition数量
     * @param partitions
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public static void incrPartition(int partitions) throws ExecutionException, InterruptedException {
        AdminClient adminClient = adminClient();

        Map<String, NewPartitions> map = new HashMap<>();
        NewPartitions newPartitions = NewPartitions.increaseTo(partitions);
        map.put(TOPIC_NAME, newPartitions);
        CreatePartitionsResult partitionsResult = adminClient.createPartitions(map);
        partitionsResult.all().get();


    }
}
