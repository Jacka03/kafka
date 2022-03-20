package com.example.demo.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class ProducerSample {

    public final static String TOPIC_NAME= "kafka_topic";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // 异步发送
        // producerSend();
        // 同步发送
        // producerSyncSend();
        // 异步发送带回调函数
        producerSendWithCallback();
    }

    /**
     * producer 异步发送
     */
    public static void producerSend() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "39.99.152.217:9092");
        properties.put(ProducerConfig.RETRIES_CONFIG,"0");
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG,"16384");
        properties.put(ProducerConfig.LINGER_MS_CONFIG,"1");
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG,"33554432");

        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");


        // producer 主对象
        Producer<String, String> producer = new KafkaProducer<>(properties);

        // 消息对象- producerRecoder
        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, "key-"+i, "value-"+i);
            producer.send(record);
        }

        // 所有打开的通道都要关闭
        producer.close();
    }

    /**
     * 同步发送
     * 异步阻塞发送
     */
    public static void producerSyncSend() throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "39.99.152.217:9092");
        properties.put(ProducerConfig.RETRIES_CONFIG,"0");
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG,"16384");
        properties.put(ProducerConfig.LINGER_MS_CONFIG,"1");
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG,"33554432");

        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");


        // producer 主对象
        Producer<String, String> producer = new KafkaProducer<>(properties);

        // 消息对象- producerRecoder
        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, "key-"+i, "value-"+i);
            Future<RecordMetadata> send = producer.send(record);
            // future 发送出去后就不会管了，出错才会get
            RecordMetadata recordMetadata = send.get();
            // 但是如果每次发送都get，这样就阻塞住了，相当于同步了
            // 其他和异步发送一样
            System.out.println("partition:"+recordMetadata.partition()+","+"offset:"+recordMetadata.offset());
        }

        // 所有打开的通道都要关闭
        producer.close();
    }

    /**
     * producer 异步发送带回调函数
     */
    public static void producerSendWithCallback() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "39.99.152.217:9092");
        properties.put(ProducerConfig.RETRIES_CONFIG,"0");
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG,"16384");
        properties.put(ProducerConfig.LINGER_MS_CONFIG,"1");
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG,"33554432");

        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");


        // producer 主对象
        Producer<String, String> producer = new KafkaProducer<>(properties);

        // 消息对象- producerRecoder
        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, "key-"+i, "value-"+i);
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    System.out.println("partition:"+recordMetadata.partition()+","+"offset:"+recordMetadata.offset());
                }
            });
        }

        // 所有打开的通道都要关闭
        producer.close();
    }
}
