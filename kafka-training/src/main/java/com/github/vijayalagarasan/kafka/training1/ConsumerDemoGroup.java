package com.github.vijayalagarasan.kafka.training1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoGroup {
    public static void main(String[] args){

        String bootstrapservers = "rk.qa.kafka.str.wsgc.com:80";
        //setting up properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapservers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"my-second-application");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");


        //creating consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //consumer subscribing to topic

        consumer.subscribe(Arrays.asList("DEV2.ARJUN"));

        //consumer receive
        while(true)
        {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record: records){
                System.out.println(
                        "Message: " + record.value() + "\n" +
                        "Partition: " +record.partition() +"\n" +
                        "Offset: " +record.offset() + "\n");
            }

        }


    }
}
