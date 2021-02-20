package com.github.vijayalagarasan.kafka.training1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Pattern;

public class ConsumerDemoGroup_1 {
    public static void main(String[] args){

        String bootstrapservers = "rk.qa.kafka.str.wsgc.com:80";
        //setting up properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapservers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"my-second-application");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");


        //creating consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //consumer subscribing to topic

     //   consumer.subscribe(Arrays.asList("QA1.B.MDMBW.ITEMFULFILLMENT.*"));
        consumer.subscribe(Pattern.compile("QA1.B.MDMBW.*.PUBLISH"));

        //consumer receive
        while(true)
        {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record: records){
                System.out.println(
                        "Partition: " +record.partition() +"\n" +
                        "Message: " + record.value() + "\n" +
                        "Offset: " +record.offset() + "\n");
            }

        }


    }
}
