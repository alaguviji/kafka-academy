package com.github.vijayalagarasan.kafka.training1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args){
        System.out.println("welcome to Kafka Learning");

        String bootstrapservers = "kafrck-vicn010:32083";
        //setting up properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapservers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        //creating producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        //creating record
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("movie_collection","Grinch is the Christmas Hatter movie");

        //sending data
        producer.send(record);
        producer.flush();
        producer.close();
    }
}
