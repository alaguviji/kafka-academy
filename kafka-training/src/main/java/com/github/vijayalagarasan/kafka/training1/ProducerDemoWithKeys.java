package com.github.vijayalagarasan.kafka.training1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoWithKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        final Logger log = LoggerFactory.getLogger(ProducerDemoWithKeys.class);

        String bootstrapservers = "127.0.0.1:9092";
        //setting up properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapservers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        //creating producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i=11; i <= 20; i++){
        //creating record
            String topic = "movie_collection";
            String key = "id_" + Integer.toString(i);
            String value = "Singam Movie" +key;

        final ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic,key, value);
            System.out.println("Key: " + key);
        //sending data
        producer.send(record, new Callback() {
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e == null){

                            System.out.println("Published the message to Kafka: " +"\n" +
                                    "Topic: " + recordMetadata.topic() +"\n" +
                                    "Partition: " + recordMetadata.partition() +"\n" +
                                    "Timestamp: " + recordMetadata.timestamp() +"\n" +
                                    "Offset: " + recordMetadata.offset() +"\n"
                            );
                        }else{
                            //error
                            log.error("Error while publishing", e);
                        }
                    }
                }).get(); // .get() makes it synchronous this will block the thread. However, this should not be done in production
        }
        producer.flush();
        producer.close();
    }
}
