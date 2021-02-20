package com.github.vijayalagarasan.kafka.training1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;  // Import the File class
import java.io.FileNotFoundException;  // Import this class to handle errors
import java.util.Scanner;

import java.util.Properties;

public class ProducerDemoWithCallback_2 {
    public static void main(String[] args){
        final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback_2.class);

        String bootstrapservers = "rk.sterling.kafka.int.wsgc.com:80";
        //setting up properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapservers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        String data="";
        try {

            File myObj = new File("/Users/valagarasan/Downloads/message.txt");
            Scanner myReader = new Scanner(myObj);
            while (myReader.hasNextLine()) {
                 data = data+ myReader.nextLine();
            }
        }
            catch (FileNotFoundException e) {
                System.out.println("An error occurred.");
                e.printStackTrace();
            }

        //creating producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        String key="20200919110020596318956";
        //creating record
        final ProducerRecord<String, String> record = new ProducerRecord<String, String>("PRD.L.OMS.EVENTS.WORKORDER",key,  data);

        //sending data
        producer.send(record, new Callback() {

                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        System.out.println("test");
                        if (e == null){
                            System.out.println("empty");
                            System.out.println("Published the message to Kafka: " +"\n" +
                                    "Topic: " + recordMetadata.topic() +"\n" +
                                    "Partition: " + recordMetadata.partition() +"\n" +
                                    "Timestamp: " + recordMetadata.timestamp() +"\n" +
                                    "Offset: " + recordMetadata.offset() +"\n"
                            );
                        }else{
                            //error
                            System.out.println("test2");
                            System.out.println(e.fillInStackTrace());
                        }
                    }
                });

        producer.flush();
        producer.close();
    }
}
