package com.github.vijayalagarasan.kafka.training1;
import  java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
//import simple producer packages
import org.apache.kafka.clients.producer.Producer;

//import KafkaProducer packages
import org.apache.kafka.clients.producer.KafkaProducer;

//import ProducerRecord packages
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

//Create java class named “SimpleProducer”

    public class SampleProducer {

        public static void main(String[] args) throws Exception{

            // Check arguments length value
            //Assign topicName to string variable
            String topicName = "QA2.L.STERLING.NOTIFICATIONS.PUBLISH";

            // create instance for properties to access producer configs
            //Properties properties = new Properties();
            //Properties properties = new Properties();
            Properties props = new Properties();

            //Assign localhost id
            props.put("bootstrap.servers", "rk.qa.kafka.int.wsgc.com:80");

            //Set acknowledgements for producer requests.
            props.put("acks", "all");

            //If the request fails, the producer can automatically retry,
       //     props.put("retries", 0);

            //Specify buffer size in config
        //    props.put("batch.size", 16384);

            //Reduce the no of requests less than 0
        //    props.put("linger.ms", 1);
//
            //The buffer.memory controls the total amount of memory available to the producer for buffering.
        //    props.put("buffer.memory", 33554432);

            props.put("key.serializer",
                    StringSerializer.class.getName());

            props.put("value.serializer",
                    StringSerializer.class.getName());

            Producer<String, String> producer = new KafkaProducer<String, String>(props);

            for(int i = 0; i < 10; i++){
                System.out.println("inside loop");

                final ProducerRecord<String, String> record = new ProducerRecord<String, String>(topicName,"{\"widget\": {\n" +
                        "    \"debug\": \"on\" }\n" +
                        "    }\n" + Integer.toString(i));

                producer.send(record, new Callback() {
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
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
                            System.out.println("Error while publishing"+ e.toString());
                        }
                    }
                });

                //System.out.println("Insert FOR");
                //producer.send(new ProducerRecord<String, String>(topicName,
                //  Integer.toString(i), Integer.toString(i)));
            }

            System.out.println("Message sent successfully");
            producer.flush();
            producer.close();
        }
    }
