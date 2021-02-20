package com.github.vijayalagarasan.kafka.training1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.FileInputStream;
import sun.misc.IOUtils;

import java.util.Properties;

public class ProducerDemoWithCallback {
    public static void main(String[] args){
        final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

        String bootstrapservers = "kafrck-vicn010:32083";
        //setting up properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapservers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i=1; i <= 1; i++){
        //creating record
       // final ProducerRecord<String, String> record = new ProducerRecord<String, String>("QA2.B.BW.INVENTORYSNAPSHOT.PUBLISH", value: eveything);

        //final ProducerRecord record = new ProducerRecord<String, String>("QA1.B.MDMBW.ITEMMASTER.PUBLISH");
        //sending data

            String str = "{\n" +
                    "  \"dispatchRequest\": {\n" +
                    "    \"version\": \"2.0.0\",\n" +
                    "    \"systemContext\": {\n" +
                    "      \"environmentIdentifier\": \"STST\",\n" +
                    "      \"regionIdentifier\": \"west\",\n" +
                    "      \"sourceSystemIdentifier\": \"stlrck-virh006.wsgc.com\",\n" +
                    "      \"transactionId\": \"797987897987987987987987\",\n" +
                    "      \"transactionTime\": \"2019-06-21T13:46:23\"\n" +
                    "    },\n" +
                    "    \"configId\": \"PK.AET_123\",\n" +
                    "    \"structuredParameters\": [\n" +
                    "      {\n" +
                    "        \"key\": \"orderInfo\",\n" +
                    "        \"value\": {\n" +
                    "          \"data\": {\n" +
                    "            \"conceptCode\": \"PK\",\n" +
                    "            \"omsOrderId\": \"676767676767123CUSTOMER_PICKED_UP_TEST\",\n" +
                    "            \"type\": \"SALES\",\n" +
                    "            \"orderCreatedTime\": \"2019-06-03T01:38:58-07:00\",\n" +
                    "            \"templateId\": \"123\",\n" +
                    "            \"billingAddress\": {\n" +
                    "              \"name\": {\n" +
                    "                \"firstName\": \"JOHN\",\n" +
                    "                \"lastName\": \"DENVER\"\n" +
                    "              },\n" +
                    "              \"address\": {\n" +
                    "                \"addressLines\": [\n" +
                    "                  \"151 Union St\"\n" +
                    "                ],\n" +
                    "                \"city\": \"San Francisco\",\n" +
                    "                \"stateProvince\": \"CA\",\n" +
                    "                \"postalCode\": \"94111\",\n" +
                    "                \"country\": \"US\",\n" +
                    "                \"verificationStatus\": \"verified\"\n" +
                    "              },\n" +
                    "              \"contacts\": [\n" +
                    "                {\n" +
                    "                  \"phone\": \"5504455044\"\n" +
                    "                },\n" +
                    "                {\n" +
                    "                  \"email\": \"rkumar13@wsgc.com\"\n" +
                    "                }\n" +
                    "              ]\n" +
                    "            },\n" +
                    "            \"totals\": [\n" +
                    "              {\n" +
                    "                \"type\": \"Order\",\n" +
                    "                \"amount\": 2214\n" +
                    "              },\n" +
                    "              {\n" +
                    "                \"type\": \"LineShippingEffective\",\n" +
                    "                \"amount\": 16\n" +
                    "              },\n" +
                    "              {\n" +
                    "                \"type\": \"LineMerchEffective\",\n" +
                    "                \"amount\": 2198\n" +
                    "              },\n" +
                    "              {\n" +
                    "                \"type\": \"MerchTax\",\n" +
                    "                \"amount\": 0\n" +
                    "              },\n" +
                    "              {\n" +
                    "                \"type\": \"ShippingTax\",\n" +
                    "                \"amount\": 0\n" +
                    "              }\n" +
                    "            ],\n" +
                    "            \"payments\": [\n" +
                    "              {\n" +
                    "                \"id\": \"201906030142392152032638\",\n" +
                    "                \"tender\": {\n" +
                    "                  \"type\": {\n" +
                    "                    \"code\": \"CREDIT_CARD\",\n" +
                    "                    \"subCode\": \"MASTERCARD\"\n" +
                    "                  },\n" +
                    "                  \"hashes\": {},\n" +
                    "                  \"maskedAccountNumber\": \"545454******5454\"\n" +
                    "                },\n" +
                    "                \"cardMetadata\": {\n" +
                    "                  \"cardHolderFullName\": \"JOHN DENVER\",\n" +
                    "                  \"cardExpiration\": \"7/2021\"\n" +
                    "                },\n" +
                    "                \"amount\": 2390.73\n" +
                    "              }\n" +
                    "            ],\n" +
                    "            \"subOrders\": [\n" +
                    "              {\n" +
                    "                \"omsSubOrderId\": \"01\",\n" +
                    "                \"pickup\": {\n" +
                    "                  \"fullName\": \"My Store\",\n" +
                    "                  \"location\": \"ST:123\",\n" +
                    "                  \"storeName\": \"WSChestnutStreet\",\n" +
                    "                  \"storePhone\": \"202986128\",\n" +
                    "                  \"address\": {\n" +
                    "                    \"addressLines\": [\n" +
                    "                      \"668 Provence Dr\"\n" +
                    "                    ],\n" +
                    "                    \"city\": \"Birmingham\",\n" +
                    "                    \"stateProvince\": \"AL\",\n" +
                    "                    \"postalCode\": \"35242\",\n" +
                    "                    \"country\": \"US\",\n" +
                    "                    \"verificationStatus\": \"Y\",\n" +
                    "                    \"classification\": \"CS\"\n" +
                    "                  },\n" +
                    "                  \"contacts\": [\n" +
                    "                    {\n" +
                    "                      \"phone\": \"9159998006\"\n" +
                    "                    },\n" +
                    "                    {\n" +
                    "                      \"email\": \"rkumar13@wsgc.com\"\n" +
                    "                    }\n" +
                    "                  ]\n" +
                    "                },\n" +
                    "                \"serviceLevel\": \"PICKUP_INTERNAL\",\n" +
                    "                \"isGift\": true,\n" +
                    "                \"processing\": [\n" +
                    "                  {\n" +
                    "                    \"model\": \"PICKUP/INTERNAL\",\n" +
                    "                    \"imageId\": \"SOP_BOPIS_PickedUp\",\n" +
                    "                    \"items\": [\n" +
                    "                      {\n" +
                    "                        \"conceptCode\": \"MG\",\n" +
                    "                        \"itemId\": \"1742345\",\n" +
                    "                        \"omsLineId\": \"1\",\n" +
                    "                        \"quantity\": 1,\n" +
                    "                        \"pricing\": {\n" +
                    "                          \"prices\": [\n" +
                    "                            {\n" +
                    "                              \"type\": \"surcharge\",\n" +
                    "                              \"modifier\": \"Discount\",\n" +
                    "                              \"amount\": \"12\"\n" +
                    "                            },\n" +
                    "                            {\n" +
                    "                              \"type\": \"lineComputedTotal\",\n" +
                    "                              \"modifier\": \"Regular\",\n" +
                    "                              \"amount\": 140\n" +
                    "                            }\n" +
                    "                          ]\n" +
                    "                        },\n" +
                    "                        \"valueAddedServices\": [\n" +
                    "                          {\n" +
                    "                            \"type\": \"Personalize\",\n" +
                    "                            \"properties\": [\n" +
                    "                              {\n" +
                    "                                \"name\": \"Style\",\n" +
                    "                                \"value\": \"15\"\n" +
                    "                              },\n" +
                    "                              {\n" +
                    "                                \"name\": \"Text1\",\n" +
                    "                                \"value\": \"For Hemant in Shipped to Block1\"\n" +
                    "                              },\n" +
                    "                              {\n" +
                    "                                \"name\": \"Text2\",\n" +
                    "                                \"value\": \"For Hemant in Shipped to Block2\"\n" +
                    "                              },\n" +
                    "                              {\n" +
                    "                                \"name\": \"Text3\",\n" +
                    "                                \"value\": \"For Hemant in Shipped to Block3\"\n" +
                    "                              }\n" +
                    "                            ],\n" +
                    "                            \"charges\": [\n" +
                    "                              {\n" +
                    "                                \"type\": \"Personalization\",\n" +
                    "                                \"amount\": 50\n" +
                    "                              },\n" +
                    "                              {\n" +
                    "                                \"type\": \"MonoPZTax\",\n" +
                    "                                \"amount\": 4.25\n" +
                    "                              }\n" +
                    "                            ]\n" +
                    "                          },\n" +
                    "                          {\n" +
                    "                            \"type\": \"giftWrapIndicator\",\n" +
                    "                            \"properties\": [\n" +
                    "                              {\n" +
                    "                                \"name\": \"giftWrapped\",\n" +
                    "                                \"value\": \"Y\"\n" +
                    "                              }\n" +
                    "                            ]\n" +
                    "                          },\n" +
                    "                          {\n" +
                    "                            \"type\": \"giftmessage\",\n" +
                    "                            \"properties\": [\n" +
                    "                              {\n" +
                    "                                \"name\": \"message\",\n" +
                    "                                \"value\": \"Hello-Testt1111\"\n" +
                    "                              }\n" +
                    "                            ]\n" +
                    "                          }\n" +
                    "                        ],\n" +
                    "                        \"charges\": [\n" +
                    "                          {\n" +
                    "                            \"type\": \"Personalization\",\n" +
                    "                            \"adjustment\": 50\n" +
                    "                          },\n" +
                    "                          {\n" +
                    "                            \"type\": \"MonoPZTax\",\n" +
                    "                            \"adjustment\": 4.25\n" +
                    "                          }\n" +
                    "                        ],\n" +
                    "                        \"productInformation\": {\n" +
                    "                          \"name\": \"Zoku Triple Pop Maker-SubOrder2\",\n" +
                    "                          \"shortName\": \"Zoku Triple Pop Make-SubOrder2\",\n" +
                    "                          \"mediumName\": \"Zoku Triple Pop Maker-SubOrder2\",\n" +
                    "                          \"longName\": \"Zoku Quick Pop Maker, White-SubOrder2 Shipped to block\",\n" +
                    "                          \"images\": [\n" +
                    "                            {\n" +
                    "                              \"type\": \"SKUPRIME\",\n" +
                    "                              \"url\": \"https://www.williams-sonoma.com/wsimgs/ab/images/dp/ecm/201924/1812/001/001.jpg\"\n" +
                    "                            }\n" +
                    "                          ]\n" +
                    "                        },\n" +
                    "                        \"deliveryDate\": {\n" +
                    "                          \"startDate\": \"2019-07-10T11:22:32-08:00\",\n" +
                    "                          \"endDate\": \"2019-07-15T11:22:32-08:00\"\n" +
                    "                        }\n" +
                    "                      }\n" +
                    "                    ]\n" +
                    "                  }\n" +
                    "                ]\n" +
                    "              }\n" +
                    "            ]\n" +
                    "          }\n" +
                    "        }\n" +
                    "      }\n" +
                    "    ]\n" +
                    "  }\n" +
                    "}";

                final ProducerRecord<String, String> record = new ProducerRecord<String, String>("PRD.B.C3.EMAIL.SEND",str);
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
                            log.error("Error while publishing", e);
                        }
                    }
                });
        }
        producer.flush();
        producer.close();
    }
}
