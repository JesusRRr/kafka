package com.rolon.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    public static void main(String[] args) {
        String bootstrapServer="127.0.0.1:9092";
        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);
        // Create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        //Create a producer record
        ProducerRecord<String, String> record =
                new ProducerRecord<String, String>("first_topic", "Hello world");

        // send data - asynchronous
        producer.send(record, new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                //executes every time a record is successfully or and exception  is thrown
                if(e == null) {
                    //the record was successfully sent
                    logger.info("\nReceived new metadata. \n"+
                    "Topic:"+ recordMetadata.topic() + "\n"+
                    "Partition:" + recordMetadata.partition() +"\n" +
                    "Offset:" + recordMetadata.offset() + "\n"+
                    "Timestamp" + recordMetadata.timestamp());
                }else{
                    logger.error("Error while producing", e);
                }
            }
        });

        //flush data
        producer.flush();

        //flush and close producer
        producer.close();
    }
}
