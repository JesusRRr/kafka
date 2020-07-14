package com.rolon.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithTread {
    Logger logger = LoggerFactory.getLogger(ConsumerDemoWithTread.class.getName());

    private void run(){
        String bootstrapServer = "127.0.0.1:9092";
        String groupID = "my-fourth-application";
        String topic = "first_topic";
        CountDownLatch latch = new CountDownLatch(1);
        logger.info("Creating the consumer thread");
        Runnable myConsumerThread = new ConsumerThead(topic,bootstrapServer, groupID, latch);

        //Start the thread
        Thread myThread = new Thread(myConsumerThread);
        myThread.start();

        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            logger.info("Caught shutdown hook");
            ((ConsumerThead) myConsumerThread).shutdown();
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.info("Application got interrupted");
        }finally {
            logger.info("Application is closing");
        }
    }

    public static void main(String[] args) {
        new ConsumerDemoWithTread().run();
    }

    public class ConsumerThead implements  Runnable{
        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;
        private Properties properties;
        private Logger logger;

        public ConsumerThead(String topic, String bootstrapServer, String groupID, CountDownLatch latch){
            this.latch = latch;
            logger = LoggerFactory.getLogger(ConsumerThead.class.getName());
            //Consumer config
            properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupID);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            //create consumer
            consumer = new KafkaConsumer<String, String>(properties);

            //subscribe consumer to our topic(s)
            consumer.subscribe(Arrays.asList(topic));
        }

        @Override
        public void run() {
            //poll for new data
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    records.forEach(record -> {
                        logger.info("Key:" + record.key() + " Value:" + record.value());
                        logger.info("Partition:" + record.partition());
                        logger.info("Offset" + record.offset());
                    });
                }
            }catch (WakeupException e){
                logger.info("Received shutdown signal");
            } finally {
                consumer.close();
                // tell out main method we're with the consumer
                latch.countDown();
            }
        }
        public void shutdown(){
            // the wakeup() method is a special method to interrupt consumer.poll()
            // it will throw the exception WakeUpException
            consumer.wakeup();
        }
    }
}
