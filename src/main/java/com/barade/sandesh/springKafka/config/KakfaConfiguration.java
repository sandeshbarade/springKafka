package com.barade.sandesh.springKafka.config;

import com.barade.sandesh.springKafka.model.User;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Configurable;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;
import org.springframework.kafka.support.serializer.DeserializationException;

import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Configuration
public class KakfaConfiguration {

    @Bean
    public ProducerFactory producerFactory(){
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return new DefaultKafkaProducerFactory<>(config);
    }

    /**
     * New configuration for the consumerFactory added
     * @return
     */
    @Bean
    public ConsumerFactory<String,User> consumerFactory(){
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "firstTopic-group");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return new DefaultKafkaConsumerFactory<>(config,new StringDeserializer(),  new JsonDeserializer<User>(User.class) );
    }


    @Bean
    public KafkaTemplate<String, User> kafkaTemplate(){
        return new KafkaTemplate<>(producerFactory());
    }




    @Bean
    public ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer factoryConfigurer
            //   ,     ConsumerFactory<Object, Object> kafkaConsumerFactory
    ) {

        ConcurrentKafkaListenerContainerFactory<String,User> factory = new ConcurrentKafkaListenerContainerFactory<>();
        //factoryConfigurer.configure(factory, kafkaConsumerFactory);

        // add the custom consumerFactory
        factory.setConsumerFactory(consumerFactory());
        factory.setRetryTemplate(kafkaRetry());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

        factory.setStatefulRetry(true);
        factory.setErrorHandler(getErrorHandler());
        factory.setRecoveryCallback(retryContext -> {
            //implement the logic to decide the action after all retries are over.
            ConsumerRecord consumerRecord = (ConsumerRecord) retryContext.getAttribute("record");
            System.out.println("Recovery is called for message  "+consumerRecord.value());
            return Optional.empty();
        });
        return factory;
    }

    public RetryTemplate kafkaRetry() {
        RetryTemplate retryTemplate = new RetryTemplate();
        //FixedBackOffPolicy fixedBackOffPolicy = new FixedBackOffPolicy();
        //fixedBackOffPolicy.setBackOffPeriod(10 * 1000l);
        //retryTemplate.setBackOffPolicy(fixedBackOffPolicy);

        ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
        backOffPolicy.setInitialInterval(60*1000);
        backOffPolicy.setMultiplier(3);
        backOffPolicy.setMaxInterval(4*60*1000);       // original 25 * 60 * 1000
        retryTemplate.setBackOffPolicy(backOffPolicy);

        SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy();
        retryPolicy.setMaxAttempts(4);
        retryTemplate.setRetryPolicy(retryPolicy);
        return retryTemplate;
    }


    public SeekToCurrentErrorHandler getErrorHandler(){
        SeekToCurrentErrorHandler  errorHandler = new SeekToCurrentErrorHandler(){

            @Override
            public void handle(     Exception thrownException,
                                    List<ConsumerRecord<?, ?>> records,
                                    Consumer<?, ?> consumer,
                                    MessageListenerContainer container) {
                //super.handle(thrownException, records, consumer, container);
                if(!records.isEmpty()){
                    ConsumerRecord<?,?> record = records.get(0);
                    String topic = record.topic();
                    long offset = record.offset();
                    int partition = record.partition();

                    if(thrownException instanceof DeserializationException){
                        System.out.println("------1111------deserialization exception ");
                    }
                    else{
                        System.out.println("------xxxx------Record is empty ");
                        consumer.seek(new TopicPartition(topic, partition),offset);
                    }
                }else{
                    System.out.println("------4444------Record is empty ");
                }

            }
        };

        return errorHandler;
    }




}
