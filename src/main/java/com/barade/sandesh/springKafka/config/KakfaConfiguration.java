package com.barade.sandesh.springKafka.config;

import com.barade.sandesh.springKafka.model.User;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Configurable;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

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


    @Bean
    public KafkaTemplate<String, User> kafkaTemplate(){
        return new KafkaTemplate<>(producerFactory());
    }


    @Bean
    public ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer factoryConfigurer,
            ConsumerFactory<Object, Object> kafkaConsumerFactory) {

        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factoryConfigurer.configure(factory, kafkaConsumerFactory);
        factory.setRetryTemplate(kafkaRetry());
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
        backOffPolicy.setMultiplier(5);
        backOffPolicy.setMaxInterval(6*60*1000);       // original 25 * 60 * 1000
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
