package com.barade.sandesh.springKafka.service;


import com.barade.sandesh.springKafka.model.User;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.File;
import java.sql.Timestamp;
import java.util.Date;

@Service
public class KafkaMessageConsumerService {

    @Autowired
    KafkaTemplate<String, User> kafkaTemplate;


    @KafkaListener(topics = "firstTopic", groupId = "firstTopic-group")
    public void onCustomerMessage(User user, Acknowledgment acknowledgment) throws Exception {

        if (user.getFirstName().equalsIgnoreCase("Test")) {
            throw new RuntimeException("Incompatible message " + user.getFirstName());
        }
        postToSecondTopic(acknowledgment, user);
    }

    @Transactional
    public void postToSecondTopic(Acknowledgment acknowledgment, User user){

        kafkaTemplate.send("secondtopic", user );
        acknowledgment.acknowledge();
        /*
            System.out.println("NOT In transaction");
            kafkaTemplate.executeInTransaction(t -> {
                t.send("secondtopic", user );
                acknowledgment.acknowledge();
                return true;
            });
        */

    }
}
