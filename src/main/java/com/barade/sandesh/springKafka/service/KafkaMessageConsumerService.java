package com.barade.sandesh.springKafka.service;


import com.barade.sandesh.springKafka.model.User;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.io.File;
import java.util.Date;

@Service
public class KafkaMessageConsumerService {

    @KafkaListener(topics = "firstTopic", groupId = "firstTopic-group")
    public void onCustomerMessage(String message) throws Exception {
        System.out.println(new Date().getTime() + "Message  = "+ message + "    is received");



        if (message.contains("Test")) {
            System.out.println("Exception caught now throw an exception for incompatible message ="+ message+"\n");
            throw new RuntimeException("Incompatible message " + message);
        }
        System.out.println("Exiting the message processing ...");
    }
}

