package com.barade.sandesh.springKafka.service;


import com.barade.sandesh.springKafka.model.User;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.io.File;
import java.sql.Timestamp;
import java.util.Date;

@Service
public class KafkaMessageConsumerService {

    private String status = null;

    @KafkaListener(topics = "firstTopic", groupId = "firstTopic-group")
    public void onCustomerMessage(User user) throws Exception {                    //, Acknowledgment acknowledgment

        if(status == null){
            status = "on";
            System.out.println("\n"+"====> setting the status to ON");
        }else{
            System.out.println("\n"+"====> status set earlier");
        }
        System.out.println(new Date() + " -> Message  = "+ user.getFirstName()+ "    is received");
        if (user.getFirstName().equalsIgnoreCase("Test")) {
            System.out.println("Exception caught now throw an exception for incompatible message ="+ user + "\n");
            throw new RuntimeException("Incompatible message " + user.getFirstName());
        }
        //acknowledgment.acknowledge();
        status = null;

        System.out.println("Exiting the message processing ... Resetting status to  = " + status);
    }
}

