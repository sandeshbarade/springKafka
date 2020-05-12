package com.barade.sandesh.springKafka;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("csrs")
public class CSRResource {
/*
    @Autowired
    KafkaTemplate<String, String> kafkaTemplates;
    @PostMapping("/id/{id}/comments/{comments}")
    public String postComments(@PathVariable("id") final String id, @PathVariable ("comments") final String comments){

        kafkaTemplates.send("firstTopic", "user="+id+", mentioned:="+comments);

        return "thank you";
    }

 */
}
//http://localhost:8080/csrs/id/1234/comments/this is insane