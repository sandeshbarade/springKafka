package com.barade.sandesh.springKafka;

import com.barade.sandesh.springKafka.model.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;

@RestController
@RequestMapping("accounts")
public class UserResource {

    @Autowired
    KafkaTemplate <String, User> kafkaTemplate;
    @PostMapping("/users")
    public String postComments(@RequestParam ("firstName") final String firstName,
                                    @RequestParam ("lastName") final String lastName,
                                    @RequestParam ("userName") final String userName )  {

        List<String> accountTypes = new ArrayList<String>();
        kafkaTemplate.send("firstTopic", new User(firstName,lastName,userName));

        return "Message sent to the Error queue";
    }



}


//http://localhost:8080/accounts/users?fName=sandesh&lName=barade&userName=sandeshnb

/*


        int count =0 ;
        int THRESHOLD =3 ;
        List<String> accountTypes = new ArrayList<String>();
        while(count < THRESHOLD){
            try{
                Integer.valueOf(firstName);
                kafkaTemplate.send("firstTopic", new User( firstName,  lastName,  userName, accountTypes) );
                return "message posted successfully";
            }catch(Exception e){
                count++;
                try {
                    System.out.println("Thread put to sleep for 2000 ms because error="+ e.getMessage());
                    Thread.sleep(2000);
                } catch (InterruptedException interruptedException) {
                    interruptedException.printStackTrace();
                }
            }
        }
 */