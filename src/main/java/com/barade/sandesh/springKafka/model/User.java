package com.barade.sandesh.springKafka.model;

import java.util.List;

public class User {
    private String firstName;

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }


    public User() {
        super();
    }
    public User(String firstName, String lastName, String userName ) {
        super();
        this.firstName = firstName;
        this.lastName = lastName;
        this.userName = userName;
    }

    private String lastName;
    private String userName;

}
