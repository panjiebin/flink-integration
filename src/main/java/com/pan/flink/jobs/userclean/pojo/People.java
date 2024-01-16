package com.pan.flink.jobs.userclean.pojo;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.io.Serializable;

/**
 * @author panjb
 */
@JsonPropertyOrder({"id", "name", "idCard", "phone", "address"})
public class People implements Serializable {
    private String id;
    private String name;
    private String idCard;
    private String phone;
    private String address;
    private String road;

    public People() {
    }

    public People(String id) {
        this.id = id;
    }

    public People(String name, String idCard, String phone, String address) {
        this.name = name;
        this.idCard = idCard;
        this.phone = phone;
        this.address = address;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getIdCard() {
        return idCard;
    }

    public void setIdCard(String idCard) {
        this.idCard = idCard;
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getRoad() {
        return road;
    }

    public void setRoad(String road) {
        this.road = road;
    }
}
