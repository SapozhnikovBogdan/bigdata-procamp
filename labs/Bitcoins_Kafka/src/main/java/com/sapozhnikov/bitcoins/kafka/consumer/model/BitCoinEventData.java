package com.sapozhnikov.bitcoins.kafka.consumer.model;

import java.util.Optional;

public class BitCoinEventData {
    private long id;
    private String id_str;
    private int order_type;
    private String datetime;
    private String microtimestamp;
    private Double amount;
    private String amount_str;
    private Double price;
    private String price_str;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getId_str() {
        return id_str;
    }

    public void setId_str(String id_str) {
        this.id_str = id_str;
    }

    public int getOrder_type() {
        return order_type;
    }

    public void setOrder_type(int order_type) {
        this.order_type = order_type;
    }

    public String getDatetime() {
        return datetime;
    }

    public void setDatetime(String datetime) {
        this.datetime = datetime;
    }

    public String getMicrotimestamp() {
        return microtimestamp;
    }

    public void setMicrotimestamp(String microtimestamp) {
        this.microtimestamp = microtimestamp;
    }

    public Double getAmount() {
        return amount;
    }

    public void setAmount(Double amount) {
        this.amount = amount;
    }

    public String getAmount_str() {
        return amount_str;
    }

    public void setAmount_str(String amount_str) {
        this.amount_str = amount_str;
    }

    public Double getPrice() {
        return Optional.ofNullable(price).orElse(0.0);
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    public String getPrice_str() {
        return price_str;
    }

    public void setPrice_str(String price_str) {
        this.price_str = price_str;
    }

    @Override
    public String toString() {
        return "{" +
                "id=" + id +
                ", id_str='" + id_str + '\'' +
                ", order_type=" + order_type +
                ", datetime='" + datetime + '\'' +
                ", microtimestamp='" + microtimestamp + '\'' +
                ", amount=" + amount +
                ", amount_str=" + amount_str +
                ", price=" + price +
                ", price_str='" + price_str + '\'' +
                "}";
    }
}
