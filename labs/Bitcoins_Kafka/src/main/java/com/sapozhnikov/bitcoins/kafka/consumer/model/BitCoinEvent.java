package com.sapozhnikov.bitcoins.kafka.consumer.model;

public class BitCoinEvent
{
    private BitCoinEventData data;
    private String channel;
    private String event;

    public BitCoinEventData getData() {
        return data;
    }

    public void setData(BitCoinEventData data) {
        this.data = data;
    }

    public String getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public String getEvent() {
        return event;
    }

    public void setEvent(String event) {
        this.event = event;
    }

    public Double getPrice(){
        return data.getPrice();
    }

    @Override
    public String toString() {
        return "{" +
                "data=" + data +
                ", channel='" + channel + '\'' +
                ", event='" + event + '\'' +
                '}';
    }

}
