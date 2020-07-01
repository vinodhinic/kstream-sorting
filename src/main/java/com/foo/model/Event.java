package com.foo.model;

public class Event {
    private EventType eventType;
    private String payload;

    public Event(Builder builder) {
        this.eventType = builder.getEventType();
        this.payload = builder.getPayload();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(Event other) {
        return new Builder().payload(other.payload).eventType(other.eventType);
    }

    public EventType getEventType() {
        return eventType;
    }

    public void setEventType(EventType eventType) {
        this.eventType = eventType;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    @Override
    public String toString() {
        return "Event{" +
                "eventType=" + eventType +
                ", payload='" + payload + '\'' +
                '}';
    }

    public static final class Builder {
        private EventType eventType;
        private String payload;

        private Builder() {
        }

        public Builder eventType(EventType eventType) {
            this.eventType = eventType;
            return this;
        }

        public Builder payload(String payload) {
            this.payload = payload;
            return this;
        }

        public EventType getEventType() {
            return eventType;
        }

        public String getPayload() {
            return payload;
        }

        public Event build() {
            return new Event(this);
        }
    }
}
