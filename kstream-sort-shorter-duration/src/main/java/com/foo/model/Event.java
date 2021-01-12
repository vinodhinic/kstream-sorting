package com.foo.model;

import java.util.Objects;

public class Event {
    private EventType eventType;
    private String id;
    private String payload;

    public Event(Builder builder) {
        this.eventType = builder.getEventType();
        this.payload = builder.getPayload();
        this.id = builder.getId();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(Event other) {
        return new Builder().payload(other.payload).eventType(other.eventType).id(other.id);
    }

    public String getId() {
        return id;
    }

    public EventType getEventType() {
        return eventType;
    }

    public String getPayload() {
        return payload;
    }

    @Override
    public String toString() {
        return "Event{" +
                "eventType=" + eventType +
                ", id='" + id + '\'' +
                ", payload='" + payload + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Event event = (Event) o;
        return eventType == event.eventType &&
                Objects.equals(id, event.id) &&
                Objects.equals(payload, event.payload);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventType, id, payload);
    }

    public static final class Builder {
        private EventType eventType;
        private String payload;
        private String id;

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

        public Builder id(String id) {
            this.id = id;
            return this;
        }

        public String getId() {
            return id;
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
