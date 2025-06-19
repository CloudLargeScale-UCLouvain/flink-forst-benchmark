package be.uclouvain.gepiciad.sources;

import java.util.Objects;

public class Event {
    private final int key;
    private final long eventTime;
    private final long sequenceNumber;
    private final String payload;

    public Event(int key, long eventTime, long sequenceNumber, String payload) {
        this.key = key;
        this.eventTime = eventTime;
        this.sequenceNumber = sequenceNumber;
        this.payload = payload;
    }

    public int getKey() {
        return key;
    }

    public long getEventTime() {
        return eventTime;
    }

    public long getSequenceNumber() {
        return sequenceNumber;
    }

    public String getPayload() {
        return payload;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Event event = (Event) o;
        return key == event.key
                && eventTime == event.eventTime
                && sequenceNumber == event.sequenceNumber
                && Objects.equals(payload, event.payload);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, eventTime, sequenceNumber, payload);
    }

    @Override
    public String toString() {
        return "Event{"
                + "key="
                + key
                + ", eventTime="
                + eventTime
                + ", sequenceNumber="
                + sequenceNumber
                + ", payload='"
                + payload
                + '\''
                + '}';
    }
}
