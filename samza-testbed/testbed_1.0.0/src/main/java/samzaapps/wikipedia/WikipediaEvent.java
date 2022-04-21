package samzaapps.wikipedia;

import org.codehaus.jackson.annotate.JsonProperty;

/**
 * A page view event
 */
public class WikipediaEvent {
    public final String rawEvent;
    public final String channel;
    public final String source;
    public final Long time;

    /**
     * Constructs a page view event.
     *
     * @param raw
     * @param channel
     * @param source
     * @param time
     */
    public WikipediaEvent(
            @JsonProperty("raw") String raw,
            @JsonProperty("channel") String channel,
            @JsonProperty("source") String source,
            @JsonProperty("time") Long time) {
        this.rawEvent = raw;
        this.channel = channel;
        this.source = source;
        this.time = time;
    }

    public long getTime() {
        return time;
    }

    public String getChannel() {
        return channel;
    }

    public String getSource() {
        return source;
    }

    public String getRawEvent() {
        return rawEvent;
    }

    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((channel == null) ? 0 : channel.hashCode());
        result = prime * result + ((rawEvent == null) ? 0 : rawEvent.hashCode());
        result = prime * result + ((source == null) ? 0 : source.hashCode());
        result = prime * result + (int) (time ^ (time >>> 32));
        return result;
    }

    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        WikipediaEvent other = (WikipediaEvent) obj;
        if (channel == null) {
            if (other.channel != null)
                return false;
        } else if (!channel.equals(other.channel))
            return false;
        if (rawEvent == null) {
            if (other.rawEvent != null)
                return false;
        } else if (!rawEvent.equals(other.rawEvent))
            return false;
        if (source == null) {
            if (other.source != null)
                return false;
        } else if (!source.equals(other.source))
            return false;
        if (time != other.time)
            return false;
        return true;
    }
}