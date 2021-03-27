package sparkengine.plan.model.sink.impl;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Builder;
import lombok.Value;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = Trigger.TriggerIntervalMs.class)
@JsonSubTypes({
        @JsonSubTypes.Type(value = Trigger.TriggerIntervalMs.class, name = "interval"),
        @JsonSubTypes.Type(value = Trigger.TriggerOnce.class, name = "once"),
        @JsonSubTypes.Type(value = Trigger.TriggerContinuousMs.class, name = "continuous")
})
public interface Trigger {

    @Value
    @Builder(setterPrefix = "with")
    @JsonDeserialize(builder = TriggerIntervalMs.TriggerIntervalMsBuilder.class)
    class TriggerIntervalMs implements Trigger {
        long milliseconds;
    }

    @Value
    @Builder(setterPrefix = "with")
    @JsonDeserialize(builder = TriggerOnce.TriggerOnceBuilder.class)
    class TriggerOnce implements Trigger {
    }

    @Value
    @Builder(setterPrefix = "with")
    @JsonDeserialize(builder = TriggerContinuousMs.TriggerContinuousMsBuilder.class)
    class TriggerContinuousMs implements Trigger {
        long milliseconds;
    }

}
