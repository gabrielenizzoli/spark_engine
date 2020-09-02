package dataengine.pipeline.model.description.sink;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Builder;
import lombok.Value;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = Trigger.TriggerTimeMs.class)
@JsonSubTypes({
        @JsonSubTypes.Type(value = Trigger.TriggerTimeMs.class, name = "milliseconds"),
        @JsonSubTypes.Type(value = Trigger.TriggerOnce.class, name = "once"),
        @JsonSubTypes.Type(value = Trigger.TriggerContinuousMs.class, name = "continuous")
})
public interface Trigger {

    @Value
    @Builder(setterPrefix = "with")
    @JsonDeserialize(builder = TriggerTimeMs.Builder.class)
    class TriggerTimeMs implements Trigger {
        long time;
    }

    @Value
    @Builder(setterPrefix = "with")
    @JsonDeserialize(builder = TriggerOnce.Builder.class)
    class TriggerOnce implements Trigger {
    }

    @Value
    @Builder(setterPrefix = "with")
    @JsonDeserialize(builder = TriggerContinuousMs.Builder.class)
    class TriggerContinuousMs implements Trigger {
        long milliseconds;
    }

}
