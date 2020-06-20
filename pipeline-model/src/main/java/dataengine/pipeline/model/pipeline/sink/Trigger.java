package dataengine.pipeline.model.pipeline.sink;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Builder;
import lombok.Value;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = Trigger.TriggerTimeMs.class)
@JsonSubTypes({
        @JsonSubTypes.Type(value = Trigger.TriggerTimeMs.class, name = "milliseconds"),
        @JsonSubTypes.Type(value = Trigger.TriggerOnce.class, name = "once"),
        @JsonSubTypes.Type(value = Trigger.TriggerContinuous.class, name = "continuous")
})
public interface Trigger {

    @Value
    @Builder(setterPrefix = "with")
    @JsonDeserialize(builder = TriggerTimeMs.Builder.class)
    class TriggerTimeMs implements Trigger {
        long milliseconds;
    }

    @Value
    @Builder(setterPrefix = "with")
    @JsonDeserialize(builder = TriggerOnce.Builder.class)
    class TriggerOnce implements Trigger {
    }

    @Value
    @Builder(setterPrefix = "with")
    @JsonDeserialize(builder = TriggerContinuous.Builder.class)
    class TriggerContinuous implements Trigger {
    }

}
