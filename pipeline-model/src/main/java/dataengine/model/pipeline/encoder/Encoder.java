package dataengine.model.pipeline.encoder;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "schema", defaultImpl = ValueEncoder.class)
@JsonSubTypes({
        @JsonSubTypes.Type(value = ValueEncoder.class, name = "value"),
        @JsonSubTypes.Type(value = TupleEncoder.class, name = "tuple"),
        @JsonSubTypes.Type(value = BeanEncoder.class, name = "bean"),
        @JsonSubTypes.Type(value = SerializationEncoder.class, name = "serialization")
})
public interface Encoder {
}
