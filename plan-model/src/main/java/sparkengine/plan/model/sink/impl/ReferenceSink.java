package sparkengine.plan.model.sink.impl;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Builder;
import lombok.Value;
import sparkengine.plan.model.Reference;
import sparkengine.plan.model.sink.Sink;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = ReferenceSink.Builder.class)
public class ReferenceSink implements Sink, Reference {

    public static final String TYPE_NAME = "ref";

    @Nonnull
    @lombok.Builder.Default
    ReferenceMode mode = ReferenceMode.RELATIVE;
    @Nullable
    String ref;

}
