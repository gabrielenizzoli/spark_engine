package dataengine.pipeline.model.source.component;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import dataengine.pipeline.model.source.SchemaComponent;
import dataengine.pipeline.model.source.SourceComponent;
import lombok.Builder;
import lombok.Value;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = InlineSource.Builder.class)
public class InlineSource implements SourceComponent, SchemaComponent {

    @Nullable
    List<Map<String, Object>> data;
    @Nonnull
    String schema;

}
