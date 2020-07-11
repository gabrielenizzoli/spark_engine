package dataengine.pipeline.model.description.source;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import dataengine.pipeline.model.description.source.component.BatchSource;
import dataengine.pipeline.model.description.source.component.SqlSource;
import dataengine.pipeline.model.description.source.component.StreamSource;
import dataengine.pipeline.model.description.source.component.*;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
        // sources
        @JsonSubTypes.Type(value = EmptyDatasetSource.class, name = "emptyDatasetSource"),
        @JsonSubTypes.Type(value = EmptyDataframeSource.class, name = "emptyDataframeSource"),
        @JsonSubTypes.Type(value = BatchSource.class, name = "batchSource"),
        @JsonSubTypes.Type(value = SqlSource.class, name = "sqlSource"),
        @JsonSubTypes.Type(value = StreamSource.class, name = "streamSource"),
        // transformation with single input
        @JsonSubTypes.Type(value = Transform.class, name = "transform"),
        @JsonSubTypes.Type(value = Encode.class, name = "encode"),
        // transformation with multiple inputs
        @JsonSubTypes.Type(value = Merge.class, name = "merge"),
        @JsonSubTypes.Type(value = Union.class, name = "union"),
        @JsonSubTypes.Type(value = Sql.class, name = "sql")
})
public interface Component {

}
