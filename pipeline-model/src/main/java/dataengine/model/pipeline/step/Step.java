package dataengine.model.pipeline.step;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import dataengine.model.pipeline.step.source.SparkBatchSource;
import dataengine.model.pipeline.step.source.SparkSqlSource;
import dataengine.model.pipeline.step.source.SparkStreamSource;
import dataengine.model.pipeline.step.transformation.*;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = SparkBatchSource.class, name = "batchSource"),
        @JsonSubTypes.Type(value = SparkSqlSource.class, name = "sqlSource"),
        @JsonSubTypes.Type(value = SparkStreamSource.class, name = "streamSource"),
        @JsonSubTypes.Type(value = Transform.class, name = "transform"),
        @JsonSubTypes.Type(value = Encode.class, name = "encode"),
        @JsonSubTypes.Type(value = Merge.class, name = "merge"),
        @JsonSubTypes.Type(value = Union.class, name = "union"),
        @JsonSubTypes.Type(value = Sql.class, name = "sql"),
        @JsonSubTypes.Type(value = SqlMerge.class, name = "sqlMerge")
})
public interface Step {

}
