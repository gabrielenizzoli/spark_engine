package dataengine.model.pipeline.step.transformation;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import dataengine.model.pipeline.step.MultiInput;
import dataengine.model.pipeline.step.Step;
import dataengine.model.pipeline.step.source.SparkBatchSource;
import lombok.Builder;
import lombok.Value;

import javax.annotation.Nonnull;
import java.util.List;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = SqlMerge.Builder.class)
public class SqlMerge implements Step, MultiInput {

    @Nonnull
    List<String> using;
    @Nonnull
    String sql;

}
