package dataengine.pipeline.model.description.sink;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Singular;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

public interface SinkWithFormat extends Sink {

    @Nonnull
    String getFormat();
    @Nullable
    Map<String, String> getOptions();
    @Nullable
    List<String> getPartitionColumns();

}
