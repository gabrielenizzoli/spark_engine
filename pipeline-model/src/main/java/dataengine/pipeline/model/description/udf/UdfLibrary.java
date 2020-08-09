package dataengine.pipeline.model.description.udf;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import javax.annotation.Nonnull;
import java.util.List;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "udf", defaultImpl = UdfList.class)
@JsonSubTypes({
        @JsonSubTypes.Type(value = UdfList.class, name = "list")
})
public interface UdfLibrary {

}
