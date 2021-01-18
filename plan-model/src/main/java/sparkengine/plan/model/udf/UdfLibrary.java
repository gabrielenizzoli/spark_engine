package sparkengine.plan.model.udf;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "udf", defaultImpl = UdfList.class)
@JsonSubTypes({
        @JsonSubTypes.Type(value = UdfList.class, name = "list")
})
public interface UdfLibrary {

}
