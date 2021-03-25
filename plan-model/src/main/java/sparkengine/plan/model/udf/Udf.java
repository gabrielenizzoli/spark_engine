package sparkengine.plan.model.udf;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import javax.annotation.Nullable;
import java.util.Map;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = UdfWithClassName.class)
@JsonSubTypes({
        @JsonSubTypes.Type(value = UdfWithClassName.class, name = "java"),
        @JsonSubTypes.Type(value = UdfWithScalaScript.class, name = "scala")
})
public interface Udf {

    /**
     * Provides a list of mappings that specifies how accumulators that are internally used in the udf remaps to global accumulators.
     *
     * @return mapping between internal accumulators and their global name
     */
    @Nullable
    Map<String, String> getAccumulators();

}
