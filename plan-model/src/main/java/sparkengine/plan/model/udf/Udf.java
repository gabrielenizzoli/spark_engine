package sparkengine.plan.model.udf;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = UdfWithClassName.class)
@JsonSubTypes({
        @JsonSubTypes.Type(value = UdfWithClassName.class, name = "java"),
        @JsonSubTypes.Type(value = UdfWithScalaScript.class, name = "scala")
})
public interface Udf {

}
