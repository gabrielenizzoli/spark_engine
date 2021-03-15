package sparkengine.plan.model.udf;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Builder;
import lombok.Value;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = UdfWithClassName.Builder.class)
public class UdfWithClassName implements Udf {

    String className;

}
