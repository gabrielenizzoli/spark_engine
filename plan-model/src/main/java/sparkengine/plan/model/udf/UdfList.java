package sparkengine.plan.model.udf;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Builder;
import lombok.Value;

import javax.annotation.Nonnull;
import java.util.List;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = UdfList.UdfListBuilder.class)
public class UdfList implements UdfLibrary {

    @Nonnull
    List<Udf> list;

}
