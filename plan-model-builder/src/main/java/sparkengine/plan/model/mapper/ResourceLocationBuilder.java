package sparkengine.plan.model.mapper;

import lombok.Value;
import lombok.With;
import sparkengine.plan.model.builder.URIBuilder;

import javax.annotation.Nonnull;
import java.util.Stack;

@Value
public class ResourceLocationBuilder {

    @With
    @Nonnull
    String root;
    @Nonnull
    String locationJoin;
    @Nonnull
    String extension;

    public String build(Stack<String> parts) {
        var rootURI = URIBuilder.ofString(root);
        var planName = rootURI.getLastPartFromPath().replaceAll("\\..*$", "");
        var cleanedRootURI = rootURI.removePartFromPath().addPartToPath(planName);
        var partsString = parts.isEmpty() ? "" : locationJoin + String.join(locationJoin, parts);
        return cleanedRootURI.addToPath(partsString + "." + extension).toString();
    }

}
