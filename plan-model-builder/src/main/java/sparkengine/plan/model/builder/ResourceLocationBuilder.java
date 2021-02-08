package sparkengine.plan.model.builder;

import lombok.Value;
import lombok.With;
import sparkengine.plan.model.common.Location;

import javax.annotation.Nonnull;

@Value
public class ResourceLocationBuilder {

    @With
    @Nonnull
    String root;
    @Nonnull
    String locationJoin;
    @Nonnull
    String extension;

    public String build(Location location) {
        var rootURI = URIBuilder.ofString(root);
        var planName = rootURI.getLastPartFromPath().replaceAll("\\..*$", "");
        var cleanedRootURI = rootURI.removePartFromPath().addPartToPath(planName);
        var partsString = location.isEmpty() ? "" : locationJoin + location.joinWith(locationJoin);
        return cleanedRootURI.addToPath(partsString + "." + extension).toString();
    }

}
