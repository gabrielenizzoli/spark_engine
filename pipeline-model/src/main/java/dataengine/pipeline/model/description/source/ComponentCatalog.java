package dataengine.pipeline.model.description.source;

import javax.annotation.Nonnull;
import java.util.Optional;

public interface ComponentCatalog {

    @Nonnull
    Optional<Component> lookup(String componentName) throws ComponentCatalogException;

}
