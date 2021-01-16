package dataengine.pipeline.model.component.catalog;

import dataengine.pipeline.model.component.Component;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Optional;

public interface ComponentCatalog {

    ComponentCatalog EMPTY = (name) -> Optional.empty();

    @Nonnull
    Optional<Component> lookup(String componentName) throws ComponentCatalogException;

}
