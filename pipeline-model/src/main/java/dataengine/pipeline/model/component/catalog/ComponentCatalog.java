package dataengine.pipeline.model.component.catalog;

import dataengine.pipeline.model.component.Component;

import javax.annotation.Nonnull;
import java.util.Optional;

public interface ComponentCatalog {

    @Nonnull
    Optional<Component> lookup(String componentName) throws ComponentCatalogException;

}
