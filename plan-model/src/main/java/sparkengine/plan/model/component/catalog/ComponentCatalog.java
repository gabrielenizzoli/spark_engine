package sparkengine.plan.model.component.catalog;

import sparkengine.plan.model.component.Component;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Optional;

public interface ComponentCatalog {

    ComponentCatalog EMPTY = (name) -> Optional.empty();

    static ComponentCatalog ofMap(Map<String, Component> components) {
        return ComponentCatalogFromMap.of(components);
    }

    @Nonnull
    Optional<Component> lookup(String componentName) throws ComponentCatalogException;

}
