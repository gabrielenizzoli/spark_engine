package sparkengine.plan.model.component.catalog;

import lombok.Value;
import sparkengine.plan.model.component.Component;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Optional;

@Value(staticConstructor = "of")
public class ComponentCatalogFromMap implements ComponentCatalog {

    @Nonnull
    Map<String, Component> components;

    @Nonnull
    @Override
    public Optional<Component> lookup(String componentName) {
        return Optional.ofNullable(components.get(componentName));
    }

}
