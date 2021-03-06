package sparkengine.plan.model.mapper.reference;

import lombok.Value;
import lombok.With;
import sparkengine.plan.model.builder.ModelFactory;
import sparkengine.plan.model.builder.ResourceLocationBuilder;
import sparkengine.plan.model.builder.input.InputStreamResourceLocator;
import sparkengine.plan.model.common.Location;
import sparkengine.plan.model.common.Reference;
import sparkengine.plan.model.component.Component;
import sparkengine.plan.model.component.impl.ReferenceComponent;
import sparkengine.plan.model.component.mapper.ComponentMapper;
import sparkengine.plan.model.component.mapper.ComponentsMapper;

import javax.annotation.Nonnull;
import java.util.Optional;

@Value(staticConstructor = "of")
public class ComponentMapperThatReplacesReferences implements ComponentMapper {

    @Nonnull
    @With
    ResourceLocationBuilder resourceLocationBuilder;
    @Nonnull
    InputStreamResourceLocator resourceLocator;

    @Override
    public Component mapReferenceComponent(Location location, ReferenceComponent component) throws Exception {

        if (component.getMode() == Reference.ReferenceMode.ABSOLUTE) {
            String uri = component.getRef();
            var inputStream = resourceLocator.getInputStreamFactory(uri);
            var newComponent = ModelFactory.readComponentFromYaml(inputStream);
            var componentMapper = this.withResourceLocationBuilder(resourceLocationBuilder.withRoot(uri));
            return ComponentsMapper.mapComponent(Location.empty(), componentMapper, newComponent);
        } else if (component.getMode() == Reference.ReferenceMode.RELATIVE) {
            var effectiveLocation = Optional
                    .ofNullable(component.getRef())
                    .map(String::strip)
                    .filter(ref -> !ref.isBlank())
                    .map(Location::of)
                    .orElse(location);
            String uri = resourceLocationBuilder.build(effectiveLocation);
            var inputStream = resourceLocator.getInputStreamFactory(uri);
            var newComponent = ModelFactory.readComponentFromYaml(inputStream);
            return ComponentsMapper.mapComponent(effectiveLocation, this, newComponent);
        }

        return component;
    }
}
