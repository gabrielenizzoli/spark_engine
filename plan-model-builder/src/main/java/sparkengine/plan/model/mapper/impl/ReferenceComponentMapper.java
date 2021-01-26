package sparkengine.plan.model.mapper.impl;

import lombok.Value;
import lombok.With;
import sparkengine.plan.model.builder.ModelFactory;
import sparkengine.plan.model.builder.input.InputStreamResourceLocator;
import sparkengine.plan.model.component.Component;
import sparkengine.plan.model.component.impl.ReferenceComponent;
import sparkengine.plan.model.component.mapper.ComponentMapper;
import sparkengine.plan.model.component.mapper.ComponentsMapper;
import sparkengine.plan.model.mapper.ComponentResourceLocationBuilder;

import javax.annotation.Nonnull;
import java.util.Optional;
import java.util.Stack;

@Value(staticConstructor = "of")
public class ReferenceComponentMapper implements ComponentMapper {

    @Nonnull
    @With
    ComponentResourceLocationBuilder componentResourceLocationBuilder;
    @Nonnull
    InputStreamResourceLocator resourceLocator;

    @Override
    public Component mapReferenceComponent(Stack<String> location, ReferenceComponent component) throws Exception {

        if (component.getMode() == ReferenceComponent.ReferenceMode.ABSOLUTE) {
            String uri = component.getRef();
            var inputStream = resourceLocator.getInputStreamFactory(uri);
            var newComponent = ModelFactory.readComponentFromYaml(inputStream);
            var componentMapper = this.withComponentResourceLocationBuilder(componentResourceLocationBuilder.withRoot(uri));
            return ComponentsMapper.mapComponent(new Stack<>(), componentMapper, newComponent);
        } else if (component.getMode() == ReferenceComponent.ReferenceMode.RELATIVE) {
            var effectiveLocation = Optional
                    .ofNullable(component.getRef())
                    .map(String::strip)
                    .filter(ref -> !ref.isBlank())
                    .map(ComponentsMapper::locationOf)
                    .orElse(location);
            String uri = componentResourceLocationBuilder.build(effectiveLocation);
            var inputStream = resourceLocator.getInputStreamFactory(uri);
            var newComponent = ModelFactory.readComponentFromYaml(inputStream);
            return ComponentsMapper.mapComponent(effectiveLocation, this, newComponent);
        }

        return component;
    }
}
