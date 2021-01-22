package sparkengine.plan.model.builder;

import lombok.Builder;
import lombok.Value;
import sparkengine.plan.model.Plan;
import sparkengine.plan.model.builder.input.InputStreamResourceLocator;
import sparkengine.plan.model.builder.input.InputStreamSupplier;
import sparkengine.plan.model.component.Component;
import sparkengine.plan.model.component.ComponentWithNoRuntime;
import sparkengine.plan.model.component.impl.FragmentComponent;
import sparkengine.plan.model.component.impl.ReferenceComponent;
import sparkengine.plan.model.component.impl.WrapperComponent;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

@Value
@Builder
public class DefaultPLanResolver implements PlanResolver {

    @Nonnull
    InputStreamResourceLocator relativeResourceLocator;
    @Nonnull
    InputStreamResourceLocator absoluteResourceLocator;

    @Override
    public Plan resolve(Plan plan) throws PlanResolverException, IOException {
        return plan.toBuilder()
                .withComponents(resolveComponents(plan.getComponents()))
                .build();
    }

    public Map<String, Component> resolveComponents(Map<String, Component> components) throws IOException, PlanResolverException {

        Map<String, Component> newComponents = new LinkedHashMap<>();
        for (var componentWithName : components.entrySet()) {

            var name = componentWithName.getKey();
            var component = resolveComponent(name, componentWithName.getValue());

            newComponents.put(name, component);
        }

        return newComponents;
    }

    private Component resolveComponent(String componentName, Component component) throws IOException, PlanResolverException {
        if (component instanceof ComponentWithNoRuntime) {
            return resolveComponentWithNoRuntime(componentName, (ComponentWithNoRuntime) component);
        }
        return component;
    }


    private Component resolveComponentWithNoRuntime(String componentName, ComponentWithNoRuntime componentWithNoRuntime) throws IOException, PlanResolverException {
        if (componentWithNoRuntime instanceof ReferenceComponent) {
            var referenceComponent = (ReferenceComponent) componentWithNoRuntime;

            var stream = getStreamSupplier(componentName, referenceComponent);
            var resolvedComponent = ModelFactories.readComponentFromYaml(stream);
            if (!(resolvedComponent instanceof FragmentComponent))
                throw new PlanResolverException("resolved component [" + componentWithNoRuntime + "] does not resolve to a fragment component [" + resolvedComponent + "]");

            WrapperComponent.builder()
                    .withUsing(referenceComponent.getUsing())
                    .withComponent((FragmentComponent) resolvedComponent)
                    .build();
        }
        throw new PlanResolverException("non-runtime component [" + componentWithNoRuntime + "] not managed for any resolution");
    }

    @Nullable
    private InputStreamSupplier getStreamSupplier(String componentName, ReferenceComponent referenceComponent) throws PlanResolverException {
        switch (referenceComponent.getReferenceType()) {
            case RELATIVE:
                return relativeResourceLocator.getInputStreamSupplier(Optional.ofNullable(referenceComponent.getRef()).filter(ref -> !ref.isBlank()).orElse(componentName));
            case ABSOLUTE:
                return absoluteResourceLocator.getInputStreamSupplier(Optional.ofNullable(referenceComponent.getRef()).filter(ref -> !ref.isBlank()).orElse(componentName));
        }

        throw new PlanResolverException("reference component [" + referenceComponent + "] can't instantiate a proper resource locator");
    }


}
