package sparkengine.plan.model.builder;

import lombok.Builder;
import lombok.Value;
import sparkengine.plan.model.Plan;
import sparkengine.plan.model.builder.input.InputStreamFactory;
import sparkengine.plan.model.builder.input.InputStreamResourceLocator;
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
public class DefaultReferencePlanResolver implements PlanResolver {

    @Nonnull
    InputStreamResourceLocator relativeResourceLocator;
    @Nonnull
    InputStreamResourceLocator absoluteResourceLocator;

    @Override
    public Plan resolve(Plan plan) throws PlanResolverException, IOException {
        return plan.toBuilder()
                .withComponents(resolveComponents(null, plan.getComponents()))
                .build();
    }

    public Map<String, Component> resolveComponents(@Nullable String componentRelativeLocation,
                                                    @Nonnull Map<String, Component> components) throws IOException, PlanResolverException {

        var newComponents = new LinkedHashMap<String, Component>();
        for (var nameAndComponent : components.entrySet()) {

            var name = nameAndComponent.getKey();
            var component = nameAndComponent.getValue();
            var newComponent = resolveComponent(composeRelativeLocation(componentRelativeLocation, name), component);

            newComponents.put(name, newComponent);
        }

        return newComponents;
    }

    private Component resolveComponent(@Nonnull String componentRelativeLocation,
                                       @Nonnull Component component) throws IOException, PlanResolverException {
        if (component instanceof ComponentWithNoRuntime) {
            return resolveComponentWithNoRuntime(componentRelativeLocation, (ComponentWithNoRuntime) component);
        } else if (component instanceof FragmentComponent) {
            var fragment = (FragmentComponent) component;
            var newComponents = resolveComponents(componentRelativeLocation, fragment.getComponents());
            return fragment.toBuilder()
                    .withComponents(newComponents)
                    .build();
        }
        return component;
    }

    private Component resolveComponentWithNoRuntime(@Nonnull String componentRelativeLocation,
                                                    @Nonnull ComponentWithNoRuntime componentWithNoRuntime) throws IOException, PlanResolverException {
        if (componentWithNoRuntime instanceof ReferenceComponent) {
            var referenceComponent = (ReferenceComponent) componentWithNoRuntime;
            var inputStreamFactory = getInputStreamFactoryToReadComponent(componentRelativeLocation, referenceComponent);
            var externalComponent = ModelFactory.readComponentFromYaml(inputStreamFactory);
            var resolvedExternalComponent = resolveComponent(composeRelativeLocation(componentRelativeLocation, externalComponent.componentTypeName()), externalComponent);
            return getResolvedComponent(referenceComponent, resolvedExternalComponent);
        }
        throw new PlanResolverException("non-runtime component [" + componentWithNoRuntime + "] not resolvable (since no code has been written to manage it!)");
    }

    @Nullable
    private InputStreamFactory getInputStreamFactoryToReadComponent(@Nonnull String componentRelativeLocation,
                                                                    @Nonnull ReferenceComponent referenceComponent) throws PlanResolverException {
        switch (referenceComponent.getRefType()) {
            case RELATIVE:
                return relativeResourceLocator.getInputStreamFactory(Optional
                        .ofNullable(referenceComponent.getRef())
                        .filter(ref -> !ref.isBlank())
                        .orElse(componentRelativeLocation));
            case ABSOLUTE:
                return absoluteResourceLocator.getInputStreamFactory(Optional
                        .ofNullable(referenceComponent.getRef())
                        .filter(ref -> !ref.isBlank())
                        .orElseThrow(() -> new PlanResolverException("reference component [" + referenceComponent + "] needs a full reference in ABSOLUTE resolution mode")));
        }
        throw new PlanResolverException("reference component [" + referenceComponent + "] can't instantiate a proper resource locator");
    }

    private Component getResolvedComponent(@Nonnull ReferenceComponent referenceComponent,
                                           @Nonnull Component externalComponent) throws PlanResolverException {
        switch (referenceComponent.getInlineMode()) {
            case INLINE:
                return externalComponent;
            case WRAPPED:
                if (!(externalComponent instanceof FragmentComponent))
                    throw new PlanResolverException("reference component [" + referenceComponent + "] in WRAPPED inline mode has reference that does not resolve to a fragment component [" + externalComponent + "]");
                return WrapperComponent.builder()
                        .withUsing(referenceComponent.getUsing())
                        .withComponent(externalComponent)
                        .build();
            default:
                throw new PlanResolverException("reference component [" + referenceComponent + "] has inline mode [" + referenceComponent.getInlineMode() + "] that is not managed");
        }
    }

    @Nonnull
    private static String composeRelativeLocation(@Nullable String root, @Nonnull String newRelativeLocation) {
        return Optional.ofNullable(root)
                .filter(base -> !base.isBlank())
                .map(base -> String.format("%s_%s", base, newRelativeLocation))
                .orElse(newRelativeLocation);
    }

}
