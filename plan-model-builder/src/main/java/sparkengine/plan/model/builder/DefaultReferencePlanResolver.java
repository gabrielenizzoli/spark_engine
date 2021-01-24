package sparkengine.plan.model.builder;

import lombok.Builder;
import lombok.Value;
import sparkengine.plan.model.Plan;
import sparkengine.plan.model.builder.input.InputStreamFactory;
import sparkengine.plan.model.builder.input.InputStreamResourceLocator;
import sparkengine.plan.model.component.Component;
import sparkengine.plan.model.component.ComponentWithChild;
import sparkengine.plan.model.component.ComponentWithChildren;
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

    @Override
    public Plan resolve(Plan plan) throws PlanResolverException {
        return plan.toBuilder()
                .withComponents(resolveComponents(null, plan.getComponents()))
                .build();
    }

    public Map<String, Component> resolveComponents(@Nullable String componentRelativeLocation,
                                                    @Nonnull Map<String, Component> components) throws PlanResolverException {

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
                                       @Nonnull Component component) throws PlanResolverException {
        if (component instanceof ComponentWithNoRuntime) {
            return resolveComponentWithNoRuntime(componentRelativeLocation, (ComponentWithNoRuntime) component);
        } else if (component instanceof ComponentWithChild) {
            return resolveComponentWithChild(componentRelativeLocation, (ComponentWithChild) component);
        } else if (component instanceof ComponentWithChildren) {
            return resolveComponentWithChildren(componentRelativeLocation, (ComponentWithChildren) component);
        }
        return component;
    }

    private Component resolveComponentWithChild(@Nonnull String componentRelativeLocation,
                                                @Nonnull ComponentWithChild component) throws PlanResolverException {
        var componentWithChild = component;
        var newComponent = resolveComponent(componentRelativeLocation, componentWithChild.getComponent());
        if (componentWithChild instanceof WrapperComponent) {
            return ((WrapperComponent) componentWithChild).withComponent(newComponent);
        }
        throw new PlanResolverException("component with child [" + componentWithChild + "] is not managed to resolve children");
    }

    private Component resolveComponentWithChildren(@Nonnull String componentRelativeLocation,
                                                   @Nonnull ComponentWithChildren component) throws PlanResolverException {
        var componentWithChildren = component;
        var newComponents = resolveComponents(componentRelativeLocation, componentWithChildren.getComponents());
        if (componentWithChildren instanceof FragmentComponent) {
            return ((FragmentComponent) componentWithChildren).withComponents(newComponents);
        }
        throw new PlanResolverException("component with children [" + componentWithChildren + "] is not managed to resolve children");
    }

    private Component resolveComponentWithNoRuntime(@Nonnull String componentRelativeLocation,
                                                    @Nonnull ComponentWithNoRuntime componentWithNoRuntime) throws PlanResolverException {
        if (componentWithNoRuntime instanceof ReferenceComponent) {
            var referenceComponent = (ReferenceComponent) componentWithNoRuntime;
            componentRelativeLocation = Optional
                    .ofNullable(referenceComponent.getRef())
                    .filter(ref -> !ref.isBlank())
                    .orElse(composeRelativeLocation(componentRelativeLocation, "ref"));
            var inputStreamFactory = relativeResourceLocator.getInputStreamFactory(componentRelativeLocation);
            var referredComponent = readReferredComponent(inputStreamFactory, componentRelativeLocation, referenceComponent);
            return resolveComponent(componentRelativeLocation, referredComponent);
        }
        throw new PlanResolverException("non-runtime component [" + componentWithNoRuntime + "] not resolvable (since no code has been written to manage it!)");
    }

    private Component readReferredComponent(@Nonnull InputStreamFactory inputStreamFactory,
                                            @Nonnull String componentRelativeLocation,
                                            ReferenceComponent referenceComponent) throws PlanResolverException {
        try {
            return ModelFactory.readComponentFromYaml(inputStreamFactory);
        } catch (IOException e) {
            throw new PlanResolverException("reference component [" + referenceComponent + "] can't be located (relative path [" + componentRelativeLocation + "])", e);
        } catch (ModelFormatException e) {
            throw new PlanResolverException("reference component [" + referenceComponent + "] has a bad format (relative path [" + componentRelativeLocation + "])", e);
        }
    }

    @Nullable
    private InputStreamFactory getInputStreamFactoryToReadComponent(@Nonnull String componentRelativeLocation,
                                                                    @Nonnull ReferenceComponent referenceComponent) throws PlanResolverException {
        return relativeResourceLocator.getInputStreamFactory(componentRelativeLocation);
    }

    @Nonnull
    private static String composeRelativeLocation(@Nullable String root, @Nonnull String newRelativeLocation) {
        return Optional.ofNullable(root)
                .filter(base -> !base.isBlank())
                .map(base -> String.format("%s_%s", base, newRelativeLocation))
                .orElse(newRelativeLocation);
    }

}
