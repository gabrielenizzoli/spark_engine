package sparkengine.plan.model.resolver;

import lombok.Value;
import sparkengine.plan.model.Plan;
import sparkengine.plan.model.component.Component;
import sparkengine.plan.model.component.impl.*;

import javax.annotation.Nonnull;
import java.util.LinkedHashMap;
import java.util.Map;

@Value(staticConstructor = "of")
public class ComponentsPlanResolver implements PlanResolver {

    @Nonnull
    ComponentResolver componentResolver;

    @Override
    public Plan resolve(Plan plan) throws PlanResolverException {
        return plan.toBuilder()
                .withComponents(resolveComponents(plan.getComponents()))
                .build();
    }

    public Map<String, Component> resolveComponents(@Nonnull Map<String, Component> components) throws PlanResolverException {

        var newComponents = new LinkedHashMap<String, Component>();
        for (var nameAndComponent : components.entrySet()) {

            var name = nameAndComponent.getKey();
            var component = nameAndComponent.getValue();
            var newComponent = resolveComponent(component);

            newComponents.put(name, newComponent);
        }

        return newComponents;
    }

    private Component resolveComponent(Component component) throws PlanResolverException {

        if (component instanceof EmptyComponent)
            return componentResolver.resolveEmptyComponent((EmptyComponent) component);
        if (component instanceof InlineComponent)
            return componentResolver.resolveInlineComponent((InlineComponent) component);
        if (component instanceof BatchComponent)
            return componentResolver.resolveBatchComponent((BatchComponent) component);
        if (component instanceof StreamComponent)
            return componentResolver.resolveStreamComponent((StreamComponent) component);
        if (component instanceof EncodeComponent)
            return componentResolver.resolveEncodeComponent((EncodeComponent) component);
        if (component instanceof TransformComponent)
            return componentResolver.resolveTransformComponent((TransformComponent) component);
        if (component instanceof UnionComponent)
            return componentResolver.resolveUnionComponent((UnionComponent) component);
        if (component instanceof FragmentComponent) {
            FragmentComponent fragmentComponent = (FragmentComponent) component;
            fragmentComponent = fragmentComponent.withComponents(resolveComponents(fragmentComponent.getComponents()));
            return componentResolver.resolveFragmentComponent(fragmentComponent);
        }
        if (component instanceof WrapperComponent) {
            WrapperComponent wrapperComponent = (WrapperComponent) component;
            wrapperComponent = wrapperComponent.withComponent(resolveComponent(wrapperComponent.getComponent()));
            return componentResolver.resolveWrapperComponent(wrapperComponent);
        }
        if (component instanceof SqlComponent)
            return componentResolver.resolveSqlComponent((SqlComponent) component);
        if (component instanceof ReferenceComponent)
            return componentResolver.resolveReferenceComponent((ReferenceComponent) component);

        throw new PlanResolverException("component [" + component + "] non managed by " + this.getClass().getName());
    }

}
