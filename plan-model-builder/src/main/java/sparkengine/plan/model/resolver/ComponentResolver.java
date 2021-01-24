package sparkengine.plan.model.resolver;

import sparkengine.plan.model.component.Component;
import sparkengine.plan.model.component.impl.*;

public interface ComponentResolver {

    default Component resolveEmptyComponent(EmptyComponent component) throws PlanResolverException {
        return component;
    }

    default Component resolveInlineComponent(InlineComponent component) throws PlanResolverException {
        return component;
    }

    default Component resolveBatchComponent(BatchComponent component) throws PlanResolverException {
        return component;
    }

    default Component resolveStreamComponent(StreamComponent component) throws PlanResolverException {
        return component;
    }

    default Component resolveSqlComponent(SqlComponent component) throws PlanResolverException {
        return component;
    }

    default Component resolveEncodeComponent(EncodeComponent component) throws PlanResolverException {
        return component;
    }

    default Component resolveTransformComponent(TransformComponent component) throws PlanResolverException {
        return component;
    }

    default Component resolveReferenceComponent(ReferenceComponent component) throws PlanResolverException {
        return component;
    }

    default Component resolveUnionComponent(UnionComponent component) throws PlanResolverException {
        return component;
    }

    default Component resolveFragmentComponent(FragmentComponent component) throws PlanResolverException {
        return component;
    }

    default Component resolveWrapperComponent(WrapperComponent component) throws PlanResolverException {
        return component;
    }

}
