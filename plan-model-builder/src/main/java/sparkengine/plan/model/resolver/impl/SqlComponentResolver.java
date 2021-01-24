package sparkengine.plan.model.resolver.impl;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Value;
import sparkengine.plan.model.component.Component;
import sparkengine.plan.model.component.impl.SqlComponent;
import sparkengine.plan.model.resolver.ComponentResolver;
import sparkengine.plan.model.resolver.PlanResolverException;

import javax.annotation.Nonnull;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

@Value(staticConstructor = "of")
public class SqlComponentResolver implements ComponentResolver {

    @FunctionalInterface
    public interface SqlReferenceFinder {
        Set<String> findReferences(String sql) throws PlanResolverException;
    }

    @AllArgsConstructor
    @Getter
    public enum ResolverMode {
        SKIP(false, false),
        VALIDATE(true, false),
        INFER(false, true);

        boolean validate;
        boolean replace;
    }

    @Nonnull
    ResolverMode resolverMode;
    @Nonnull
    SqlReferenceFinder sqlReferenceFinder;

    @Override
    public Component resolveSqlComponent(SqlComponent component) throws PlanResolverException {

        if (resolverMode == ResolverMode.SKIP) {
            return component;
        }

        var declaredDependencies = component.getUsing() == null ? Set.<String>of() : new HashSet<String>(component.getUsing());
        var discoveredDependencies = sqlReferenceFinder.findReferences(component.getSql());

        if (resolverMode.isValidate() && !declaredDependencies.equals(discoveredDependencies)) {
            throw new PlanResolverException("while validating for sql component [" + component + "] declared and discovered dependencies are not matching: " + declaredDependencies + " != " + discoveredDependencies);
        }

        if (resolverMode.isReplace()) {
            component = component.withUsing(discoveredDependencies.stream().sorted().collect(Collectors.toList()));
        }

        return component;
    }

}
