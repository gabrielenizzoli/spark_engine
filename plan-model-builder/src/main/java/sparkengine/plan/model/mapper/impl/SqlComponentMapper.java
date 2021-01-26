package sparkengine.plan.model.mapper.impl;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Value;
import sparkengine.plan.model.component.Component;
import sparkengine.plan.model.component.impl.SqlComponent;
import sparkengine.plan.model.component.mapper.ComponentMapper;
import sparkengine.plan.model.mapper.PlanMapperException;

import javax.annotation.Nonnull;
import java.util.HashSet;
import java.util.Set;
import java.util.Stack;
import java.util.stream.Collectors;

@Value(staticConstructor = "of")
public class SqlComponentMapper implements ComponentMapper {

    @FunctionalInterface
    public interface SqlReferenceFinder {
        Set<String> findReferences(String sql) throws PlanMapperException;
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
    public Component mapSqlComponent(Stack<String> location, SqlComponent component) throws Exception {

        if (resolverMode == ResolverMode.SKIP) {
            return component;
        }

        var declaredDependencies = component.getUsing() == null ? Set.<String>of() : new HashSet<String>(component.getUsing());
        var discoveredDependencies = sqlReferenceFinder.findReferences(component.getSql());

        if (resolverMode.isValidate() && !declaredDependencies.equals(discoveredDependencies)) {
            throw new IllegalArgumentException("declared and discovered dependencies are not matching: " + declaredDependencies + " != " + discoveredDependencies);
        }

        if (resolverMode.isReplace()) {
            component = component.withUsing(discoveredDependencies.stream().sorted().collect(Collectors.toList()));
        }

        return component;
    }

}
