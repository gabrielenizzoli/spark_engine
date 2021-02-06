package sparkengine.plan.model.mapper.sql;

import lombok.Value;
import sparkengine.plan.model.component.Component;
import sparkengine.plan.model.component.impl.SqlComponent;
import sparkengine.plan.model.component.mapper.ComponentMapper;

import javax.annotation.Nonnull;
import java.util.HashSet;
import java.util.Set;
import java.util.Stack;
import java.util.stream.Collectors;

@Value(staticConstructor = "of")
public class SqlComponentMapper implements ComponentMapper {

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
