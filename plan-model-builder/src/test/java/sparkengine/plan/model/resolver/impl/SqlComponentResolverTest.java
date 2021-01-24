package sparkengine.plan.model.resolver.impl;

import org.junit.jupiter.api.Test;
import sparkengine.plan.model.component.impl.SqlComponent;
import sparkengine.plan.model.resolver.PlanResolverException;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SqlComponentResolverTest {

    @Test
    void resolveSqlComponent() throws PlanResolverException {

        var sqlComponent = SqlComponent.builder()
                .withUsing(List.of("a", "b"))
                .withSql("select a.id from a join b on a.id = b.id")
                .build();
        var resolvedComponent = (SqlComponent) SqlComponentResolver.of(SqlComponentResolver.ResolverMode.VALIDATE, sql -> Set.of("b", "a")).resolveSqlComponent(sqlComponent);
        assertEquals(List.of("a", "b"), resolvedComponent.getUsing());

    }

    @Test
    void resolveSqlComponent_onInfer() throws PlanResolverException {
        var sqlComponent = SqlComponent.builder()
                .withUsing(null)
                .withSql("select a.id from a join b on a.id = b.id")
                .build();
        var resolvedComponent = (SqlComponent) SqlComponentResolver.of(SqlComponentResolver.ResolverMode.INFER, sql -> Set.of("b", "a")).resolveSqlComponent(sqlComponent);
        assertEquals(List.of("a", "b"), resolvedComponent.getUsing());
    }

    @Test
    void resolveSqlComponent_onErrorWrongDeps() throws PlanResolverException {
        var sqlComponent = SqlComponent.builder()
                .withUsing(List.of("a"))
                .withSql("select a.id from a join b on a.id = b.id")
                .build();
        assertThrows(PlanResolverException.class, () -> SqlComponentResolver.of(SqlComponentResolver.ResolverMode.VALIDATE, sql -> Set.of("b", "a")).resolveSqlComponent(sqlComponent));
    }

    @Test
    void resolveSqlComponent_onErrorEmptyDeps() throws PlanResolverException {
        var sqlComponent = SqlComponent.builder()
                .withUsing(List.of())
                .withSql("select a.id from a join b on a.id = b.id")
                .build();
        assertThrows(PlanResolverException.class, () -> SqlComponentResolver.of(SqlComponentResolver.ResolverMode.VALIDATE, sql -> Set.of("b", "a")).resolveSqlComponent(sqlComponent));
    }

    @Test
    void resolveSqlComponent_onSkip() throws PlanResolverException {
        var sqlComponent = SqlComponent.builder()
                .withUsing(List.of())
                .withSql("select a.id from a join b on a.id = b.id")
                .build();
        var resolvedComponent = (SqlComponent) SqlComponentResolver.of(SqlComponentResolver.ResolverMode.SKIP, sql -> Set.of("b", "a")).resolveSqlComponent(sqlComponent);
        assertEquals(List.of(), resolvedComponent.getUsing());
    }

}