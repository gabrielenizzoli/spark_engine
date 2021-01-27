package sparkengine.plan.model.mapper.impl;

import org.junit.jupiter.api.Test;
import sparkengine.plan.model.component.impl.SqlComponent;
import sparkengine.plan.model.mapper.PlanMapperException;

import java.util.List;
import java.util.Set;
import java.util.Stack;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SqlComponentMapperTest {

    @Test
    void resolveSqlComponent() throws Exception {

        var sqlComponent = SqlComponent.builder()
                .withUsing(List.of("a", "b"))
                .withSql("select a.id from a join b on a.id = b.id")
                .build();
        var resolvedComponent = (SqlComponent) SqlComponentMapper.of(SqlComponentMapper.ResolverMode.VALIDATE, sql -> Set.of("b", "a")).mapSqlComponent(new Stack<>(), sqlComponent);
        assertEquals(List.of("a", "b"), resolvedComponent.getUsing());

    }

    @Test
    void resolveSqlComponent_onInfer() throws Exception {
        var sqlComponent = SqlComponent.builder()
                .withUsing(null)
                .withSql("select a.id from a join b on a.id = b.id")
                .build();
        var resolvedComponent = (SqlComponent) SqlComponentMapper.of(SqlComponentMapper.ResolverMode.INFER, sql -> Set.of("b", "a")).mapSqlComponent(new Stack<>(), sqlComponent);
        assertEquals(List.of("a", "b"), resolvedComponent.getUsing());
    }

    @Test
    void resolveSqlComponent_onErrorWrongDeps() throws Exception {
        var sqlComponent = SqlComponent.builder()
                .withUsing(List.of("a"))
                .withSql("select a.id from a join b on a.id = b.id")
                .build();
        assertThrows(Exception.class, () -> SqlComponentMapper.of(SqlComponentMapper.ResolverMode.VALIDATE, sql -> Set.of("b", "a")).mapSqlComponent(new Stack<>(), sqlComponent));
    }

    @Test
    void resolveSqlComponent_onErrorEmptyDeps() throws Exception {
        var sqlComponent = SqlComponent.builder()
                .withUsing(List.of())
                .withSql("select a.id from a join b on a.id = b.id")
                .build();
        assertThrows(Exception.class, () -> SqlComponentMapper.of(SqlComponentMapper.ResolverMode.VALIDATE, sql -> Set.of("b", "a")).mapSqlComponent(new Stack<>(), sqlComponent));
    }

    @Test
    void resolveSqlComponent_onSkip() throws Exception {
        var sqlComponent = SqlComponent.builder()
                .withUsing(List.of())
                .withSql("select a.id from a join b on a.id = b.id")
                .build();
        var resolvedComponent = (SqlComponent) SqlComponentMapper.of(SqlComponentMapper.ResolverMode.SKIP, sql -> Set.of("b", "a")).mapSqlComponent(new Stack<>(), sqlComponent);
        assertEquals(List.of(), resolvedComponent.getUsing());
    }

}