package sparkengine.plan.model.mapper.sql;

import org.junit.jupiter.api.Test;
import sparkengine.plan.model.common.Location;
import sparkengine.plan.model.component.impl.SqlComponent;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SqlComponentMapperTest {

    @Test
    void resolveSqlComponent() throws Exception {

        var sqlComponent = SqlComponent.builder()
                .withUsing(List.of("a", "b"))
                .withSql("select a.id from a join b on a.id = b.id")
                .build();
        var resolvedComponent = (SqlComponent) SqlComponentMapper.of(ResolverMode.VALIDATE, sql -> Set.of("b", "a")).mapSqlComponent(Location.empty(), sqlComponent);
        assertEquals(List.of("a", "b"), resolvedComponent.getUsing());

    }

    @Test
    void resolveSqlComponent_onInfer() throws Exception {
        var sqlComponent = SqlComponent.builder()
                .withUsing(null)
                .withSql("select a.id from a join b on a.id = b.id")
                .build();
        var resolvedComponent = (SqlComponent) SqlComponentMapper.of(ResolverMode.INFER, sql -> Set.of("b", "a")).mapSqlComponent(Location.empty(), sqlComponent);
        assertEquals(List.of("a", "b"), resolvedComponent.getUsing());
    }

    @Test
    void resolveSqlComponent_onErrorWrongDeps() throws Exception {
        var sqlComponent = SqlComponent.builder()
                .withUsing(List.of("a"))
                .withSql("select a.id from a join b on a.id = b.id")
                .build();
        assertThrows(Exception.class, () -> SqlComponentMapper.of(ResolverMode.VALIDATE, sql -> Set.of("b", "a")).mapSqlComponent(Location.empty(), sqlComponent));
    }

    @Test
    void resolveSqlComponent_onErrorEmptyDeps() throws Exception {
        var sqlComponent = SqlComponent.builder()
                .withUsing(List.of())
                .withSql("select a.id from a join b on a.id = b.id")
                .build();
        assertThrows(Exception.class, () -> SqlComponentMapper.of(ResolverMode.VALIDATE, sql -> Set.of("b", "a")).mapSqlComponent(Location.empty(), sqlComponent));
    }

    @Test
    void resolveSqlComponent_onSkip() throws Exception {
        var sqlComponent = SqlComponent.builder()
                .withUsing(List.of())
                .withSql("select a.id from a join b on a.id = b.id")
                .build();
        var resolvedComponent = (SqlComponent) SqlComponentMapper.of(ResolverMode.SKIP, sql -> Set.of("b", "a")).mapSqlComponent(Location.empty(), sqlComponent);
        assertEquals(List.of(), resolvedComponent.getUsing());
    }

}