package sparkengine.plan.model.mapper.sql;

import sparkengine.plan.model.plan.mapper.PlanMapperException;

import java.util.Set;

@FunctionalInterface
public interface SqlReferenceFinder {
    Set<String> findReferences(String sql) throws PlanMapperException;
}
