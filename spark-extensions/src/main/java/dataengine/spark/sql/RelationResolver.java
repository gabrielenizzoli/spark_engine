package dataengine.spark.sql;

import dataengine.scala.compat.JavaToScalaFunction1;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.function.Function;

@Value
@Builder
public class RelationResolver implements Function<LogicalPlan, LogicalPlan> {

    @Singular
    @Nonnull
    Map<String, LogicalPlan> plans;

    @Override
    @SuppressWarnings("unchecked")
    public LogicalPlan apply(LogicalPlan logicalPlan) {
        if (!(logicalPlan instanceof UnresolvedRelation))
            return logicalPlan.mapChildren(new JavaToScalaFunction1<>(this));

        LogicalPlan resolvedRelation = plans.get(((UnresolvedRelation) logicalPlan).tableName());
        // TODO exception if not found
        if (resolvedRelation == null)
            return logicalPlan.mapChildren(new JavaToScalaFunction1<>(this));
        return resolvedRelation;
    }

}
