package sparkengine.spark.sql.logicalplan.tablelist;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.Value;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.PlanExpression;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import sparkengine.spark.sql.logicalplan.ExpressionExplorer;
import sparkengine.spark.sql.logicalplan.LogicalPlanExplorer;
import sparkengine.spark.sql.logicalplan.PlanExplorerException;
import sparkengine.spark.sql.logicalplan.PlanMapperException;

import javax.annotation.Nonnull;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

@EqualsAndHashCode
@ToString
public class TableListExplorer implements LogicalPlanExplorer {

    @Getter
    private final Set<String> tableNames = new HashSet<>();

    public static Set<String> findTableListInSql(@Nonnull SparkSession sparkSession, @Nonnull String sql) throws PlanExplorerException {
        var logicalPlan = generateLogicalPlan(sparkSession, sql);
        var explorer = new TableListExplorer();
        explorer.exploreLogicalPlan(logicalPlan);
        return explorer.getTableNames();
    }

    private static LogicalPlan generateLogicalPlan(@Nonnull SparkSession sparkSession, @Nonnull String sql) throws PlanExplorerException {
        try {
            return sparkSession.sessionState().sqlParser().parsePlan(sql);
        } catch (ParseException e) {
            throw new PlanExplorerException("can;t compile spark sql: " + sql, e);
        }
    }

    @Override
    public void exploreLogicalPlan(LogicalPlan logicalPlan) {
        if (logicalPlan instanceof UnresolvedRelation) {
            collectUnresolvedRelation((UnresolvedRelation) logicalPlan);
        } else {
            TableListExpressionExplorer.of(this).exploreExpressionsInsideLogicalPlan(logicalPlan);
            exploreChildrenOfLogicalPlan(logicalPlan);
        }
    }

    private void collectUnresolvedRelation(UnresolvedRelation unresolvedRelation) {
        Optional.ofNullable(unresolvedRelation.tableName())
                .map(String::strip)
                .filter(tableName -> !tableName.isBlank())
                .ifPresent(tableNames::add);
    }

    @Value(staticConstructor = "of")
    public static class TableListExpressionExplorer implements ExpressionExplorer {

        TableListExplorer tableListExplorer;

        @Override
        public void exploreExpression(Expression expression) {
            if (expression instanceof PlanExpression) {
                var subQuery = (PlanExpression<LogicalPlan>) expression;
                tableListExplorer.exploreLogicalPlan(subQuery.plan());
            }

            exploreChildrenOfExpression(expression);
        }

    }

}
