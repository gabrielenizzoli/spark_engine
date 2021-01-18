package sparkengine.spark.sql.logicalplan;

import sparkengine.spark.sql.logicalplan.functionresolver.FunctionResolver;
import sparkengine.spark.sql.logicalplan.tableresolver.Table;
import sparkengine.spark.sql.logicalplan.tableresolver.TableResolver;
import sparkengine.spark.sql.udf.SqlFunction;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

@Value
@Builder
public class SqlCompiler {

    @Nonnull
    SparkSession sparkSession;
    @Singular
    @Nonnull
    List<LogicalPlanMapper> planMappers;

    public static class Builder {

        public Builder tableResolver(Table... tables) {
            return planMapper(TableResolver.builder().tables(tables).build());
        }

        public Builder tableResolver(@Nullable Collection<Table> tables) {
            return planMapper(TableResolver.builder().tables(tables).build());
        }

        public Builder functionResolver(SqlFunction... sqlFunctions) {
            return planMapper(FunctionResolver.builder().sqlFunctions(sqlFunctions).build());
        }

        public Builder functionResolver(Collection<SqlFunction> sqlFunctions) {
            return planMapper(FunctionResolver.builder().sqlFunctions(sqlFunctions).build());
        }

    }

    public static SqlCompiler emptyCompiler() {
        return SqlCompiler.builder().tableResolver().functionResolver().build();
    }

    /**
     * Utility method that creates a sql compiler and compiles a sql statement into a dataset
     *
     * @param tables       tables to be resolved during compilation
     * @param sqlFunctions function s to be resolved during compilation
     * @param sql          sql statement
     * @return dataset
     */
    public static Dataset<Row> sql(@Nonnull SparkSession sparkSession,
                                   @Nullable Collection<Table> tables,
                                   @Nullable Collection<SqlFunction> sqlFunctions,
                                   @Nonnull String sql) throws PlanMapperException {
        var sqlCompiler = SqlCompiler.builder()
                .sparkSession(sparkSession)
                .tableResolver(tables)
                .functionResolver(sqlFunctions)
                .build();

        return sqlCompiler.sql(sql);
    }

    public static Dataset<Row> sql(@Nonnull SparkSession sparkSession,
                                   @Nullable Collection<SqlFunction> sqlFunctions,
                                   @Nonnull String sql) throws PlanMapperException {
        return sql(sparkSession, Collections.emptyList(), sqlFunctions, sql);
    }

    public Dataset<Row> sql(@Nonnull String sql) throws PlanMapperException {
        return Dataset.ofRows(sparkSession, sqlToLogicalPlan(sql));
    }

    private LogicalPlan sqlToLogicalPlan(@Nonnull String sql) throws PlanMapperException {
        var logicalPlan = generateLogicalPlan(sql);
        var remappedLogicalPlan = remapLogicalPlan(logicalPlan);
        return remappedLogicalPlan;
    }

    private LogicalPlan generateLogicalPlan(@Nonnull String sql) throws PlanMapperException {
        try {
            return sparkSession.sessionState().sqlParser().parsePlan(sql);
        } catch (ParseException e) {
            throw new PlanMapperException("can't parse sql: " + sql, e);
        }
    }

    @Nonnull
    private LogicalPlan remapLogicalPlan(@Nonnull final LogicalPlan logicalPlan) throws PlanMapperException {

        // shortcuts to just return input plan if no mapper provided
        if (planMappers.isEmpty())
            return logicalPlan;

        var remappedLogicalPlan = logicalPlan;
        for (LogicalPlanMapper planMapper : planMappers) {
            if (planMapper == null)
                continue;
            remappedLogicalPlan = planMapper.map(remappedLogicalPlan);
        }
        return remappedLogicalPlan;
    }

}
