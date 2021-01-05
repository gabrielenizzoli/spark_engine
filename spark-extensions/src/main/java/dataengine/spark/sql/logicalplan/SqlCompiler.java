package dataengine.spark.sql.logicalplan;

import dataengine.spark.sql.logicalplan.functionresolver.FunctionResolver;
import dataengine.spark.sql.logicalplan.tableresolver.Table;
import dataengine.spark.sql.logicalplan.tableresolver.TableResolver;
import dataengine.spark.sql.udf.SqlFunction;
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
    public static Dataset<Row> sql(@Nullable Collection<Table> tables,
                                   @Nullable Collection<SqlFunction> sqlFunctions,
                                   @Nonnull String sql) throws PlanMapperException {
        var sqlCompiler = SqlCompiler.builder()
                .tableResolver(tables)
                .functionResolver(sqlFunctions)
                .build();

        return sqlCompiler.sql(SparkSession.active(), sql);
    }

    public static Dataset<Row> sql(@Nullable Collection<SqlFunction> sqlFunctions,
                                   @Nonnull String sql) throws PlanMapperException {
        return sql(Collections.emptyList(), sqlFunctions, sql);
    }

    public Dataset<Row> sql(@Nonnull String sql) throws PlanMapperException {
        return sql(SparkSession.active(), sql);
    }

    public Dataset<Row> sql(@Nonnull SparkSession sparkSession,
                            @Nonnull String sql) throws PlanMapperException {
        return Dataset.ofRows(sparkSession, sqlToLogicalPlan(sparkSession, sql));
    }

    private LogicalPlan sqlToLogicalPlan(@Nonnull SparkSession sparkSession,
                                         @Nonnull String sql) throws PlanMapperException {
        var logicalPlan = generateLogicalPlan(sparkSession, sql);
        var remappedLogicalPlan = remapLogicalPlan(logicalPlan);
        return remappedLogicalPlan;
    }

    private static LogicalPlan generateLogicalPlan(@Nonnull SparkSession sparkSession,
                                                   @Nonnull String sql) throws PlanMapperException {
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
