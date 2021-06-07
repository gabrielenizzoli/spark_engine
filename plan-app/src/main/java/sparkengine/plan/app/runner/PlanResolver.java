package sparkengine.plan.app.runner;

import lombok.Value;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;
import sparkengine.plan.model.builder.ResourceLocationBuilder;
import sparkengine.plan.model.builder.input.AppResourceLocator;
import sparkengine.plan.model.mapper.parameters.ParameterReplacerComponentMapper;
import sparkengine.plan.model.mapper.parameters.ParameterReplacerPlanMapper;
import sparkengine.plan.model.mapper.pipeline.PipelinesReorderingPlanMapper;
import sparkengine.plan.model.mapper.reference.PlanMapperThatReplacesReferences;
import sparkengine.plan.model.mapper.sql.SqlPlanMapper;
import sparkengine.plan.model.plan.Plan;
import sparkengine.plan.model.plan.mapper.PlanMapper;
import sparkengine.plan.model.plan.mapper.PlanMapperException;
import sparkengine.plan.model.plan.mapper.PlanMappers;
import sparkengine.spark.sql.logicalplan.PlanExplorerException;
import sparkengine.spark.sql.logicalplan.tablelist.TableListExplorer;

import javax.annotation.Nonnull;
import java.util.HashMap;

@Value(staticConstructor = "of")
public class PlanResolver implements PlanMapper {

    private static final String PREFIX_DEFAULT = "${";
    private static final String POSTFIX_DEFAULT = "}";

    @Nonnull
    String planLocation;
    @Nonnull
    RuntimeArgs runtimeArgs;
    @Nonnull
    SparkSession sparkSession;
    @Nonnull
    Logger log;

    @Nonnull
    @Override
    public Plan map(@Nonnull Plan sourcePlan) throws PlanMapperException {
        if (runtimeArgs.isSkipResolution()) {
            log.info("skipping resolution of the plan, resolved plan will be the same as the source plan");
            return sourcePlan;
        }

        var resolvedPlan = PlanMappers
                .ofMappers(
                        getReferencePlanResolver(),
                        getSqlResolver(),
                        getParameterResolver(),
                        PipelinesReorderingPlanMapper.of())
                .map(sourcePlan);

        if (log.isTraceEnabled()) {
            log.trace(String.format("resolved plan [%s]", resolvedPlan));
        }

        return resolvedPlan;
    }

    private PlanMapper getReferencePlanResolver() {
        var resourceLocationBuilder = new ResourceLocationBuilder(planLocation, "_", "yaml");
        var resourceLocator = new AppResourceLocator();
        return PlanMapperThatReplacesReferences.of(resourceLocationBuilder, resourceLocator);
    }

    private PlanMapper getSqlResolver() {
        return SqlPlanMapper.of(runtimeArgs.getSqlResolutionMode(), sql -> {
            try {
                return TableListExplorer.findTableListInSql(sparkSession, sql);
            } catch (PlanExplorerException e) {
                throw new PlanMapperException(String.format("error resolving sql tables in sql [%s]", sql), e);
            }
        });
    }

    private PlanMapper getParameterResolver() {

        var parameters = new HashMap<String, String>();
        if (runtimeArgs.isParametersFromEnvironment()) {
            parameters.putAll(System.getenv());
        }
        parameters.putAll(runtimeArgs.getParameters());

        if (log.isInfoEnabled() && !parameters.isEmpty()) {
            var logMsg = new StringBuilder("PARAMETERS:");
            parameters.forEach((k,v) -> logMsg.append("\n").append(k).append("=").append(v));
            log.info(logMsg.toString());
        }

        return ParameterReplacerPlanMapper.of(parameters, PREFIX_DEFAULT, POSTFIX_DEFAULT);
    }

}
