package dataengine.spark.sql.function;

import dataengine.spark.sql.PlanMapperException;
import org.apache.spark.sql.catalyst.analysis.UnresolvedFunction;
import org.apache.spark.sql.catalyst.expressions.Expression;

public interface UnresolvedFunctionResolver {

    Expression resolve(UnresolvedFunction unresolvedFunction) throws PlanMapperException;

}
