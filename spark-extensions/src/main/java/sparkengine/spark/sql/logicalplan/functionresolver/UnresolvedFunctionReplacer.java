package sparkengine.spark.sql.logicalplan.functionresolver;

import org.apache.spark.sql.catalyst.analysis.UnresolvedFunction;
import org.apache.spark.sql.catalyst.expressions.Expression;

import javax.annotation.Nonnull;

public interface UnresolvedFunctionReplacer {

    /**
     * Substitute an undefined function with a well defined expression.
     *
     * @param unresolvedFunction input parameter
     * @return the replacement expression for the input
     * @throws FunctionResolverException if any issue arise during the replacement (like undefined or unknown function), then an exception is thrown
     */
    Expression replace(@Nonnull UnresolvedFunction unresolvedFunction) throws FunctionResolverException;

}
