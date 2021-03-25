package sparkengine.spark.sql.udf;

import org.apache.spark.broadcast.Broadcast;
import sparkengine.spark.sql.logicalplan.functionresolver.UnresolvedFunctionReplacer;
import sparkengine.spark.sql.udf.context.UdfContext;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

public class SqlFunctionWrapper implements SqlFunction {

    @Nonnull
    private final SqlFunction sqlFunction;

    public SqlFunctionWrapper(@Nonnull SqlFunction sqlFunction) {
        this.sqlFunction = Objects.requireNonNull(sqlFunction);
    }

    protected final SqlFunction getSqlFunction() {
        return sqlFunction;
    }

    @Nonnull
    @Override
    public String getName() {
        return sqlFunction.getName();
    }

    @Override
    public UnresolvedFunctionReplacer asFunctionReplacer(@Nullable Broadcast<UdfContext> udfContextBroadcast) {
        return sqlFunction.asFunctionReplacer(udfContextBroadcast);
    }

}
