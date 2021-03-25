package sparkengine.spark.sql.udf;

import org.apache.spark.broadcast.Broadcast;
import sparkengine.spark.sql.logicalplan.functionresolver.UnresolvedFunctionReplacer;
import sparkengine.spark.sql.udf.context.UdfContext;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;

public interface SqlFunction extends Serializable {

    @Nonnull
    String getName();

    @Nonnull
    UnresolvedFunctionReplacer asFunctionReplacer(@Nullable Broadcast<UdfContext> udfContextBroadcast);

}
