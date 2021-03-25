package sparkengine.spark.sql.logicalplan.functionresolver;

import lombok.Value;
import org.apache.spark.broadcast.Broadcast;
import sparkengine.spark.sql.udf.SqlFunction;
import sparkengine.spark.sql.udf.context.UdfContext;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Value
public class Function {

    @Nonnull
    SqlFunction sqlFunction;
    @Nullable
    Broadcast<UdfContext> udfContextBroadcast;

    public static Function of(@Nonnull SqlFunction sqlFunction) {
        return new Function(sqlFunction, null);
    }

    public static Function of(@Nonnull SqlFunction sqlFunction, Broadcast<UdfContext> udfContextBroadcast) {
        return new Function(sqlFunction, udfContextBroadcast);
    }

    public static List<Function> ofSqlFunctions(SqlFunction... sqlFunctions) {
        return Arrays.stream(sqlFunctions).map(f -> Function.of(f)).collect(Collectors.toList());
    }

    public void addToMap(Map<String, UnresolvedFunctionReplacer> functionReplacers) {
        functionReplacers.put(sqlFunction.getName(), sqlFunction.asFunctionReplacer(udfContextBroadcast));
    }

}
