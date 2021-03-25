package sparkengine.plan.runtime.builder.dataset.utils;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import sparkengine.plan.model.udf.*;
import sparkengine.plan.model.udf.UdfWithScalaScript;
import sparkengine.plan.runtime.datasetfactory.DatasetFactoryException;
import sparkengine.spark.sql.logicalplan.functionresolver.Function;
import sparkengine.spark.sql.udf.*;
import sparkengine.spark.sql.udf.context.GlobalUdfContext;
import sparkengine.spark.sql.udf.context.UdfContext;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

public class UdfUtils {

    public static Collection<Function> buildSqlFunctionCollection(@Nullable UdfLibrary udfLibrary,
                                                                  SparkSession sparkSession)
            throws DatasetFactoryException {
        if (udfLibrary == null) {
            return null;
        } else if (udfLibrary instanceof UdfList) {
            UdfList udfList = (UdfList) udfLibrary;
            var functions = new LinkedList<Function>();
            for (Udf udf : udfList.getList()) {
                if (udf instanceof UdfWithClassName) {
                    var udfWithClassName = (UdfWithClassName)udf;
                    var sqlFunction = getUdfWithClassNameFunction(udfWithClassName);
                    var broadcastUdfContext = getUdfContextBroadcast(sparkSession, udfWithClassName.getAccumulators());
                    functions.add(Function.of(sqlFunction, broadcastUdfContext));
                } else if (udf instanceof UdfWithScalaScript) {
                    try {
                        var udfWithScala = (UdfWithScalaScript)udf;
                        var sqlFunction = getUdfWithScalaFunction(udfWithScala);
                        var broadcastUdfContext = getUdfContextBroadcast(sparkSession, udfWithScala.getAccumulators());
                        functions.add(Function.of(sqlFunction, broadcastUdfContext));
                    } catch (UdfCompilationException e) {
                        throw new DatasetFactoryException(String.format("scala udf [%s] can't be compiled", udf), e);
                    }
                } else {
                    throw new DatasetFactoryException(String.format("udf [%s] not managed", udf));
                }
            }
            return Collections.unmodifiableList(functions);
        }
        throw new DatasetFactoryException(String.format("udf library [%s] not managed", udfLibrary));

    }

    @Nullable
    private static Broadcast<UdfContext> getUdfContextBroadcast(SparkSession sparkSession, Map<String, String> accumulatorNames) {
        var broadcastUdfContext = GlobalUdfContext.get()
                .map(ctx -> ctx.withAccumulatorNameRemap(accumulatorNames))
                .map(ctx -> new JavaSparkContext(sparkSession.sparkContext()).broadcast(ctx))
                .orElse(null);
        return broadcastUdfContext;
    }

    @Nonnull
    private static SqlFunction getUdfWithScalaFunction(UdfWithScalaScript udfWithScala) throws UdfCompilationException {
        return ScalaUdfCompiler.compile(udfWithScala.getName(), udfWithScala.getScala());
    }

    @Nonnull
    public static SqlFunction getUdfWithClassNameFunction(UdfWithClassName udfWithClassName) throws DatasetFactoryException {
        try {
            Class<SqlFunction> c = (Class<SqlFunction>) Class.forName(udfWithClassName.getClassName());
            return c.getDeclaredConstructor().newInstance();
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException | NoSuchMethodException | InvocationTargetException e) {
            throw new DatasetFactoryException(String.format("udf/udaf class [%s] has problems creating instance", udfWithClassName.getClassName()), e);
        }
    }

}

