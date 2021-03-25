package sparkengine.plan.runtime.builder.dataset.utils;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import sparkengine.plan.model.udf.*;
import sparkengine.plan.model.udf.UdfWithScalaScript;
import sparkengine.plan.runtime.datasetfactory.DatasetFactoryException;
import sparkengine.spark.sql.logicalplan.functionresolver.UnresolvedFunctionReplacer;
import sparkengine.spark.sql.udf.*;
import sparkengine.spark.sql.udf.context.GlobalUdfContext;
import sparkengine.spark.sql.udf.context.UdfContext;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

public class UdfUtils {

    public static Collection<SqlFunction> buildSqlFunctionCollection(@Nullable UdfLibrary udfLibrary,
                                                                     SparkSession sparkSession)
            throws DatasetFactoryException {
        if (udfLibrary == null) {
            return null;
        } else if (udfLibrary instanceof UdfList) {
            UdfList udfList = (UdfList) udfLibrary;
            List<SqlFunction> sqlFunctions = new ArrayList<>(udfList.getList().size());

            for (Udf udf : udfList.getList()) {
                if (udf instanceof UdfWithClassName) {
                    sqlFunctions.add(getSqlFunction((UdfWithClassName)udf));
                } else if (udf instanceof UdfWithScalaScript) {
                    var udfWithScala = (UdfWithScalaScript)udf;
                    try {
                        sqlFunctions.add(ScalaUdfCompiler.compile(udfWithScala.getName(), udfWithScala.getScala()));
                    } catch (UdfCompilationException e) {
                        throw new DatasetFactoryException(String.format("scala udf [%s] can't be compiled", udf), e);
                    }
                } else {
                    throw new DatasetFactoryException(String.format("udf [%s] not managed", udf));
                }
            }
            return Collections.unmodifiableList(sqlFunctions);
        }
        throw new DatasetFactoryException(String.format("udf library [%s] not managed", udfLibrary));

    }

    @Nonnull
    public static SqlFunction getSqlFunction(UdfWithClassName udfWithClassName) throws DatasetFactoryException {
        try {
            Class<SqlFunction> c = (Class<SqlFunction>) Class.forName(udfWithClassName.getClassName());
            SqlFunction sqlFunction = c.getDeclaredConstructor().newInstance();

            Map<String, String> accumulatorNames = udfWithClassName.getAccumulators();
            sqlFunction = bindUdfContextInSqlFunction(sqlFunction, accumulatorNames);

            return sqlFunction;
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException | NoSuchMethodException | InvocationTargetException e) {
            throw new DatasetFactoryException(String.format("udf/udaf class [%s] has problems creating instance", udfWithClassName.getClassName()), e);
        }
    }

    private static SqlFunction bindUdfContextInSqlFunction(SqlFunction sqlFunction,
                                                           Map<String, String> accumulatorNames) {
        return new SqlFunctionWrapper(sqlFunction) {

            @Nullable
            @Override
            public UdfContext initUdfContext(@Nullable UdfContext templateUdfContext) {
                if (templateUdfContext == null)
                    return null;
                return templateUdfContext.withAccumulatorNameRemap(accumulatorNames);
            }

        };

    }

}

