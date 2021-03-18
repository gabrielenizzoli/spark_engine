package sparkengine.plan.runtime.builder.dataset.utils;

import sparkengine.plan.model.udf.*;
import sparkengine.plan.runtime.datasetfactory.DatasetFactoryException;
import sparkengine.spark.sql.udf.ScalaUdfCompiler;
import sparkengine.spark.sql.udf.SqlFunction;
import sparkengine.spark.sql.udf.UdfCompilationException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class UdfUtils {

    public static Collection<SqlFunction> buildSqlFunctionCollection(@Nullable UdfLibrary udfLibrary)
            throws DatasetFactoryException {
        if (udfLibrary == null) {
            return null;
        } else if (udfLibrary instanceof UdfList) {
            UdfList udfList = (UdfList) udfLibrary;
            List<SqlFunction> sqlFunctions = new ArrayList<>(udfList.getList().size());

            for (Udf udf : udfList.getList()) {
                if (udf instanceof UdfWithClassName) {
                    sqlFunctions.add(getSqlFunction(((UdfWithClassName)udf).getClassName()));
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
    public static SqlFunction getSqlFunction(String sqlFunctionClass) throws DatasetFactoryException {
        SqlFunction sqlFunction = null;
        try {
            Class<SqlFunction> c = (Class<SqlFunction>) Class.forName(sqlFunctionClass);
            sqlFunction = c.getDeclaredConstructor().newInstance();
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException | NoSuchMethodException | InvocationTargetException e) {
            throw new DatasetFactoryException("udf/udaf class " + sqlFunctionClass + " has problems creating instance", e);
        }
        return sqlFunction;
    }

}
