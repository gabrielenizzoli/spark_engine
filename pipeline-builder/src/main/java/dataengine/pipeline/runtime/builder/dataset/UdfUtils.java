package dataengine.pipeline.runtime.builder.dataset;

import dataengine.pipeline.runtime.datasetfactory.DatasetFactoryException;
import dataengine.pipeline.model.udf.UdfLibrary;
import dataengine.pipeline.model.udf.UdfList;
import dataengine.spark.sql.udf.SqlFunction;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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
            List<SqlFunction> sqlFunctions = new ArrayList<>(udfList.getOfClasses().size());
            for (String sqlFunctionClass : udfList.getOfClasses()) {
                sqlFunctions.add(getSqlFunction(sqlFunctionClass));
            }
            return Collections.unmodifiableList(sqlFunctions);
        }
        throw new DatasetFactoryException(udfLibrary + " udf library not managed");

    }

    @Nonnull
    public static SqlFunction getSqlFunction(String sqlFunctionClass) throws DatasetFactoryException {
        SqlFunction sqlFunction = null;
        try {
            Class<SqlFunction> c = (Class<SqlFunction>) Class.forName(sqlFunctionClass);
            sqlFunction = c.newInstance();
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new DatasetFactoryException("udf/udaf class " + sqlFunctionClass + " has problems creating instance", e);
        }
        return sqlFunction;
    }

}
