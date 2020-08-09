package dataengine.pipeline.core.source.utils;

import dataengine.pipeline.core.source.factory.DataSourceFactoryException;
import dataengine.pipeline.model.description.udf.UdfLibrary;
import dataengine.pipeline.model.description.udf.UdfList;
import dataengine.spark.sql.udf.Udf;
import dataengine.spark.sql.udf.UdfCollection;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

public class UdfUtils {

    public static UdfCollection buildUdfCollection(@Nullable UdfLibrary udfLibrary)
            throws DataSourceFactoryException {
        if (udfLibrary == null) {
            return null;
        } else if (udfLibrary instanceof UdfList) {
            UdfList udfList = (UdfList) udfLibrary;
            List<Udf> udfs = new ArrayList<>(udfList.getOfClasses().size());
            for (String udfClass : udfList.getOfClasses()) {
                udfs.add(getUdf(udfClass));
            }
            return () -> udfs;
        }
        throw new DataSourceFactoryException(udfLibrary + " udf library not managed");

    }

    @Nonnull
    public static Udf getUdf(String udfClass) throws DataSourceFactoryException {
        Udf udf = null;
        try {
            Class<Udf> c = (Class<Udf>) Class.forName(udfClass);
             udf = c.newInstance();
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new DataSourceFactoryException("udf class " + udfClass + " has problems creating instance", e);
        }
        return udf;
    }

}
