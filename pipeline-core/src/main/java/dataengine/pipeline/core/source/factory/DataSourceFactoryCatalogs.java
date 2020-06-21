package dataengine.pipeline.core.source.factory;

import dataengine.pipeline.core.source.DataSource;
import dataengine.pipeline.core.DataFactoryException;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;

@Value
@Builder
public class DataSourceFactoryCatalogs implements DataSourceFactory {

    /**
     * A catalog name is a dot-separated name where everything before the dot is the catalog name,
     * and everything after is the source name inside the catalog.
     */
    @Value
    public static class QualifiedName {
        @Nullable
        String catalogName;
        @Nonnull
        String sourceName;

        public static QualifiedName builder(String name) {
            int index = name.indexOf(".");
            if (index < 0)
                return new QualifiedName(null, name);
            return new QualifiedName(name.substring(0, index), name.substring(index + 1));
        }
    }

    @Nullable
    DataSourceFactory defaultCatalog;
    @Nonnull
    @Singular
    Map<String, DataSourceFactory> catalogs;

    @Override
    public DataSource apply(String name) {
        QualifiedName qualifiedName = QualifiedName.builder(name);
        if (qualifiedName.getCatalogName() == null) {
            if (defaultCatalog != null) {
                return defaultCatalog.apply(qualifiedName.getSourceName());
            }
            throw new DataFactoryException("can't find datasource with name " + qualifiedName);
        }
        DataSourceFactory catalog = catalogs.get(qualifiedName.getCatalogName());
        if (catalog == null) {
            throw new DataFactoryException("can't find catalog for datasource with name " + qualifiedName);
        }
        return catalog.apply(qualifiedName.getSourceName());
    }

}
