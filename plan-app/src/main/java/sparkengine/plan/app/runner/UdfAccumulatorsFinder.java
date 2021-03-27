package sparkengine.plan.app.runner;

import sparkengine.plan.model.common.Location;
import sparkengine.plan.model.component.impl.SqlComponent;
import sparkengine.plan.model.component.visitor.ComponentVisitor;
import sparkengine.plan.model.udf.UdfLibrary;
import sparkengine.plan.model.udf.UdfList;

import javax.annotation.Nonnull;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class UdfAccumulatorsFinder implements ComponentVisitor {

    public Set<String> getAccumulatorNames() {
        return accumulatorNames;
    }

    private final Set<String> accumulatorNames = new HashSet<>();

    @Override
    public void visitSqlComponent(Location location, SqlComponent component) throws Exception {
        var udfLibrary = component.getUdfs();
        if (udfLibrary == null)
            return;
        findAccumulatorNames(udfLibrary);
    }

    private void findAccumulatorNames(@Nonnull UdfLibrary udfLibrary) {
        if (udfLibrary instanceof UdfList) {
            for (var udf : ((UdfList) udfLibrary).getList()) {
                accumulatorNames.addAll(Optional.ofNullable(udf.getAccumulators()).orElse(Map.of()).values());
            }
        }
    }

}
