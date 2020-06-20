package dataengine.pipeline.model.pipeline.step;

import java.util.List;

public interface MultiInputStep extends Step {

    List<String> getUsing();

}
