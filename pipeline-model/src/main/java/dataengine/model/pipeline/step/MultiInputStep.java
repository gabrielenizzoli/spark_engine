package dataengine.model.pipeline.step;

import java.util.List;

public interface MultiInputStep extends Step {

    List<String> getUsing();

}
