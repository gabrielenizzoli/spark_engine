package dataengine.pipeline.core.supplier;

import lombok.Getter;

import java.util.List;
import java.util.Set;

public class GraphException extends Exception {

    public GraphException(String str) {
        super(str);
    }

    public GraphException(String str, Throwable t) {
        super(str, t);
    }

    public static class MissingNode extends GraphException {

        @Getter
        private final String missingNode;
        @Getter
        private final Set<String> availableNodes;

        public MissingNode(String str, String node, Set<String> nodes) {
            super(str);
            this.missingNode = node;
            this.availableNodes = nodes;
        }

    }

    public static class RepeatedNode extends GraphException {

        @Getter
        private final String repeatedNode;
        @Getter
        private final List<String> path;

        public RepeatedNode(String str, String node, List<String> path) {
            super(str);
            this.repeatedNode = node;
            this.path = path;
        }

    }

}
