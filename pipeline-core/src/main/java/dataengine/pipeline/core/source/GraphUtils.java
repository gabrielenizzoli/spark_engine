package dataengine.pipeline.core.source;

import javax.annotation.Nonnull;
import java.util.*;

public class GraphUtils {

    public static Set<List<String>> pathsToSources(@Nonnull Set<String> roots,
                                                   @Nonnull Map<String, Set<String>> graph) throws GraphException {

        // validate graph
        Objects.requireNonNull(graph, "graph is null");
        for (String node : graph.keySet()) {
            Objects.requireNonNull(graph.get(node));
            for (String childNode : graph.get(node))
                Objects.requireNonNull(childNode);
        }

        Set<List<String>> validPaths = new HashSet<>();

        Queue<List<String>> paths = new LinkedList<>();
        for (String root : Objects.requireNonNull(roots, "root nodes are null")) {
            if (!graph.containsKey(Objects.requireNonNull(root, "a root nodes is null")))
                throw new GraphException.MissingNode("graph does not contain root " + root, root, graph.keySet());
            List<String> startPath = new LinkedList<>();
            startPath.add(root);
            paths.add(startPath);
        }

        while (!paths.isEmpty()) {
            List<String> path = paths.remove();

            String latest = path.get(path.size() - 1);
            if (!graph.containsKey(latest))
                throw new GraphException.MissingNode("graph does not contain node " + latest + " in path " + path, latest, graph.keySet());

            Set<String> children = graph.get(latest);
            if (children.isEmpty()) {
                validPaths.add(path);
            } else {
                for (String child : children) {
                    if (path.contains(child))
                        throw new GraphException.RepeatedNode("path " + path + " already has node " + child, child, path);
                    List<String> newPath = new LinkedList<>(path);
                    newPath.add(child);
                    paths.add(newPath);
                }
            }

        }

        return validPaths;
    }

}
