package sparkengine.plan.model;

import java.util.Stack;

public class LocationUtils {

    public static Stack<String> empty() {
        return new Stack<>();
    }

    public static Stack<String> of(String... parts) {
        var stack = new Stack<String>();
        for (var part : parts)
            stack.push(part);
        return stack;
    }

    public static Stack<String> push(Stack<String> location, String value) {
        Stack<String> stack = new Stack<>();
        stack.addAll(location);
        stack.push(value);
        return stack;
    }

}
