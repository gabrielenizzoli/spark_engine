package sparkengine.plan.model.resolver;

public class PlanResolverException extends Exception {

    public PlanResolverException(String str) {
        super(str);
    }

    public PlanResolverException(String str, Throwable t) {
        super(str, t);
    }

}
