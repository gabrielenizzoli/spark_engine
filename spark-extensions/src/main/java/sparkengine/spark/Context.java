package sparkengine.spark;

public interface Context {


    /**
     * Increase the accumulator value.
     *
     * @param name  name of the accumulator
     * @param value value to increase (or decrease) the accumulator of.
     */
    void acc(String name, long value);

    default void acc(String name) {
        acc(name, 1L);
    }

}
