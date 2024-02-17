package org.apache.bookkeeper.conf;

import java.util.Random;

public class InvalidObject {
    /**
     * The hashCode() method in Java returns an integer value that <br>
     * represents the unique identifier of an object. <br>
     * The hashCode() method is required to follow certain rules: <br>
     * <p>
     * Consistency: <br>
     * If an object does not change its internal state, <br>
     * calling hashCode() multiple times should consistently return the same value. <br>
     * <p>
     * Equality: <br>
     * If two objects are equal according to the equals() method, <br>
     * their hashCode() values should be the same. <br>
     * However, the reverse is not necessarily true. <br>
     * <p>
     * Uniqueness: <br>
     * Ideally, each distinct object should have a unique hashCode() value, <br>
     * but due to the limited range of integers, collisions may occur. <br>
     * <p>
     * Using random int returns we are violating what was just explained here making <br>
     * it an invalid instance for Object
     */
    @Override
    public int hashCode() {
        Random random = new Random(System.currentTimeMillis());
        return random.nextInt();
    }
}