package org.apache.bookkeeper.conf;

import org.apache.bookkeeper.client.DistributionSchedule;

public class InvalidWriteSet implements DistributionSchedule.WriteSet {
    @Override
    public int size() {throw new ArrayIndexOutOfBoundsException();}
    @Override
    public boolean contains(int i) {
        return false;
    }
    @Override
    public int get(int i) {
        throw new ArrayIndexOutOfBoundsException();
    }
    @Override
    public int set(int i, int index) {
        throw new ArrayIndexOutOfBoundsException();
    }
    @Override
    public void sort() {}
    @Override
    public int indexOf(int index) {
        return -1;
    }
    @Override
    public void addMissingIndices(int maxIndex) {
        throw new ArrayIndexOutOfBoundsException();
    }
    @Override
    public void moveAndShift(int from, int to) {
        throw new ArrayIndexOutOfBoundsException();
    }
    @Override
    public void recycle() {}
    @Override
    public DistributionSchedule.WriteSet copy() {
        return this;
    }


}
