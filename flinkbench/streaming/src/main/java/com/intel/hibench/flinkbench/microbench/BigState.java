package com.intel.hibench.flinkbench.microbench;

public class BigState {
    public int[] arr;

    public BigState() {}
  
    public BigState(int[] arr) {
        this.arr = arr;
    }

    public int[] getArr() {
        return this.arr;
    }

    public void setArr(int[] arr) {
        this.arr = arr;
    }
}