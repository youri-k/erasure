package de.hpi.isg;

public class Utils {
    // unused, instantiation time, model construction, optimization time, set to null
    final static long[] optimalTimes = new long[5];
    final static long[] approximateTimes = new long[5];
    final static long[] ilpTimes = new long[5];

    // deletions, instantiations, tree height
    final static long[] approximateCounts = new long[3];
    final static long[] optimalCounts = new long[3];
    final static long[] ilpCounts = new long[3];


    // deletions, instantiations, tree height
    final static long[] approximateBatchingCounts = new long[3];
    final static long[] optimalBatchingCounts = new long[3];
    final static long[] ilpBatchingCounts = new long[3];
}
