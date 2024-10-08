package de.hpi.isg;

public class Utils {
    // total time, instantiation time, model construction, optimization time, set to null
    final static long[] optimalTimes = new long[5];
    final static long[] approximateTimes = new long[5];
    final static long[] ilpTimes = new long[5];

    // deletions, instantiations, tree height
    final static long[] approximateCounts = new long[3];
    final static long[] optimalCounts = new long[3];
    final static long[] ilpCounts = new long[3];
}
