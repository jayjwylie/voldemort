package voldemort.store.stats;

import java.util.Arrays;

import org.apache.log4j.Logger;

import voldemort.annotations.concurrency.Threadsafe;

/**
 * A class for computing percentiles based on a simple histogram.
 * 
 * The histogram starts at 0 and then has uniformly sized buckets. The number of
 * buckets and width of each bucket is specified upon construction. Each bucket
 * in the histogram "counts" the number of values inserted into the histogram
 * that fall into the bucket's range.
 * 
 * All interfaces for adding data to the histogram or querying the histogram for
 * quantiles are synchronized to make this object threadsafe.
 * 
 */
@Threadsafe
public class Histogram {

    private final int nBuckets;
    private final int step;
    private final int[] buckets;
    private final long upperBound;

    private int size;
    private static final Logger logger = Logger.getLogger(Histogram.class);

    /**
     * Initialize an empty histogram
     * 
     * @param nBuckets The number of buckets to use
     * @param step The size (width) of each bucket
     */
    public Histogram(int nBuckets, int step) {
        logger.info("Constructing a histogram with " + nBuckets + " buckets.");
        this.nBuckets = nBuckets;
        this.step = step;
        this.upperBound = step * nBuckets;
        this.buckets = new int[nBuckets];
        reset();
    }

    /**
     * Reset the histogram back to empty (set all values to 0)
     */
    public synchronized void reset() {
        Arrays.fill(buckets, 0);
        size = 0;
    }

    /**
     * Insert a value into the right bucket of the histogram. If the value is
     * larger than any bound, insert into the last bucket. If the value is less
     * than zero, then ignore it.
     * 
     * @param data The value to insert into the histogram
     */
    public synchronized void insert(long data) {
        long index = 0;
        if(data >= this.upperBound) {
            index = nBuckets - 1;
        } else if(data < 0) {
            logger.error(data + " can't be bucketed because it is negative!");
            return;
        } else {
            index = data / step;
        }
        if(index < 0 || index >= nBuckets) {
            // This is dead code. Defend against code changes in future.
            logger.error(data + " can't be bucketed because index is not in range [0,nBuckets).");
            return;
        }
        buckets[(int) index]++;
        size++;
    }

    /**
     * Find the a value <em>n</em> such that the percentile falls within [
     * <em>n</em>, <em>n + step</em>). This method does a <em>LINEAR</em> prove
     * of the histogram. I.e., this method is O(nBuckets).
     * 
     * @param quantile The percentile to find
     * @return Lower bound associated with the percentile
     */
    public synchronized long getQuantile(double quantile) {
        int total = 0;
        for(int i = 0; i < nBuckets; i++) {
            total += buckets[i];
            double currQuantile = ((double) total) / ((double) size);
            if(currQuantile >= quantile) {
                return i * step;
            }
        }
        return 0;
    }

}
