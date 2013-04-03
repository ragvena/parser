import java.util.concurrent.*;

/**
 * User: eyakovleva
 * Date: 2/20/13
 * Time: 3:05 PM
 */

public class BlockingThreadPoolExecutor extends ThreadPoolExecutor {

    public BlockingThreadPoolExecutor(int poolSize, int queueSize, long keepAliveTime, TimeUnit keepAliveTimeUnit,
                                      long maxBlockingTime, TimeUnit maxBlockingTimeUnit, Callable<Boolean> blockingTimeCallback) {

        super(
                poolSize, // Core size
                poolSize, // Max size
                keepAliveTime,
                keepAliveTimeUnit,
                new ArrayBlockingQueue<Runnable>(
                        // to avoid redundant threads
                        Math.max(poolSize, queueSize)
                ),
                // our own RejectExecutionHandler &ndash; see below
                new BlockThenRunPolicy(
                        maxBlockingTime,
                        maxBlockingTimeUnit,
                        blockingTimeCallback
                )
        );

        super.allowCoreThreadTimeOut(true);
    }

    @Override
    public void setRejectedExecutionHandler(RejectedExecutionHandler h) {
        throw new UnsupportedOperationException("setRejectedExecutionHandler is not allowed on this class.");
    }

    // --------------------------------------------------
    // Inner private class of BlockingThreadPoolExecutor
    // A reject policy that waits on the queue
    // -------------------------------------------------- 
    private static class BlockThenRunPolicy
            implements RejectedExecutionHandler {

        private long blockTimeout;
        private TimeUnit blocTimeoutUnit;
        private Callable<Boolean> blockTimeoutCallback;


        private BlockThenRunPolicy(long blockTimeout, TimeUnit blocTimeoutUnit, Callable<Boolean> blockTimeoutCallback) {
            this.blockTimeout = blockTimeout;
            this.blocTimeoutUnit = blocTimeoutUnit;
            this.blockTimeoutCallback = blockTimeoutCallback;
        }

        // Straight-forward constructor
        // public BlockThenRunPolicy() {}

        // --------------------------------------------------

        @Override
        public void rejectedExecution(
                Runnable task,
                ThreadPoolExecutor executor) {

            BlockingQueue<Runnable> queue = executor.getQueue();
            boolean taskSent = false;

            while (!taskSent) {

                if (executor.isShutdown()) {
                    throw new RejectedExecutionException(
                            "ThreadPoolExecutor has shutdown  while attempting to offer a new task.");
                }

                try {
                    // offer the task to the queue, for a blocking-timeout
                    if (queue.offer(task, blockTimeout, blocTimeoutUnit)) {
                        taskSent = true;
                    } else {
                        // task was not accepted - call the user's Callback
                        Boolean result = null;
                        try {
                            result = blockTimeoutCallback.call();
                        } catch (Exception e) {
                            // wrap the Callback exception and re-throw
                            throw new RejectedExecutionException(e);
                        }
                        // check the Callback result
                        if (!result) {
                            throw new RejectedExecutionException(
                                    "User decided to stop waiting  for task insertion");
                        }
                    }
                } catch (InterruptedException e) {
                    // we need to go back to the offer call...
                }

            } // end of while for InterruptedException 

        } // end of method rejectExecution

        // --------------------------------------------------

    } // end of inner private class BlockThenRunPolicy
}