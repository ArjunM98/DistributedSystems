package shared;

import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class ResourcePool<T> {
    /**
     * Track count of (theoretically) available resources in the pool
     */
    protected final Semaphore COUNT;

    /**
     * Track available resources for use. This Queue is lazily populated by {@link #acquireResource(long, TimeUnit)}
     */
    protected final Queue<T> AVAILABLE_RESOURCES;

    /**
     * Track acquired resources e.g. for cleanup
     */
    protected final Set<T> ACQUIRED_RESOURCES;

    /**
     * Whether or not this pool is accepting clients
     */
    protected final AtomicBoolean IS_ACTIVE;

    /**
     * Create a resource pool that may grow up to a maximum size.
     *
     * @param size maximum number of resources that may ever be allocated
     */
    public ResourcePool(int size) {
        if (size <= 0) throw new IllegalArgumentException("Size must be positive");

        this.COUNT = new Semaphore(size);
        this.AVAILABLE_RESOURCES = new LinkedList<>();
        this.ACQUIRED_RESOURCES = new LinkedHashSet<>();
        this.IS_ACTIVE = new AtomicBoolean(true);
    }

    /**
     * Clean up resources
     */
    public synchronized void close() {
        if (IS_ACTIVE.getAndSet(false)) {
            for (T resource : AVAILABLE_RESOURCES) closeResource(resource);
            for (T resource : ACQUIRED_RESOURCES) closeResource(resource);
        }
    }

    /**
     * Implementation dependent: create a new resource to be added to the pool
     *
     * @return ready-to-use resource
     * @throws Exception on failure
     */
    protected abstract T createNewResource() throws Exception;

    /**
     * Implementation dependent: close a resource (if applicable)
     *
     * @param resource from the pool
     */
    protected abstract void closeResource(T resource);

    /**
     * Get a resource from the pool. Caller should call {@link #releaseResource(Object)} when finished
     * Blocks until a resource is available, or when given timeout is met.
     *
     * @return resource
     * @throws Exception on failure to acquire (or create) a resource
     */
    public synchronized T acquireResource(long timeout, TimeUnit unit) throws Exception {
        if (!IS_ACTIVE.get()) throw new IllegalStateException("Pool is closed");
        if (!COUNT.tryAcquire(timeout, unit)) throw new TimeoutException("Resource allocation timed out");

        try {
            if (AVAILABLE_RESOURCES.isEmpty()) AVAILABLE_RESOURCES.add(createNewResource());
        } catch (Exception e) {
            COUNT.release();
            throw e;
        }

        final T resource = AVAILABLE_RESOURCES.poll();
        ACQUIRED_RESOURCES.add(resource);
        return resource;
    }

    /**
     * Return a resource back to the pool.
     *
     * @param resource from {@link #acquireResource(long, TimeUnit)}
     */
    public synchronized void releaseResource(T resource) {
        if (!IS_ACTIVE.get()) return;

        if (!ACQUIRED_RESOURCES.remove(resource)) {
            throw new IllegalArgumentException("Resource was not from this pool");
        }
        COUNT.release();
        AVAILABLE_RESOURCES.add(resource);
    }
}
