package app_kvServer.storage;

import java.util.List;

public interface ILoadBalancer<T> {
    /**
     * Fairly distributes KVs across nodes
     *
     * @param key   to assign to storage
     * @param nodes (stores) along which to partition data
     * @return node (store) to be used
     */
    public T balanceRequest(String key, List<T> nodes);
}
