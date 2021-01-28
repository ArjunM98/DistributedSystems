package app_kvServer.storage;

public interface ILoadBalancer {
    /**
     * Fairly distributes KVs across stores
     *
     * @return index of store to be used
     */
    public int getStoreIndex(String key, int numStores);
}
