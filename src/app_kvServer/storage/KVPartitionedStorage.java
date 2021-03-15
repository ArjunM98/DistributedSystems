package app_kvServer.storage;

import app_kvServer.KVServerException;
import app_kvServer.balancer.ILoadBalancer;
import app_kvServer.balancer.ModuloLoadBalancer;

import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * An orchestrator for {@link KVSingleFileStorage} to increase concurrency
 */
public class KVPartitionedStorage implements IKVStorage {
    private static final int NUM_PERSISTENT_STORES = 8;
    private static final ILoadBalancer<KVSingleFileStorage> loadBalancer = ModuloLoadBalancer.create(NUM_PERSISTENT_STORES);

    private final List<KVSingleFileStorage> stores;

    public KVPartitionedStorage(String directory) {
        stores = IntStream.rangeClosed(1, NUM_PERSISTENT_STORES)
                .mapToObj(i -> new KVSingleFileStorage(directory, String.format("store%d.txt", i)))
                .collect(Collectors.toUnmodifiableList());
    }

    @Override
    public boolean inStorage(String key) {
        return loadBalancer.balanceRequest(key, stores).inStorage(key);
    }

    @Override
    public String getKV(String key) throws KVServerException {
        return loadBalancer.balanceRequest(key, stores).getKV(key);
    }

    @Override
    public void putKV(String key, String value) {
        loadBalancer.balanceRequest(key, stores).putKV(key, value);
    }

    @Override
    public void delete(String key) throws KVServerException {
        loadBalancer.balanceRequest(key, stores).delete(key);
    }

    @Override
    public void clearStorage() {
        stores.forEach(KVSingleFileStorage::clearStorage);
    }

    @Override
    public Stream<KVPair> openKvStream(Predicate<KVPair> filter) {
        return stores.stream()
                .map(store -> store.openKvStream(filter))
                .reduce(Stream::concat)
                .orElseGet(Stream::empty);
    }

    @Override
    public void deleteIf(Predicate<KVPair> filter) {
        stores.forEach(store -> store.deleteIf(filter));
    }
}

