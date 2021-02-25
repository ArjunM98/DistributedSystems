package testing.performance;

import app_kvServer.KVServer;
import client.KVStore;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Server01Client05PerformanceTest extends BasePerformanceTest {
    @Override
    protected List<KVStore> generateNewClients() {
        return IntStream.range(0, getNumClients())
                .mapToObj(i -> new KVStore("localhost", 50000))
                .collect(Collectors.toList());
    }

    @Override
    protected List<KVServer> generateNewServers() {
        return Collections.singletonList(new KVServer(50000, CACHE_SIZE, CACHE_STRATEGY.toString()));
    }

    @Override
    protected int getNumClients() {
        return 5;
    }

    @Override
    protected int getNumServers() {
        return 1;
    }
}
