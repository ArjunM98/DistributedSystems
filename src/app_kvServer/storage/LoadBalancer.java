package app_kvServer.storage;

public class LoadBalancer implements ILoadBalancer {

    public LoadBalancer() {

    }

    @Override
    public int getStoreIndex(String key, int numStores) {
        return (int) (key.charAt(0))  % numStores;
    }
}
