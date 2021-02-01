package app_kvServer.balancer;

import java.util.List;

public class ModuloLoadBalancer<T> implements ILoadBalancer<T> {
    protected final int nodeCount;

    ModuloLoadBalancer(int nodeCount) {
        this.nodeCount = nodeCount;
    }

    @Override
    public T balanceRequest(String key, List<T> nodes) {
        return nodes.get(Math.abs(key.hashCode()) % this.nodeCount);
    }

    /**
     * A special case of {@link ModuloLoadBalancer} when the node count is a power of two, in which case
     * you can use bitmasks to speed up the modulus operation.
     */
    private static class PowerOfTwoLoadBalancer<T> extends ModuloLoadBalancer<T> {
        final int mask;

        PowerOfTwoLoadBalancer(int nodeCount) {
            super(nodeCount);
            this.mask = nodeCount - 1;
        }

        @Override
        public T balanceRequest(String key, List<T> nodes) {
            return nodes.get(key.hashCode() & this.mask);
        }
    }

    /**
     * @return best possible node balancer for the problem at hand
     */
    public static <T> ModuloLoadBalancer<T> createLoadBalancer(int nodeCount) {
        return ((nodeCount & (nodeCount - 1)) == 0) ? new PowerOfTwoLoadBalancer<>(nodeCount) : new ModuloLoadBalancer<>(nodeCount);
    }
}
