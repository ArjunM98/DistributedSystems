package ecs;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * TODO: consider rewriting as a proper {@link Collection}
 */
public class ECSHashRing<T extends ECSNode> {
    /**
     * A {@link java.util.NavigableMap} representing the hash ring of servers
     */
    private final TreeMap<BigInteger, T> hashRing = new TreeMap<>();

    /**
     * Parse and construct an ECSHashRing according to the example ecs.config file provided on Quercus
     *
     * @param config     string with lines like "server1 localhost 50000"
     * @param fromConfig function that maps a string to an ECSNode-like object
     * @return constructed ECSHashRing
     * @throws IllegalArgumentException if lines are poorly formatted
     */
    public static <T extends ECSNode> ECSHashRing<T> fromConfig(String config, Function<String, T> fromConfig) {
        return new ECSHashRing<>(config.lines().map(fromConfig).collect(Collectors.toList()));
    }

    /**
     * Serialize this ECSHashRing into the format provided in ecs.config
     *
     * @return string representation of this node
     */
    public String toConfig() {
        return this.hashRing.values().stream().map(ECSNode::toConfig).collect(Collectors.joining("\n"));
    }

    /**
     * Construct an ECSHashRing with zero or more given nodes
     *
     * @param nodes to include in the hash ring
     */
    @SafeVarargs
    public ECSHashRing(T... nodes) {
        this.addAll(Arrays.asList(nodes));
    }

    /**
     * Construct an ECSHashRing from the provided collection
     *
     * @param nodes to include in the hash ring
     */
    public ECSHashRing(Collection<T> nodes) {
        this.addAll(nodes);
    }

    /**
     * Return a deep copy of this hash ring
     *
     * @param copyConstructor for the class of node this hash ring holds
     * @return deep copy of this hash ring
     */
    public ECSHashRing<T> deepCopy(Function<T, T> copyConstructor) {
        return new ECSHashRing<>(getAllNodes().stream().map(copyConstructor).collect(Collectors.toList()));
    }

    /**
     * @param payload is the string to hash
     * @return MD5 Hash of String in {@link BigInteger} format
     */
    public static BigInteger computeHash(String payload) {
        try {
            final MessageDigest md5 = MessageDigest.getInstance("MD5");
            md5.update(payload.getBytes());
            return new BigInteger(1, md5.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("System does not support MD5 hashing");
        }
    }

    /**
     * Get a mutable collection of all nodes in this hash ring, no guarantees on order even though it's a list.
     * It's a shallow copy so modifications to entries in the list will be written through, but modifications to the
     * list itself are completely safe.
     *
     * @return list of nodes
     */
    public List<T> getAllNodes() {
        return new ArrayList<>(hashRing.values());
    }

    /**
     * Return a node by its name
     *
     * @param name node name
     * @return node if found else null
     */
    public T getNodeByName(String name) {
        return hashRing.values().stream().filter(e -> e.getNodeName().equals(name)).findFirst().orElse(null);
    }

    /**
     * Wrapper for {@link #getServer(BigInteger)}
     *
     * @param payload string holding key or ip:port
     * @return the node this payload maps to
     */
    public T getServer(String payload) {
        return getServer(computeHash(payload));
    }

    /**
     * Get server responsible for the given hash, considering wraparound as defined in Quercus guidelines
     *
     * @param hash {@link BigInteger} representation of an MD5 hash
     * @return the node responsible for this hash
     */
    public T getServer(BigInteger hash) {
        if (hashRing.isEmpty()) return null;
        if (hashRing.size() == 1) return hashRing.firstEntry().getValue();

        if (hash.compareTo(hashRing.lastKey()) > 0) {
            return hashRing.firstEntry().getValue();
        } else {
            return hashRing.ceilingEntry(hash).getValue();
        }
    }

    /**
     * Return the server which would precede the passed-in server in the hash ring
     *
     * @param server (does not have to be in the ring) to get predecessor for
     * @return predecessor (or potential predecessor) for server, or server if hash ring is empty
     */
    public T getPredecessor(T server) {
        if (hashRing.isEmpty()) return server;

        Map.Entry<BigInteger, T> predecessorEntry = hashRing.lowerEntry(server.getNodeHash());
        if (predecessorEntry == null) predecessorEntry = hashRing.lastEntry();
        return predecessorEntry.getValue();
    }

    /**
     * Return the nth predecessor in reference to the passed-in server
     *
     * @param server server (does not have to be in the ring) to get predecessor for
     * @param n which predecessor to get in reference to server
     * @return
     */
    public T getNthPredecessor(T server, int n, boolean wrapAround) {
        if (hashRing.isEmpty() || (!wrapAround && hashRing.size() <= n)) return server;

        T predecessor = server;
        for (int i = 0; i < n; i++) {
            predecessor = this.getPredecessor(predecessor);
        }
        return predecessor;
    }

    /**
     * Return the server which would succeed the passed-in server in the hash ring
     *
     * @param server (does not have to be in the ring) to get successor for
     * @return successor (or potential successor) for server, or server if hash ring is empty
     */
    public T getSuccessor(T server) {
        if (hashRing.isEmpty()) return server;

        final BigInteger nodeHash = server.getNodeHash();
        if (hashRing.containsKey(nodeHash)) {
            Map.Entry<BigInteger, T> successorEntry = hashRing.higherEntry(nodeHash);
            if (successorEntry == null) successorEntry = hashRing.firstEntry();
            return successorEntry.getValue();
        } else {
            return getServer(nodeHash);
        }
    }

    /**
     * Return the nth successor in reference to the passed-in server
     *
     * @param server server (does not have to be in the ring) to get successor for
     * @param n which successor to get in reference to server
     * @return
     */
    public T getNthSuccessor(T server, int n, boolean wrapAround) {
        if (hashRing.isEmpty() || (!wrapAround && hashRing.size() <= n)) return server;

        T successor = server;
        for (int i = 0; i < n; i++) {
            successor = this.getSuccessor(successor);
        }
        return successor;
    }

    /**
     * Add a server to the hash ring. Update is done in-place.
     *
     * @param server to add to ring
     * @return true if the hash ring has changed as a result of calling this method
     */
    public boolean addServer(T server) {
        // Already present, this is a no-op
        if (hashRing.containsKey(server.getNodeHash())) return false;

        if (hashRing.isEmpty()) {
            hashRing.put(server.getNodeHash(), server);
        } else {
            final T successor = getSuccessor(server), predecessor = getPredecessor(server);
            hashRing.put(server.getNodeHash(), server);
            successor.setPredecessor(server);
            server.setPredecessor(predecessor);
        }

        return hashRing.containsKey(server.getNodeHash());
    }

    /**
     * Remove a server from the hash ring. Update is done in-place.
     *
     * @param server to remove from ring
     * @return true if the hash ring has changed as a result of calling this method
     */
    public boolean removeServer(T server) {
        // Not present, this is a no-op
        if (!hashRing.containsKey(server.getNodeHash())) return false;

        if (hashRing.size() <= 1) {
            hashRing.remove(server.getNodeHash());
        } else {
            final T successor = getSuccessor(server), predecessor = getPredecessor(server);
            hashRing.remove(server.getNodeHash());
            successor.setPredecessor(predecessor);
        }

        return true;
    }

    /**
     * Wrapper for {@link #addAll(Collection)}
     *
     * @param other the other hash ring to merge into this one
     * @return true if the hash ring has changed as a result of calling this method (does not mean all add operations were successful though)
     */
    public boolean addAll(ECSHashRing<T> other) {
        return this.addAll(other.hashRing.values());
    }

    /**
     * Add multiple servers to this hash ring
     *
     * @param servers to add to the ring
     * @return true if the hash ring has changed as a result of calling this method (does not mean all add operations were successful though)
     */
    public boolean addAll(Collection<T> servers) {
        boolean result = false;
        for (T server : servers) result |= addServer(server);
        return result;
    }

    /**
     * See {@link Collection#clear()}
     */
    public void clear() {
        hashRing.clear();
    }

    /**
     * See {@link Collection#size()}
     */
    public int size() {
        return hashRing.size();
    }
}
