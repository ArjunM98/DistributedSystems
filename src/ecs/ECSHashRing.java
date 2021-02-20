package ecs;

import app_kvECS.IECSClient;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ECSHashRing {
    /**
     * A {@link java.util.NavigableMap} representing the hash ring of servers
     */
    private final TreeMap<BigInteger, ECSNode> hashRing = new TreeMap<>();

    /**
     * Parse and construct an ECSHashRing according to the example ecs.config file provided on Quercus
     *
     * @param config string with lines like "server1 localhost 50000"
     * @return constructed ECSHashRing
     * @throws IllegalArgumentException if lines are poorly formatted
     */
    public static ECSHashRing fromConfig(String config) {
        return new ECSHashRing(config.lines().map(ECSNode::fromConfig).toArray(ECSNode[]::new));
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
     * @param nodes ECSNodes to include in the hash ring
     */
    public ECSHashRing(ECSNode... nodes) {
        for (ECSNode node : nodes) this.addServer(node);
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
     * Return a Map of name to nodes.
     *
     * @return functionality for {@link IECSClient#getNodes()}
     */
    public Map<String, IECSNode> getAllNodes() {
        return hashRing.values().stream().collect(Collectors.toMap(ECSNode::getNodeName, Function.identity()));
    }

    /**
     * Return a node by its name
     *
     * @param name node name
     * @return node if found else null
     */
    public ECSNode getNodeByName(String name) {
        return hashRing.values().stream().filter(e -> e.getNodeName().equals(name)).findFirst().orElse(null);
    }

    /**
     * Wrapper for {@link #getServer(BigInteger)}
     *
     * @param payload string holding key or ip:port
     * @return the {@link ECSNode} this payload maps to
     */
    public ECSNode getServer(String payload) {
        return getServer(computeHash(payload));
    }

    /**
     * Get server responsible for the given hash, considering wraparound as defined in Quercus guidelines
     *
     * @param hash {@link BigInteger} representation of an MD5 hash
     * @return the {@link ECSNode} responsible for this hash
     */
    public ECSNode getServer(BigInteger hash) {
        if (hashRing.isEmpty()) return null;
        if (hashRing.size() == 1) return hashRing.firstEntry().getValue();

        if (hash.compareTo(hashRing.lastKey()) > 0) {
            return hashRing.firstEntry().getValue();
        } else {
            return hashRing.ceilingEntry(hash).getValue();
        }
    }

    /**
     * Add a server to the hash ring. Update is done in-place.
     *
     * @param server to add to ring
     * @return true if the hash ring has changed as a result of calling this method
     */
    public boolean addServer(ECSNode server) {
        // Already present, this is a no-op
        if (hashRing.containsKey(server.getNodeHash())) return false;

        if (hashRing.isEmpty()) {
            hashRing.put(server.getNodeHash(), server);
        } else {
            // Tell successor its no longer responsible for this hash
            ECSNode successor = getServer(server.getNodeHash());
            if (successor == null) successor = server;
            successor.setPredecessor(server);

            // Add to hash ring
            hashRing.put(server.getNodeHash(), server);

            // Tell this server its lower bound for hashes
            final Map.Entry<BigInteger, ECSNode> lowerEntry = hashRing.lowerEntry(server.getNodeHash());
            ECSNode predecessor = lowerEntry == null ? hashRing.lastEntry().getValue() : lowerEntry.getValue();
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
    public boolean removeServer(ECSNode server) {
        // Not present, this is a no-op
        if (!hashRing.containsKey(server.getNodeHash())) return false;

        if (hashRing.size() <= 1) {
            hashRing.remove(server.getNodeHash());
        } else {
            // Get predecessor ready
            final Map.Entry<BigInteger, ECSNode> lowerEntry = hashRing.lowerEntry(server.getNodeHash());
            ECSNode predecessor = lowerEntry == null ? null : lowerEntry.getValue();

            // Remove from hash ring
            hashRing.remove(server.getNodeHash());

            // Tell successor it's again responsible for this hash range
            ECSNode successor = getServer(server.getNodeHash());
            if (predecessor == null) predecessor = hashRing.lastEntry().getValue();
            successor.setPredecessor(predecessor);
        }

        return true;
    }
}
