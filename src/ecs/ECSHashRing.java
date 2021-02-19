package ecs;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.TreeMap;

public class ECSHashRing {
    /**
     * {@link java.util.SortedMap} representing the hash ring of servers
     */
    private final TreeMap<BigInteger, ECSNode> hashRing = new TreeMap<>();

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
     */
    public void addServer(ECSNode server) {
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
    }

    /**
     * Remove a server from the hash ring. Update is done in-place.
     *
     * @param server to remove from ring
     */
    public void removeServer(ECSNode server) {
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
    }
}
