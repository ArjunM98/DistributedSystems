package client;

import ecs.ECSHashRing;
import ecs.ECSNode;
import org.apache.log4j.Logger;
import shared.ResourcePool;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class KVStorePool extends ResourcePool<KVStore> {
    private static final Logger logger = Logger.getRootLogger();

    /**
     * Metadata cache to pull new connections from
     */
    private final ECSHashRing<ECSNode> hashRing = new ECSHashRing<>();

    public KVStorePool(int size) {
        super(size);
    }

    @Override
    public synchronized void releaseResource(KVStore resource) {
        super.releaseResource(resource);
        resource.suggestMetadataUpdate(hashRing.toConfig());
    }

    /**
     * @param newConfig from {@link ecs.zk.ZooKeeperService#ZK_METADATA}
     */
    public void updateMetadata(byte[] newConfig) {
        logger.debug("Received metadata update");
        try {
            final ECSHashRing<ECSNode> newRing = ECSHashRing.fromConfig(new String(newConfig, StandardCharsets.UTF_8), ECSNode::fromConfig);
            hashRing.clear();
            hashRing.addAll(newRing);
        } catch (Exception e) {
            hashRing.clear();
        }
    }

    @Override
    protected KVStore createNewResource() throws Exception {
        if (hashRing.size() == 0) throw new IllegalStateException("ECS unavailable");
        final KVStore candidate = new KVStore(hashRing);
        try {
            candidate.connect();
            logger.info("Created new KVStore resource");
            return candidate;
        } catch (Exception e) {
            closeResource(candidate);
            Logger.getRootLogger().error("KV Store", e);
            throw new IOException("Unable to create KVStore resource", e);
        }
    }

    @Override
    protected void closeResource(KVStore resource) {
        resource.disconnect();
    }
}
