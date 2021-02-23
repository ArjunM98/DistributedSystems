package ecs.zkwatcher;

import app_kvECS.ECSClient;
import ecs.IECSNode;
import ecs.zk.ZooKeeperService;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

public class ECSServerDeletionWatcher {

    private final IECSNode node;
    private final ECSClient ecs;
    private final ZooKeeperService zk;

    public ECSServerDeletionWatcher(IECSNode node, ZooKeeperService zk, ECSClient ecs) {
        this.node = node;
        this.ecs = ecs;
        this.zk = zk;
    }

    public final Watcher changedState = new Watcher() {
        @Override
        public void process(WatchedEvent watchedEvent) {
            if (watchedEvent.getType() == Event.EventType.NodeDeleted) {
                ecs.handleNodeFailure(node);
            } else try {
                // Start watching again; we don't care about this event at all, no callback required
                zk.getData(ZooKeeperService.ZK_SERVERS + "/" + node.getNodeName());
            } catch (Exception ignored) {
            }
        }
    };

}
