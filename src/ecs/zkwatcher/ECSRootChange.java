package ecs.zkwatcher;

import ecs.ECS;
import ecs.zk.ZooKeeperService;
import org.apache.log4j.Logger;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

import java.util.List;

public class ECSRootChange {

    private static final Logger logger = Logger.getRootLogger();
    private ECS ecs;
    private ZooKeeperService zk;

    public ECSRootChange(ECS ecs, ZooKeeperService zk) {
        this.ecs = ecs;
        this.zk = zk;
    }

    public Watcher childrenChanges = new Watcher() {
        @Override
        public void process(WatchedEvent watchedEvent) {
            zk.getChildren(ECS.ZK_SERVERS, childrenChanges, processChangedChildren);
        }
    };

    public AsyncCallback.Children2Callback processChangedChildren = new AsyncCallback.Children2Callback() {
        @Override
        public void processResult(int i, String s, Object o, List<String> children, Stat stat) {
            ecs.initializeNewServer(children);
        }
    };

}
