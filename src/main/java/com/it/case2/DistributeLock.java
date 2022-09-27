package com.it.case2;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * @author ZuYingFang
 * @time 2021-11-25 10:59
 * @description   zookeeper部署后，会占用8080端口，和tomcat一样，因此如果两个服务都开的话要改一下zk的端口，admin.serverPort=
 */
public class DistributeLock {

    private final String connectString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
    private final int sessionTimeout = 2000;
    private final ZooKeeper zooKeeper;

    private CountDownLatch connectLatch = new CountDownLatch(1);

    private CountDownLatch waitLatch = new CountDownLatch(1);

    private String waitPath;  // 监听的前一个结点的路径
    private String currentNode;


    public  DistributeLock() throws IOException, InterruptedException, KeeperException {

        // 获取连接
        zooKeeper = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                // connectLatch 如果连接上zookeeper，可以释放掉
                if (event.getState() == Event.KeeperState.SyncConnected) {
                    connectLatch.countDown();
                }

                // waitLatch 获取到监听之后也要释放
                if (event.getType() == Event.EventType.NodeDeleted && event.getPath().equals(waitPath)) {
                    waitLatch.countDown();
                }
            }
        });

        // 等待zookeeper创建成功，成功就继续往下走，不成功就阻塞在这里
        connectLatch.await();

        // 判断根节点/locks是否存在，不存在要自行创建
        Stat exists = zooKeeper.exists("/locks", false);

        if (exists == null) {
            // 节点不存在，我们要创建根节点，存在那就不用管了
            zooKeeper.create("/locks", "locks".getBytes(StandardCharsets.UTF_8), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

    }


    // 给zk加锁
    public void zkLock() throws InterruptedException, KeeperException {

        // 创建相应的临时带序号节点
        currentNode = zooKeeper.create("/locks/" + "seq-", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

        // 判断节点序号是否是最小的，如果是则获取到锁，如果不是则监听序号的前一个节点
        List<String> children = zooKeeper.getChildren("/locks", false);

        // 如果children只有一个值，那就直接获取锁，如果有多个，就判断谁最小
        if (children.size() == 1) {
            return;
        }else {
            // 对children进行排序，会直接影响到children
            Collections.sort(children);
            // 获取到当前节点的名称seq-00000000
            String thisNode = currentNode.substring("/locks/".length());
            // 通过seq-00000000获取到节点在children中的位置
            int index = children.indexOf(thisNode);

            // 判断节点如果在最前面，则获取锁，否则监听他的前一个节点
            if (index == -1) {
                System.out.println("数据异常");
            }else if(index == 0){
                return;  // 表示获取到锁
            }else {
                // 监听前一个结点，先获取到这个节点的路径，然后通过路径去获取值的方法来监听
                waitPath = "/locks/" + children.get(index-1);
                zooKeeper.getData(waitPath, true, null);

                // 等待监听完成再执行下一步的操作，就是一个代码健壮性的完善
                waitLatch.await();
                // 获取到监听了，结束
                return;
            }
        }
    }


    // 给zk解锁
    public void unZkLock() throws InterruptedException, KeeperException {

        // 用完资源后，直接删除掉这个临时节点
        zooKeeper.delete(currentNode, -1);

    }


}
