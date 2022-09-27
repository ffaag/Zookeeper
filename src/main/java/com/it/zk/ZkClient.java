package com.it.zk;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * @author ZuYingFang
 * @time 2021-11-23 15:57
 * @description
 *
 * 图中Main()线程作为客户端，当在主线程中创建Zookeeper客户端时，会默认创建两个子线程：Listener和connect
 * connect线程负责将某一操作对应的的监听事件发送给Zookeeper服务集群。Zookeeper收到监听事件后会在该操作对应的监听器列表中注册该事件。
 * 比如图中的获取节点“/”的子节点getChildren这一事件，并设置了true，表示监听此事件，那么Zookeeper就会在监听器列表中注册该事件。
 * 一旦“/”节点的子节点发生变化，getChildren的结果就随之发生变化，Zookeeper就会通知客户端的Listener线程，Listener就会去调用process
 * 方法对“/”的变化做出应对处理。“/”的变化可能是客户端不能控制的，但是为了适应这种变化，客户端在收到服务器的通知后可根据自身情况做出应对。
 *
 */
public class ZkClient {

    // 注意，逗号左右不能有空格，连接地址
    private String connectString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
    // 超时时间，单位毫秒，超过2000还没连上就不连了
    private int sessionTimeout = 2000;
    // zookeeper客户端
    private ZooKeeper zkClient;


    // 获取Zookeeper客户端，用于连接Zookeeper集群，其功能类似于Linux中启动./zkCli.sh
    @Before
    public void init() throws IOException {
        // watcher为监听器
        zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                System.out.println("----------------------------");
                List<String> children = null;
                // 获取指定目录下的所有节点
                try {
                    // 客户端调用方法查看节点下的所有子节点
                    children = zkClient.getChildren("/", true);
                    for (String child : children) {
                        System.out.println(child);
                    }
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println("----------------------------");
            }
        });

    }


    // 创建节点
    @Test
    public void create() throws InterruptedException, KeeperException {

        String node = zkClient.create("/xiaofang", "zhenshuai".getBytes(StandardCharsets.UTF_8), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

    }


    // 监听节点
    @Test
    public void getChildren() throws InterruptedException, KeeperException {

        // 获取指定目录下的所有节点，使用上面那个重写了的监听器，因此只要方法不死会一直监听的
        /*
        List<String> children = zooKeeper.getChildren("/", true);执行后，Zookeeper会在/节点的监听列表中注册一个监听事件，
        如果该节点发生变化，就通知给申请监听的客户端的listener，并将该监听事件从节点的监听列表中删除
        如果被监听的节点发生变化，会调用监听器的process方法，可以在process方法中再次调用getChildren方法并申请对目标节点的监听
        通过这个小技巧使得监听的次数变得无限多了。
         */
        List<String> children = zkClient.getChildren("/", true);  // 使用上面的那个匿名监听器
        for (String child : children) {
            System.out.println(child);
        }

        // 延时，一直不死，，这样服务器端新创建了节点我们这能看到，只要执行zk的api指令，就会走上面监听器的重写方法
        // 从某种意义上更像是客户端那样持续保持连接，所以必须让getChildren方法处在执行的过程中。
        Thread.sleep(Long.MAX_VALUE);

    }


    // 判断子节点是否存在
    @Test
    public void exist() throws InterruptedException, KeeperException {

        Stat exists = zkClient.exists("/xiaofang", false);  // 不开启监听，因此只会调用一次

        // 这里要把上面重写的方法里面的内容注释掉，因为他会继续调用getChildren方法继续监听
        System.out.println(exists == null ? "not exist" : "exist");

    }


}
