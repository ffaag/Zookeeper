package com.it.case1;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author ZuYingFang
 * @time 2021-11-24 17:37
 * @description
 */
public class DistributeClient {

    private String connectString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
    private int sessionTimeout = 2000;
    private ZooKeeper zooKeeper;

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {

        DistributeClient client = new DistributeClient();


        // 1 获取zk连接
        client.getConnection();

        // 2 监听/servers下面的子节点的增加和删除
        client.getServerList();

        // 3 业务逻辑（睡觉）
        client.business();


    }

    private void business() throws InterruptedException {

        Thread.sleep(Long.MAX_VALUE);

    }

    private void getServerList() throws InterruptedException, KeeperException {

        List<String> children = zooKeeper.getChildren("/servers", true);

        ArrayList<String> servers = new ArrayList<>();

        for (String child : children) {

            byte[] data = zooKeeper.getData("/servers/" + child, false, null);
            servers.add(new String(data));

        }

        System.out.println(servers);

    }

    private void getConnection() throws IOException {

        zooKeeper = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent event) {

                // 再次启动监听
                try {
                    getServerList();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (KeeperException e) {
                    e.printStackTrace();
                }
            }

        });


    }


}
