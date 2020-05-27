import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public class Zookeeperdemo {

          ZooKeeper zk;

          @Before
          public void connect() throws InterruptedException, IOException {
              final CountDownLatch cdl=new CountDownLatch(1);
              zk=new ZooKeeper("10.42.130.77:2181", 5000, new Watcher() {
                  public void process(WatchedEvent watchedEvent) {
                      if (watchedEvent.getState()==Event.KeeperState.SyncConnected){
                          System.out.println("连接成功");
                      }
                      cdl.countDown();
                  }
              });
              cdl.await();
          }

          //判断节点是否存在
         @Test
          public void exist() throws KeeperException, InterruptedException {
             Stat exists = zk.exists("/log", null);
             System.out.println(exists);
         }

          //监控节点的数据是否被修改
         @Test
         public void nodeDataChanged() throws KeeperException, InterruptedException {
             final  CountDownLatch cdl=new CountDownLatch(1);
              zk.getData("/log", new Watcher() {
                  public void process(WatchedEvent watchedEvent) {
                           if(watchedEvent.getType()==Event.EventType.NodeDataChanged){
                               System.out.println("节点数据被修改");
                           }else{
                               System.out.println("没有改");
                           }
                   cdl.countDown();
                  }
              },null);
           cdl.await();
         }
        @Test
         public void nodeChildrenChanged() throws KeeperException, InterruptedException {
              final CountDownLatch cdl =new CountDownLatch(1);
              zk.getChildren("/log", new Watcher() {
                  public void process(WatchedEvent watchedEvent) {
                      if (watchedEvent.getType()==Event.EventType.NodeChildrenChanged){
                          System.out.println("子节点个数发生改变");
                      }
                      cdl.countDown();
                  }
              });
              cdl.await();
         }
       //监控节点的变化--是被创建还是被删除
        @Test
        public void  nodeChanged() throws KeeperException, InterruptedException {
               final CountDownLatch cdl=new CountDownLatch(1);
              zk.exists("/log", new Watcher() {
                 public void process(WatchedEvent watchedEvent) {
                     if(watchedEvent.getType()==Event.EventType.NodeCreated){
                         System.out.println("节点发生改变");
                     }

                     if(watchedEvent.getType()==Event.EventType.NodeDeleted){
                         System.out.println("节点已被删除");
                     }
                     cdl.countDown();
                 }
             });
              cdl.await();
       }

}
