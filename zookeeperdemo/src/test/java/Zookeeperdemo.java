import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class Zookeeperdemo {

    private  ZooKeeper zk;
    //第一个参数是连接ip+端口号
    //第二个参数表示回话超时时间,默认单位是ms
    //第三个参数表示监控者,用于监控连接是否建立
    @Before
    public void connect() throws IOException, InterruptedException {
        final CountDownLatch cdl=new CountDownLatch(1);
         zk=new ZooKeeper("10.42.130.77:2181", 5000,
                new Watcher(){
                    public void process(WatchedEvent watchedEvent) {
                        if(watchedEvent.getState()==Event.KeeperState.SyncConnected){
                            System.out.println("连接zookeeper成功");

                        }
                        cdl.countDown();
                    }
                }
        );
        cdl.await();
        System.out.println("主线程结束,先抢占执行");
    }
    @Test
    public void createNode() throws KeeperException, InterruptedException {
        //path -路径
        //data-节点数据
        //acl -权限策略
        //createMode -节点类型
        String path=zk.create("/log","用于管理日志".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println(path);
    }

     @Test
    public void setData() throws KeeperException, InterruptedException {
        //path节点路径
         //data-节点数据
         //version -数据版本 ->dataversion
         //在修改数据的时候,回去检查version和节点的dataVersion是否一致
         //如果一致才能修改
         Stat s=zk.setData("/log","吃鸡大吉".getBytes(),0);
         System.out.println();

     }

     //第三个参数表示节点信息
    //如果需要节点信息,那么可以创建一个Stat对象传入
    //stat s =new Stat();
     @Test
     public void getData() throws KeeperException, InterruptedException {
        byte [] data=zk.getData("/log",null,null);
         System.out.println(data);
     }


     //String path,int version
    //当版本不一致的时候会删除失败
    //如果需要墙纸执行而忽略版本号,那么版本号设置为-1
    //要求没有子节点
    @Test
    public void deleteNode() throws KeeperException, InterruptedException {
        zk.delete("/log",-1);
    }

    //获取子节点
    @Test
    public void getChridren() throws KeeperException, InterruptedException {
        List<String> children = zk.getChildren("/log", null);

        for (String c:children
             ) {
            System.out.println(c);

        }
    }

}
