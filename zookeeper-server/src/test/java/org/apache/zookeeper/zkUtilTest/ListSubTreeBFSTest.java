package org.apache.zookeeper.zkUtilTest;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.*;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.junit.Assert.assertEquals;
import static org.apache.zookeeper.utils.Tree.*;

@RunWith(value = Parameterized.class)
public class ListSubTreeBFSTest {

    private ZooKeeper zk;
    private Node root;
    private List<String> bfs;
    private final Class<? extends Exception> expectedException;

    private enum  ObjType{
        NULL, VALID, INVALID
    }

    public ListSubTreeBFSTest(ObjType zkEntry, Node root, Class<? extends Exception> expectedException) throws KeeperException, InterruptedException{
        this.root = root;
        this.bfs  = new ArrayList<>();
        this.expectedException = expectedException;

        switch (zkEntry){
            case NULL:
                this.zk = null;
                break;
            case VALID:
                this.zk = mock(ZooKeeper.class);

                if (root.data.equals("znRoot")){
                    this.root.left = createNode("znChild1");
                    this.root.right = createNode("znChild2");
                    this.bfs.add("/" + this.root.data);
                    this.bfs.add("/" + this.root.data+"/"+"znChild1");
                    this.bfs.add("/" + this.root.data+"/"+"znChild2");
                } else if (root.data.equals("")){
                    this.root.left = createNode("znChild1");
                    this.bfs.add("/" + this.root.data);
                    this.bfs.add("/" + this.root.data+"znChild1");
                } else {
                    this.bfs.add("/" + this.root.data);
                }


                when(this.zk.getChildren("/" + this.root.data, false)).
                        thenReturn(getChildrenNodes(this.root, new ArrayList<>()));
                when(this.zk.getChildren("/" + this.root.data+"/"+"znChild1", false)).
                        thenReturn(getChildrenNodes(this.root.left, new ArrayList<>()));
                when(this.zk.getChildren("/" + this.root.data+"/"+"znChild2", false)).
                        thenReturn(getChildrenNodes(this.root.right, new ArrayList<>()));

                break;

            case INVALID:
                this.zk = mock(ZooKeeper.class);
                when(zk.getChildren("/" + this.root.data , false)).
                        thenReturn(getChildrenNodes(null, null));
                break;
        }

    }

    @Parameters
    public static Collection parameters() {
        return Arrays.asList(new Object[][]{
                {ObjType.NULL    , createNode("znRoot")  , NullPointerException.class},
                {ObjType.NULL    , createNode("/\uFFFF") , NullPointerException.class},
                {ObjType.NULL    , createNode("")        , NullPointerException.class},
                {ObjType.INVALID , createNode("znRoot")  , NullPointerException.class},
                {ObjType.INVALID , createNode("/\uFFFF") , NullPointerException.class},
                {ObjType.INVALID , createNode("")        , NullPointerException.class},
                {ObjType.VALID   , createNode("znRoot")  , null},
                {ObjType.VALID   , createNode("/\uFFFF") , null},
                {ObjType.VALID   , createNode("")        , null},

        });
    }

    @Test
    public void listTest(){

        if(expectedException == null){
            Assertions.assertDoesNotThrow(() ->{
                List<String> result = ZKUtil.listSubTreeBFS(this.zk, "/" + this.root.data);
                assertEquals(this.bfs, result);
            });

        }else{
            Assertions.assertThrows(expectedException, ()->{
                ZKUtil.listSubTreeBFS(this.zk, "/" + this.root.data);
                Assertions.fail();
            });
        }

    }

}