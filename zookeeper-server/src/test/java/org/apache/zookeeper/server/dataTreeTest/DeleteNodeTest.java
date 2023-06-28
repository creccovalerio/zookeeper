package org.apache.zookeeper.server.dataTreeTest;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.server.DataNode;
import org.apache.zookeeper.server.DataTree;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import java.util.*;
import static org.junit.Assert.assertEquals;


@RunWith(Parameterized.class)
public class DeleteNodeTest {
    private String path;
    private long zxid;
    private DataTree dtValidNodes;
    private int lengthDtValidNodes;
    private DataTree dtInvalidNode;
    private DataTree dtNoParent;
    private DataTree dtCheckPzxid;
    private DataTree dtEphemeral;
    private int lengthEphemeral;
    private DataTree dtContainer;
    private int lengthCont;
    private DataTree dtTTL;
    private int lengthTTL;
    private int testType;
    private int count;
    private final Class<? extends Exception> expectedException;

    public DeleteNodeTest(String path, long zxid, int testType, Class<? extends Exception> expectedException) throws KeeperException.NoNodeException, KeeperException.NodeExistsException {
        this.path = path;
        this.zxid = zxid;
        this.testType = testType;
        this.expectedException = expectedException;
        this.dtInvalidNode = new DataTree();

        if(testType == 1){
            this.dtValidNodes = new DataTree();
            this.dtEphemeral = new DataTree();
            this.dtTTL = new DataTree();
            this.dtContainer = new DataTree();

            String currentPath;
            String prevPath = "/";
            String nodeParent = "/";
            count = 1;
            String[] pathNodes = splitPath(path);
            int n = pathNodes.length;

            //Creating Tree with all non-ephemeral nodes
            for(String pathNode: pathNodes){
                currentPath =  prevPath  + pathNode;
                this.dtValidNodes.createNode(currentPath, new byte[500], ZooDefs.Ids.CREATOR_ALL_ACL, 0, this.dtValidNodes.getNode(nodeParent).stat.getCversion(), 0, 1);
                prevPath = currentPath;
                nodeParent = currentPath;
                if(count != 1){
                    count++;
                    prevPath += "/";
                    continue;
                }
                count++;
            }
            this.lengthDtValidNodes = n;

            count = 1;
            prevPath = "/";
            nodeParent = "/";

            //Creating Tree with ephemeral last node
            for(String pathNode: pathNodes){
                currentPath =  prevPath  + pathNode;
                if(count == pathNodes.length)
                    this.dtEphemeral.createNode(currentPath, new byte[500], ZooDefs.Ids.CREATOR_ALL_ACL, 1, this.dtEphemeral.getNode(nodeParent).stat.getCversion(), 0, 1);
                else
                    this.dtEphemeral.createNode(currentPath, new byte[500], ZooDefs.Ids.CREATOR_ALL_ACL, 0, this.dtEphemeral.getNode(nodeParent).stat.getCversion(), 0, 1);
                prevPath = currentPath;
                nodeParent = currentPath;
                if(count!=1){
                    count++;
                    prevPath += "/";
                    continue;
                }
                count++;
            }
            this.lengthEphemeral = n;

            //Creating Tree with TTL last node
            System.setProperty("zookeeper.extendedTypesEnabled", "true");
            count = 1;
            prevPath = "/";
            nodeParent = "/";
            for(String pathNode: pathNodes){
                currentPath =  prevPath  + pathNode;
                if(count == pathNodes.length)
                    this.dtTTL.createNode(currentPath, new byte[500], ZooDefs.Ids.CREATOR_ALL_ACL, 0xFF00000000000001L, this.dtTTL.getNode(nodeParent).stat.getCversion(), 0, 1);
                else
                    this.dtTTL.createNode(currentPath, new byte[500], ZooDefs.Ids.CREATOR_ALL_ACL, 0, this.dtTTL.getNode(nodeParent).stat.getCversion(), 0, 1);
                prevPath = currentPath;
                nodeParent = currentPath;
                if(count!=1){
                    count++;
                    prevPath += "/";
                    continue;
                }
                count++;
            }
            this.lengthTTL = n;

            //Creating Tree with Container last node
            count = 1;
            prevPath = "/";
            nodeParent = "/";
            for(String pathNode: pathNodes){
                currentPath =  prevPath  + pathNode;
                if(count==pathNodes.length)
                    this.dtContainer.createNode(currentPath, new byte[500], ZooDefs.Ids.CREATOR_ALL_ACL, 0x8000000000000000L, this.dtContainer.getNode(nodeParent).stat.getCversion(), 0, 1);
                else
                    this.dtContainer.createNode(currentPath, new byte[500], ZooDefs.Ids.CREATOR_ALL_ACL, 0, this.dtContainer.getNode(nodeParent).stat.getCversion(), 0, 1);
                prevPath = currentPath;
                nodeParent = currentPath;
                if(count!=1){
                    count++;
                    prevPath += "/";
                    continue;
                }
                count++;
            }
            this.lengthCont = n;
        }

        if(testType == 3){
            this.dtNoParent = new DataTree();
            String currentPath;
            String prevPath = "/";
            String nodeParent = "/";
            this.path = path;
            this.zxid = zxid;
            count = 1;

            String[] pathNodes = this.splitPath(path);

            //Creating Tree with all non-ephemeral nodes
            for(String pathNode: pathNodes){
                currentPath =  prevPath  + pathNode;
                this.dtNoParent.createNode(currentPath, new byte[500], ZooDefs.Ids.CREATOR_ALL_ACL, 0, dtNoParent.getNode(nodeParent).stat.getCversion(), 0, 1);
                prevPath = currentPath;
                nodeParent = currentPath;
                if(count!=1){
                    count++;
                    prevPath += "/";
                    continue;
                }
                count++;
            }
            int indexLastSlash = path.lastIndexOf('/');
            String parentNode = path.substring(0, indexLastSlash);
            dtNoParent.deleteNode(parentNode, 0);
        }

        if(testType == 4){
            String currentPath;
            String prevPath = "/";
            String nodeParent = "/";
            this.dtCheckPzxid = new DataTree();
            this.path = path;
            this.zxid = zxid;
            int count = 1;

            String[] pathNodes = splitPath(path);

            //Creating Tree with all non-ephemeral nodes
            for(String pathNode: pathNodes){
                currentPath =  prevPath  + pathNode;
                this.dtCheckPzxid.createNode(currentPath, new byte[500], ZooDefs.Ids.CREATOR_ALL_ACL, 0, dtCheckPzxid.getNode(nodeParent).stat.getCversion(), 0, 1);
                prevPath = currentPath;
                nodeParent = currentPath;
                if(count != 1){
                    count++;
                    prevPath += "/";
                }
                count++;
            }
        }

    }

    private String[] splitPath(String path){
        String[] pathNodes;
        pathNodes = path.split("/");
        return pathNodes;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() {
        return Arrays.asList(new Object[][] {
                // deleting an existing node
                {"/zn1/zn2/zn3/zn4/zn5"     ,        -1 , 1, null                                 },
                {"/zn.1/zn.2"               ,         5 , 1, null                                 },
                {"/zn1/zn2"                 ,         0 , 1, null                                 },
                {"/zn1"                     ,   1000000 , 1, null                                 },
                {"/zn1/zn2/zn3"             ,  -1234567 , 1, null                                 },

                //deleting a NON-existing node
                {"/zn1"                     ,     10000 , 2, KeeperException.NoNodeException.class},
                {null                       ,         0 , 2, NullPointerException.class           },
                {""                         ,        -2 , 2, StringIndexOutOfBoundsException.class},

                //deleting no parent
                {"/zn1/zn2/zn3/zn4/zn5/zn6" ,         1 , 3, KeeperException.NoNodeException.class},
                {"/zn1/zn2/zn3"             ,     10000 , 3, KeeperException.NoNodeException.class},
                {"/zn1/zn2"                 ,         0 , 3, KeeperException.NoNodeException.class},

                // deleting checking pzxid
                {"/zn1/zn2/zn3/zn4/zn5"     ,        -1 , 1, null                                 },
                {"/zn.1/zn.2"               ,         5 , 1, null                                 },
                {"/zn1/zn2"                 ,         0 , 1, null                                 },
                {"/zn1"                     ,   1000000 , 1, null                                 },
                {"/zn1/zn2/zn3"             ,  -1234567 , 1, null                                 },
        });
    }

    @Test
    public void testDeleteValidNode(){
        if(testType == 1){
            if(expectedException == null){
                Assertions.assertDoesNotThrow(() -> {
                    this.dtValidNodes.deleteNode(this.path, this.zxid);
                    this.dtEphemeral.deleteNode(this.path, this.zxid);
                    this.dtContainer.deleteNode(this.path, this.zxid);
                    this.dtTTL.deleteNode(this.path, this.zxid);
                    assertEquals(this.lengthDtValidNodes-1, this.dtValidNodes.getNodeCount()-4);
                    assertEquals(this.lengthEphemeral-1, this.dtEphemeral.getNodeCount()-4);
                    assertEquals(this.lengthCont-1, this.dtContainer.getNodeCount()-4);
                    assertEquals(this.lengthTTL-1, this.dtTTL.getNodeCount()-4);
                });
            }
        }
    }

    @Test
    public void testDeleteInvalidNode(){
        if(testType == 2){
            if(expectedException != null){
                Assertions.assertThrows(expectedException, () ->{
                    this.dtInvalidNode.deleteNode(this.path, this.zxid);
                    Assertions.fail();
                });
            }
        }
    }

    @Test
    public void testDeleteNoParent(){
        if(testType == 3){
            if(expectedException != null){
                Assertions.assertThrows(expectedException, () ->{
                    this.dtNoParent.deleteNode(this.path, this.zxid);
                    Assertions.fail();
                });
            }
        }
    }

    @Test
    public void testDeleteCheckingPzxid() {

        if(testType == 4){
            Assertions.assertDoesNotThrow(() -> {
                int lastIndex = this.path.lastIndexOf('/');
                String parentString = this.path.substring(0, lastIndex);
                DataNode parentNode = dtCheckPzxid.getNode(parentString);
                long parentPxzid = parentNode.stat.getPzxid();
                dtCheckPzxid.deleteNode(this.path, this.zxid);
                parentNode = dtCheckPzxid.getNode(parentString);
                if(parentPxzid < this.zxid) {
                    assertEquals(this.zxid, parentNode.stat.getPzxid());
                } else{
                    assertEquals(parentPxzid, parentNode.stat.getPzxid());
                }

            });
        }
    }

}

