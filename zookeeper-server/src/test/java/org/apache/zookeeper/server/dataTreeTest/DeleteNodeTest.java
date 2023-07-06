package org.apache.zookeeper.server.dataTreeTest;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Quotas;
import org.apache.zookeeper.ZooDefs;;
import org.apache.zookeeper.server.DataNode;
import org.apache.zookeeper.server.DataTree;
import org.junit.Assert;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private DataTree dtQuotas;
    private DataTree dtMutant;
    private int testType;
    private int count;
    private final Class<? extends Exception> expectedException;
    private static final Logger LOG = LoggerFactory.getLogger(DataTree.class);
    public DeleteNodeTest(String path, long zxid, int testType, Class<? extends Exception> expectedException) throws KeeperException.NoNodeException, KeeperException.NodeExistsException {
        this.path = path;
        this.zxid = zxid;
        this.testType = testType;
        this.expectedException = expectedException;

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

        if(testType == 2){
            this.dtInvalidNode = new DataTree();
        }

        if(testType == 3){
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

        if(testType == 4){
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

        if (testType == 5) {
            dtQuotas = new DataTree();
            this.dtQuotas.createNode(Quotas.procZookeeper+"/parent", new byte[500], null, 0, 0,1, 1);
        }
        if (testType == 6){
            this.dtMutant = new DataTree();
            this.dtMutant.createNode("/zn1", new byte[500], null, 0, 0,1, 1);
            this.dtMutant.createNode("/zn1/zn2", new byte[500], null, 0, 0,1, 1);
            this.dtMutant.createNode("/zn1/zn2/zn3", new byte[500], null, 1, 0,1, 1);
        }
        if (testType == 7){
            this.dtMutant = new DataTree();
            this.dtMutant.createNode("/zn1", new byte[500], null, 0, 0,1, 1);
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
                {"/zn.1/zn.2"               ,         1 , 1, null                                 },
                {"/zn1/zn2"                 ,         0 , 1, null                                 },
                {"/zn1"                     ,   1000000 , 1, null                                 },
                {"/zn1/zn2"                 ,  -1234567 , 1, null                                 },

                //deleting a NON-existing node
                {"/zn1"                     ,         1 , 2, KeeperException.NoNodeException.class},
                {null                       ,         0 , 2, NullPointerException.class           },
                {""                         ,        -1 , 2, StringIndexOutOfBoundsException.class},

                // deleting checking pzxid
                {"/zn1/zn2/zn3/zn4/zn5"     ,        -1 , 3, null                                 },
                {"/zn.1/zn.2"               ,         1 , 3, null                                 },
                {"/zn1/zn2"                 ,         0 , 3, null                                 },
                {"/zn1"                     ,   1000000 , 3, null                                 },
                {"/zn1/zn2/zn3"             ,  -1234567 , 3, null                                 },

                //deleting no parent
                {"/zn1/zn2/zn3/zn4/zn5/zn6" ,         1 , 4, KeeperException.NoNodeException.class},
                {"/zn1/zn2/zn3"             ,   1000000 , 4, KeeperException.NoNodeException.class},
                {"/zn1/zn2"                 ,         0 , 4, KeeperException.NoNodeException.class},

                // deleting quotas
                {Quotas.procZookeeper+"/parent/"+Quotas.limitNode, 1, 5, null},
                {Quotas.procZookeeper+"/parent/zn1"              , 1, 5, null}

                //killing mutants
//                {"/zn1/zn2/zn3",  1, 6, null},
//                {"/zn1/" + Quotas.limitNode, 1, 7, null},
//                {"/zn1/zn2", 1, 7, null},*/
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
    public void testDeleteCheckingPzxid() {

        if(testType == 3){
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

    @Test
    public void testDeleteNoParent(){
        if(testType == 4){
            if(expectedException != null){
                Assertions.assertThrows(expectedException, () ->{
                    this.dtNoParent.deleteNode(this.path, this.zxid);
                    Assertions.fail();
                });
            }
        }
    }

    @Test
    public void testDeleteQuota() {
        if(testType == 5){
            if(expectedException == null){
                Assertions.assertDoesNotThrow(() -> {
                    this.dtQuotas.createNode(this.path, new byte[500], null, 0, 0,this.zxid, 1);
                    this.dtQuotas.deleteNode(this.path, this.zxid);
                    Assert.assertEquals(0, this.dtQuotas.getNode(Quotas.procZookeeper+"/parent").getChildren().size());
                });
            }
        }
    }

    @Test
    public void testDeleteMutant() {
        if(testType == 6){
            if(expectedException == null){
                Assertions.assertDoesNotThrow(() -> {
                    this.dtMutant.deleteNode(this.path, this.zxid);
                    Assert.assertEquals(0, this.dtMutant.getNode("/zn1/zn2").getChildren().size());
                });
            }
        }
    }

    @Test
    public void testDeleteMutant2() {
        if(testType == 7){
            if(expectedException == null){
                Assertions.assertDoesNotThrow(() -> {
                    this.dtMutant.createNode(this.path, new byte[500], null, 0, 0,this.zxid, 1);
                    this.dtMutant.deleteNode(this.path, this.zxid);
                    Assert.assertEquals(0, this.dtMutant.getNode("/zn1").getChildren().size());
                });
            }
        }
    }


}

