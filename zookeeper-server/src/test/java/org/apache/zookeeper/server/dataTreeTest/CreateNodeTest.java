package org.apache.zookeeper.server.dataTreeTest;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Quotas;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.DataNode;
import org.apache.zookeeper.server.DataTree;
import org.junit.Assert;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.*;


@RunWith(Parameterized.class)
public class CreateNodeTest {
    private String path;
    private byte[] data;
    private List<ACL> acl;
    private long ephemeralOwner;
    private int parentCVersion;
    private long zxid;
    private long time;
    private int nNodes;
    private DataTree dtValidPath;
    private DataTree dtInvalidPath;
    private DataTree dtEphemeralChild;
    private DataTree dtCVersion;
    private DataTree dtEphemeralParent;
    private DataTree dtQuotas;
    private DataTree dtStat;
    String[] pathNodes;
    private int testType;
    private final Class<? extends Exception> expectedException;
    private int nEphemeral;
    private static Random random = new Random(System.currentTimeMillis());
    private enum ObjType {
        NULL, EMPTY, VALID, INVALID
    }

    public CreateNodeTest(String path, ObjType dataType, List<ACL> acl, long ephemeralOwner, int parentCVersion, long zxid, long time, int testType, Class<? extends Exception> expectedException) throws KeeperException.NoNodeException, KeeperException.NodeExistsException {
        this.path = path;
        this.acl = acl;
        this.ephemeralOwner = ephemeralOwner;
        this.parentCVersion = parentCVersion;
        this.zxid = zxid;
        this.time = time;
        this.testType = testType;
        this.expectedException = expectedException;

        switch(dataType) {
            case NULL:
                this.data = null;
                break;
            case EMPTY:
                this.data = new byte[0];
                break;
            case VALID:
                this.data = new byte[500];
                break;
            case INVALID:
                this.data = new byte[100000000];
                random.nextBytes(this.data);
                break;
        }

        // testing only valid path
        if(this.testType == 1){
            this.dtValidPath = new DataTree();

            //Create Tree all not ephemeral nodes
            this.pathNodes = splitPath(path);
            int n = pathNodes.length;
            String currentPath;
            String prevPath = "/";
            String nodeParent = "/";
            int count = 1;

            for (String pathElement : pathNodes) {
                currentPath = prevPath + pathElement;
                if (count == (n)) {
                    break;
                }
                this.dtValidPath.createNode(currentPath, new byte[1000], ZooDefs.Ids.CREATOR_ALL_ACL, 0, dtValidPath.getNode(nodeParent).stat.getCversion(), 0, 1);
                prevPath = currentPath;
                nodeParent = currentPath;
                if (count != 1) {
                    prevPath += "/";
                    count++;
                    continue;
                }
                count++;
            }
            this.nNodes = n;
        }else if(testType == 2){
            // testing invalid paths
            this.dtInvalidPath = new DataTree();
        } else if (testType == 3) {
            // testing number of ephemeral child
            this.dtEphemeralChild = new DataTree();
        } else if(testType == 4){
            // testing CVersion
            this.dtCVersion = new DataTree();
        }else if(testType == 5 || testType == 6){
            // testing ephemeral parent
            this.dtEphemeralParent = new DataTree();
            this.dtEphemeralParent.createNode("/.zn1", new byte[300], ZooDefs.Ids.CREATOR_ALL_ACL, 1, dtEphemeralParent.getNode("/").stat.getCversion(), 1, 1);
        } else if (testType == 7) {
            this.dtQuotas = new DataTree();
            this.dtQuotas.createNode(Quotas.quotaZookeeper+"/parent", new byte[0], null, 0, 0,1, 1);
        }else if (testType == 8) {
            this.dtStat = new DataTree();
            this.dtStat.createNode("/zn1", new byte[0], null, 0, 0,1, 1, new Stat());
        }else if (testType == 9) {
            this.dtQuotas = new DataTree();
            this.dtQuotas.createNode("/parent", new byte[0], null, 0, 0,1, 1);
        }
    }

    private String[] splitPath(String path) {
        String[] pathNodes;
        pathNodes = path.split("/");
        return pathNodes;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() {
        return Arrays.asList(new Object[][]{
                // testing if path is valid
                {"/zn1"                               , ObjType.VALID  , ZooDefs.Ids.CREATOR_ALL_ACL, 0                   ,  3,     1,       1, 1, null},
                //{"/zn2"                             , ObjType.INVALID, ZooDefs.Ids.OPEN_ACL_UNSAFE, 1                   ,  0,     1,       1, 1, IllegalArgumentException.class},
                {"/zn2"                               , ObjType.VALID  , ZooDefs.Ids.OPEN_ACL_UNSAFE, 1                   ,  0,     1,       1, 1, null},
                {"/zn3"                               , ObjType.EMPTY  , ZooDefs.Ids.CREATOR_ALL_ACL, 0                   , -2,     1,       1, 1, null},
                {"/zn4"                               , ObjType.NULL   , ZooDefs.Ids.READ_ACL_UNSAFE, 1                   ,  0,     1,       1, 1, null},
                {"/zn5"                               , ObjType.NULL   , ZooDefs.Ids.READ_ACL_UNSAFE, 1                   , -1,     1,       1, 1, null},
                {"/zn6/zn7"                           , ObjType.VALID  , ZooDefs.Ids.READ_ACL_UNSAFE, 0                   ,  0,     1,       1, 1, null},

                // testing if path is invalid
                {"zn1"                                , ObjType.VALID  , new ArrayList<ACL>()       , 0                   ,  0,      0 ,      0, 2, StringIndexOutOfBoundsException.class},
                {"zn1/zn2"                            , ObjType.VALID  , null                       , 0xff00000000000001L ,  0,     -1 ,      1, 2, KeeperException.NoNodeException.class},
                {"/../zn1"                            , ObjType.NULL   , ZooDefs.Ids.OPEN_ACL_UNSAFE, 0x8000000000000000L ,  0,      1 ,  50000, 2, KeeperException.NoNodeException.class},
                {null                                 , ObjType.VALID  , ZooDefs.Ids.CREATOR_ALL_ACL, -1                  , -1,   5000 ,      1, 2, NullPointerException.class           },
                {""                                   , ObjType.VALID  , ZooDefs.Ids.READ_ACL_UNSAFE, 0                   ,  1,      1 ,     -1, 2, StringIndexOutOfBoundsException.class},
                {"/zn1/zn2"                           , ObjType.VALID  , null                       , 0                   ,  0,      1 ,      1, 2, KeeperException.NoNodeException.class},

                // testing ephemeral child
                {"/zn1"                               , ObjType.VALID  , ZooDefs.Ids.CREATOR_ALL_ACL,  0                   ,  3,     1,       1, 3, null},
                {"/zn2"                               , ObjType.VALID  , ZooDefs.Ids.OPEN_ACL_UNSAFE,  1                   ,  0,     1,       1, 3, null},
                {"/zn3"                               , ObjType.EMPTY  , ZooDefs.Ids.CREATOR_ALL_ACL,  0                   , -2,     1,       1, 3, null},
                {"/zn4"                               , ObjType.NULL   , ZooDefs.Ids.READ_ACL_UNSAFE,  1                   ,  0,     1,       1, 3, null},

                // testing CVersion
                {"/zn1"                               , ObjType.VALID  , ZooDefs.Ids.CREATOR_ALL_ACL, 0                   ,  3,     1,       1, 4, null},
                {"/zn2"                               , ObjType.VALID  , ZooDefs.Ids.OPEN_ACL_UNSAFE, 1                   ,  0,     1,       1, 4, null},
                {"/zn3"                               , ObjType.EMPTY  , ZooDefs.Ids.CREATOR_ALL_ACL, 0                   , -2,     1,       1, 4, null},
                {"/zn4"                               , ObjType.NULL   , ZooDefs.Ids.READ_ACL_UNSAFE, 1                   ,  0,     1,       1, 4, null},

                //{"/.zn1/.zn2"                       , ObjType.VALID  , ZooDefs.Ids.OPEN_ACL_UNSAFE, 0, 0, 1, 1, 5, null},
                //{"/.zn1/.zn2"                       , ObjType.VALID  , ZooDefs.Ids.READ_ACL_UNSAFE, 1, 0, 1, 1, 6, null},

                // increasing coverage
                {"/zn10"                              , ObjType.VALID  , ZooDefs.Ids.OPEN_ACL_UNSAFE, 0                   ,  0,     1,       1, 1, null},
                //TTL node
                {"/zn.1/zn.2"                         , ObjType.VALID  , ZooDefs.Ids.CREATOR_ALL_ACL, 0xff00000000000001L ,  0,     0,       1, 1, null},
                //TTL node
                {"/zn5/zn6/zn7/zn8"                   , ObjType.VALID  , ZooDefs.Ids.READ_ACL_UNSAFE, 0xff00000000000001L ,  0,     0,       1, 1, null},
                //CONTAINER node
                {"/zn5/zn6/zn7/zn8/zn9"               , ObjType.VALID  , ZooDefs.Ids.READ_ACL_UNSAFE, 0x8000000000000000L ,  0,     0,       1, 1, null},

                {"/zn1/zn2", ObjType.VALID, null, 0, 0, 1, 0, 8, null},
                {Quotas.quotaZookeeper+"/parent"+"/"+Quotas.limitNode  , ObjType.VALID, null, 0, 0, 1, 0, 7, null},
                // coverage & kill mutant 504
                {Quotas.quotaZookeeper+"/parent"+"/"+Quotas.statNode   , ObjType.VALID, null, 0, 0, 1, 0, 7, null},
                // kill mutant 502
                {"/parent"+"/"+Quotas.limitNode  , ObjType.VALID, null, 0, 0, 1, 0, 9, null}


        });
    }

    @Test
    public void test() {
        if(testType == 1){
            if(expectedException == null){
                Assertions.assertDoesNotThrow(() -> {
                    this.dtValidPath.createNode(this.path, this.data, this.acl, this.ephemeralOwner, this.parentCVersion, this.zxid, this.time);
                    Assert.assertEquals(this.nNodes, this.dtValidPath.getNodeCount()-4);
                });
            }else{
                Assertions.assertThrows(expectedException, () ->{
                    this.dtValidPath.createNode(this.path, this.data, this.acl, this.ephemeralOwner, this.parentCVersion, this.zxid, this.time);
                    Assertions.fail();
                });
            }
        }else if(testType == 2){
            if(expectedException != null){
                Assertions.assertThrows(expectedException, () ->{
                    this.dtInvalidPath.createNode(this.path, this.data, this.acl, this.ephemeralOwner, this.parentCVersion,this.zxid,this.time);
                    Assertions.fail();
                });
            }
        }
    }

    //increasing coverage
    @Test
    public void testAlreadyExists() {

        if(testType == 1){
            Exception error = null;
            try {
                // dummmy test of duplicate node
                if(this.path.equals("/zn10")){
                    this.dtValidPath.createNode(this.path, this.data, this.acl, this.ephemeralOwner, this.parentCVersion, this.zxid, this.time);
                    this.dtValidPath.createNode(this.path, this.data, this.acl, this.ephemeralOwner, this.parentCVersion, this.zxid, this.time);
                }
            } catch (KeeperException.NodeExistsException | KeeperException.NoNodeException e) {
                error = e;
            }
            if(error != null){
                Assert.assertEquals(KeeperException.NodeExistsException.class, error.getClass());
            }
        }
    }

    @Test
    public void testEphemeral() throws KeeperException.NoNodeException, KeeperException.NodeExistsException {
        if(testType == 3){
            this.dtEphemeralChild.createNode(this.path, this.data, this.acl, this.ephemeralOwner, this.parentCVersion, this.zxid, this.time);
            nEphemeral = this.dtEphemeralChild.getEphemerals().size();
            if(this.ephemeralOwner != 0){
                Assert.assertEquals(1, nEphemeral);
            }else{
                Assert.assertEquals(0, nEphemeral);
            }
        }
    }

    @Test
    public void testCVersion() throws KeeperException.NoNodeException, KeeperException.NodeExistsException {
        if(testType == 4){
            int n;
            DataNode root = this.dtCVersion.getNode("/");
            n = root.stat.getCversion();
            this.dtCVersion.createNode(this.path, this.data, this.acl, this.ephemeralOwner, this.parentCVersion, this.zxid, this.time);
            root = this.dtCVersion.getNode("/");
            if(n <= this.parentCVersion)
                Assert.assertEquals(this.parentCVersion, root.stat.getCversion());
            if(n > this.parentCVersion) {
                Assert.assertNotEquals(this.parentCVersion, root.stat.getCversion());
                Assert.assertEquals(n, root.stat.getCversion());
            }
        }
    }

    @Test
    public void testCreateQuota() {
        if(testType == 7){
            if(expectedException == null){
                Assertions.assertDoesNotThrow(() -> {
                    this.dtQuotas.createNode(this.path, this.data, this.acl, this.ephemeralOwner, this.parentCVersion, this.zxid, this.time);
                    Assert.assertEquals(1, this.dtQuotas.getNode(Quotas.quotaZookeeper+"/parent").getChildren().size());
                });
            }
        }
    }

    @Test
    public void testCreateStat() {
        if(testType == 8){
            if(expectedException == null){
                Assertions.assertDoesNotThrow(() -> {
                    this.dtStat.createNode(this.path, this.data, this.acl, this.ephemeralOwner, this.parentCVersion, this.zxid, this.time);
                    Assert.assertEquals(1, this.dtStat.getNode("/zn1").getChildren().size());
                });
            }
        }
    }

    @Test
    public void testMutations() {
        if(testType == 9){
            if(expectedException == null){
                Assertions.assertDoesNotThrow(() -> {
                    this.dtQuotas.createNode(this.path, this.data, this.acl, this.ephemeralOwner, this.parentCVersion, this.zxid, this.time);
                    Assert.assertEquals(1, this.dtQuotas.getNode("/parent").getChildren().size());
                });
            }
        }
    }


    /*@Test
    public void testEphemeralNode(){
        if(testType == 5){
            if(expectedException == null){
                Assertions.assertDoesNotThrow(() -> {
                    dtEphemeralParent.createNode(this.path, this.data, this.acl, this.ephemeralOwner, this.parentCVersion, this.zxid, this.time);
                    Assert.assertEquals(0, dtEphemeralParent.getNode("/.zn1").getChildren().size());
                });

            }
        }
    }


    @Test
    public void testChildEphemeral(){
        if(testType == 6){
            if(expectedException == null){
                Assertions.assertDoesNotThrow(() -> {
                    dtEphemeralParent.createNode(this.path, this.data, this.acl, this.ephemeralOwner, this.parentCVersion, this.zxid, this.time);
                    Assert.assertEquals(0, dtEphemeralParent.getNode("/.zn1").getChildren().size());
                });

            }
        }
    }*/
}