package org.apache.zookeeper.utils;

import java.util.ArrayList;
import java.util.List;

public class Tree {

    public static class Node {
        public String data;
        public Node left;
        public Node right;
    }

    public static List<String> getChildrenNodes(Node root, ArrayList<String> leaves) {

        if (root == null) {
            return leaves;
        }
        if (root.left != null){
            leaves.add(root.left.data);
        }

        if (root.right != null){
            leaves.add(root.right.data);
        }

        return leaves;
    }

    public static Node createNode(String data) {
        Node node = new Node();
        node.data = data;
        node.left = null;
        node.right = null;
        return node;
    }
}

