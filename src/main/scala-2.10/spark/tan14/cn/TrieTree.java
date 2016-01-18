package spark.tan14.cn;

/**
 * Created by xie on 15-10-31.
 */
/*
 * Trie树，用于存储、检索ip地址
 * 叶子节点标记为ip地址对应的ISP
 */
public class TrieTree {
    private Node root = null;   //根节点

    /*二叉树的节点*/
    private static class Node {
        String element;  //非叶子节点为空 叶子节点标记为ISP
        Node[] children; //左孩子节点为0 右孩子节点为1

        public Node() {
            element = "";
            children = new Node[2];
            for (int i = 0; i < children.length; i++) {
                children[i] = null;
            }
        }
    }

    public TrieTree() {
        root = new Node();
    }

    /*插入ip地址*/
    public void insert(Node root, String ipAddress, String isp) {
        if(ipAddress.length() > 32) {
            System.out.println("ip地址处理错误");
        } else {
            Node crawl = root;
            for(int i=0; i<ipAddress.length(); i++) {
                int index = (int) ipAddress.charAt(i) - '0';
                if(crawl.children[index] == null) {
                    crawl.children[index] = new Node();
                }
                crawl = crawl.children[index];
            }
            crawl.element = isp;
        }
    }

    public void insert(String ipAddress, String isp) {
        insert(root, ipAddress, isp);
    }

    /*
     * 检索ip地址，返回其所对应的ISP
     * 若不在Trie树中，则返回null
     * */
    public String search(String binaryIP) {
        Node crawl = root;
        for(int i = 0; crawl.element.length() == 0; i++) {
            int index = (int) binaryIP.charAt(i) - '0';
            if(crawl.children[index] == null) {
                return null;
            }
            crawl = crawl.children[index];
        }
        return crawl.element;
    }
}
