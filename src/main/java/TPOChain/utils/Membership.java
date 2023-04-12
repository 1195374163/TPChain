package TPOChain.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pt.unl.fct.di.novasys.network.data.Host;

import java.util.*;

public class Membership {

    private static final Logger logger = LogManager.getLogger(Membership.class);
    
    //存放了系统中的节点
    private final List<Host> members;
    private final Map<Host, Integer> indexMap;
    //代表着系统中前链
    private  final  Map<Host,Boolean>  chainNode;
    
    /**
     * 待处理的要删除的节点
     * */
    private final Set<Host> pendingRemoval;
    
    //标记着系统中最小的运行数量，也是系统中要求前链的数量
    //此字段代表着F+1
    private final int MIN_QUORUM_SIZE;


    //对系统中的节点进行初始分配
    public Membership(List<Host> initial, int MIN_QUORUM_SIZE) {
        this.MIN_QUORUM_SIZE = MIN_QUORUM_SIZE;
        members = new ArrayList<>(initial);
        indexMap = new HashMap<>();
        chainNode = new HashMap<>();
        int i=0;
        for (Host temp : initial) {
            if(i<MIN_QUORUM_SIZE){
                chainNode.put(temp,Boolean.TRUE);
            }else{
                chainNode.put(temp,Boolean.FALSE);
            }
            i++;
        }
        pendingRemoval = new HashSet<>();
        //logger.info("New " + this);
        checkSizeAgainstMaxFailures();
    }


    public List<Host> getMembers() {
        return Collections.unmodifiableList(members);
    }


    /**
     * 设置某个节点为前链节点
     * */
    public  void  setFrontedChainNode(Host me){
        chainNode.put(me,Boolean.TRUE);
    }


    /**
     *确定当前节点数小于MIN_QUORUM_SIZE的系统最终节点数 则终止系统
     */
    private void checkSizeAgainstMaxFailures() {
        if (members.size() < MIN_QUORUM_SIZE) {
            logger.error("Not enough nodes to continue. Current nodes: " + members.size() +
                    "; min nodes: " + MIN_QUORUM_SIZE);
            throw new AssertionError("Not enough nodes to continue. Current nodes: " + members.size() +
                    "; min nodes: " + MIN_QUORUM_SIZE);
        }
    }


    /**
     * 检测前段节点是否为QUORUM  F+1
     * */
    public  boolean checkFrntedChainIsQUORUM(){
        int frontedNodeSum=0;
        checkSizeAgainstMaxFailures();
        for (Map.Entry<Host, Boolean> entry : chainNode.entrySet()) {
            Boolean value = entry.getValue();
            if (value.equals(Boolean.TRUE)){
                frontedNodeSum++;
            }
        }
        //TODO 这里应该是相等，前链的节点应该
        if (frontedNodeSum>=MIN_QUORUM_SIZE){
            return true;
        }else{
            return false;
        }
    }

    //TODO  判断是链尾
    /**
     * 判断后链的节点是否是链中的最后的节点
     * */
    public boolean isAfterLeader(Host me, Host leader, Host other) {
        if(!contains(me) || !contains(leader) || !contains(other)){
            logger.error("Membership does not contain: " + me + " " + leader + " " + other + ".." + members);
        }
        assert contains(me) && contains(leader) && contains(other);


        if (me.equals(other)) return true;
        int distLeader = distanceFrom(leader, me);
        int distOther = distanceFrom(other, me);
        if (distLeader == 0) distLeader += members.size();
        return distOther >= distLeader;
    }


    /**
     * 返回当前节点向右的下一个存活节点s
     * */
    public Host nextLivingInChain(Host myHost) {
        assert contains(myHost);
        int nextIndex = (indexOf(myHost) + 1) % members.size();
        Host nextHost = members.get(nextIndex);
        while (pendingRemoval.contains(nextHost)) {
            nextIndex = (nextIndex + 1) % members.size();
            nextHost = members.get(nextIndex);
        }
        return nextHost;
    }
    
    
    //TODO 返回前链的下一个节点
    /**
     * 返回当前节点在前段链的下一个存活节点s
     * */
    public Host nextLivingInFrontedChain(Host myHost) {
        assert contains(myHost);
        int nextIndex = (indexOf(myHost) + 1) % members.size();
        Host nextHost = members.get(nextIndex);
        while (chainNode.get(nextHost).equals(Boolean.FALSE) || pendingRemoval.contains(nextHost)){
            nextIndex = (nextIndex + 1) % members.size();
            nextHost = members.get(nextIndex);
        }
        return nextHost;
    }


    /**
     * 返回当前节点在后段链的下一个存活节点s
     * */
    public Host nextLivingInBackChain(Host myHost) {
        assert contains(myHost);
        int nextIndex = (indexOf(myHost) + 1) % members.size();
        Host nextHost = members.get(nextIndex);
        while (pendingRemoval.contains(nextHost)) {
            nextIndex = (nextIndex + 1) % members.size();
            nextHost = members.get(nextIndex);
        }
        return nextHost;
    }

    /**
     * 返回两个主机在链表中的距离
     * */
    public int distanceFrom(Host current, Host initial) {
        assert contains(current) && contains(initial);

        int currentIndex = indexOf(current);
        int initialIndex = indexOf(initial);
        int dist = currentIndex - initialIndex;
        if (dist < 0) dist += members.size();
        return dist;
    }


    /**
     * 返回节点在链表中的索引
     * */
    public int indexOf(Host host) {
        return indexMap.computeIfAbsent(host, members::indexOf);
    }

    
    /**
     * 根据索引判断某个节点是否在链中
     * */
    public boolean contains(Host host) {
        return indexOf(host) >= 0;
    }

    
    /**
     *判断一个节点是否在前链中
     * */
    //TODO  判断前链
    public  boolean containFrontedChain(Host host){
        return chainNode.get(host);
    }
    
    /**
     * 不需要
     * */
    public Host nodeAt(int pos){
        return members.get(pos);
    }


    public void addMember(Host host, int position) {
        if (contains(host)) {
            logger.error("Trying to add already existing host: " + host);
            throw new AssertionError("Trying to add already existing host: " + host);
        }
        indexMap.clear();
        /*
        *  void add(int index, E element);则可以在插入操作过程中指定插入的位置，
        * 此时，会自动将当前位置及只有的元素后移进行插入，需要注意的是，参数index的值不可大于当前list的容量，
        * 即在使用此方法填充一个list时，必须以0开始依次填充
        * */
        members.add(position, host);
        logger.debug("New " + this);
        checkSizeAgainstMaxFailures();
    }

    /**
     * 彻底删除某个节点
     * */
    public void removeMember(Host host) {
        if (!contains(host)) {
            logger.error("Removing non-existing host: " + host);
            throw new AssertionError("Trying to remove non-existing host: " + host);
        }
        logger.debug("Removing member: " + host);
        indexMap.clear();
        members.remove(host);
        pendingRemoval.remove(host);
        logger.debug("New " + this);
        checkSizeAgainstMaxFailures();
    }


    public int size() {
        return members.size();
    }


    /**
     * 返回的从当前节点(不包括当前节点)到目标节点(包括目标节点)的所有节点的Iterator
     * */
    public Iterator<Host> nextNodesUntil(Host self, Host h){
        //forward()的需要
        int myIdx = indexOf(self);
        int lastIdx = indexOf(h);
        if(lastIdx < 0 || myIdx < 0) {
            logger.error("Called nextNodesUntil with hosts not in membership");
            throw new RuntimeException("Called nextNodesUntil with hosts not in membership");
        }
        int dist = lastIdx - myIdx;
        if (dist < 0) dist += members.size();


        List<Host> res = new ArrayList<>(dist);
        for(int i = 1 ; i <= dist ; i++){
            res.add(members.get((myIdx + i)%members.size()));
        }
        return res.iterator();
    }


    public void addToPendingRemoval(Host affectedHost) {
        boolean add = pendingRemoval.add(affectedHost);
        assert add;
    }

    public void cancelPendingRemoval(Host affectedHost) {
        boolean remove = pendingRemoval.remove(affectedHost);
        assert remove;
    }


    @Override
    public String toString() {
        return "{" +
                "members=" + members +
                '}';
    }


    //进行浅层拷贝
    public List<Host> shallowCopy() {
        return new ArrayList<>(members);
    }

}
