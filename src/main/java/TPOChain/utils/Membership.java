package TPOChain.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pt.unl.fct.di.novasys.network.data.Host;

import java.util.*;

public class Membership {

    private static final Logger logger = LogManager.getLogger(Membership.class);
    
    //有序
    //存放了系统中的节点
    //以这个为主，附加一个hashmap，显示这个节点是前链还是后链
    private final List<Host> members;
    //作为缓存，存放元素的索引位置
    private final Map<Host, Integer> indexMap;
    
    
    //所有节点在前链的标识，true标识前链，false标识后链
    private  final  Map<Host,Boolean> frontedChainNode;
    
    //对于要删除的节点，还保留着原来在list中的位置，并且将其纳入pendingRemoval集合
    //在查询下一个节点要跳过这个节点
    /**
     * 待处理的要删除的节点
     * */
    private final Set<Host> pendingRemoval;
    
    //标记着系统中最小的运行数量，
    //也是系统中要求前链的数量
    //此字段代表着F+1
    private final int MIN_QUORUM_SIZE;


    //对系统中的节点进行初始分配
    public Membership(List<Host> initial, int MIN_QUORUM_SIZE) {
        this.MIN_QUORUM_SIZE = MIN_QUORUM_SIZE;
        members = new ArrayList<>(initial);
        indexMap = new HashMap<>();
        
        frontedChainNode = new HashMap<>();
        int i=0;
        for (Host temp : initial) {
            if(i<MIN_QUORUM_SIZE){
                frontedChainNode.put(temp,Boolean.TRUE);
            }else{
                frontedChainNode.put(temp,Boolean.FALSE);
            }
            i++;
        }
        
        //输出前链节点
        for (HashMap.Entry<Host, Boolean> entry : frontedChainNode.entrySet()) {
            Host key = entry.getKey();
            Boolean value = entry.getValue();
            if (value.equals(Boolean.TRUE)){
                logger.info("前链节点 " + key);
            }
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
        frontedChainNode.put(me,Boolean.TRUE);
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
    public  boolean checkFrontedChainIsQUORUM(){
        int frontedNodeSum=0;
        checkSizeAgainstMaxFailures();
        for (Map.Entry<Host, Boolean> entry : frontedChainNode.entrySet()) {
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

    
    //TODO  排序应该先前链 ，后后链
    /**
     * 返回当前节点向右的下一个存活节点s 主要排序过程需要
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
    
    
    //分发过程需要这些字段
    //TODO 返回前链的下一个节点
    /**
     * 返回当前节点在前段链的下一个存活节点s
     * */
    public Host nextLivingInFrontedChain(Host myHost) {
        assert contains(myHost);
        int nextIndex = (indexOf(myHost) + 1) % members.size();
        Host nextHost = members.get(nextIndex);
        while (frontedChainNode.get(nextHost).equals(Boolean.FALSE) || pendingRemoval.contains(nextHost)){
            nextIndex = (nextIndex + 1) % members.size();
            nextHost = members.get(nextIndex);
        }
        return nextHost;
    }

    /**
     * 返回前链中是否存在指定节点的boolean值
     * **/
    public boolean frontcontains(Host host) {
        return indexOf(host) >= 0;
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
        return frontedChainNode.get(host);
    }
    
    /**
     * 不需要
     * */
    public Host nodeAt(int pos){
        return members.get(pos);
    }

    
    
    
    //这两个
    
    /**
     * 在添加节点时用
     * */
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


    
    
    
    //在删除操作时，先进行标记，后进行删除
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
    //这里不需要改变，因为调用这个方法的是刚加入节点向全体广播 joinsuccessMsg
    public List<Host> shallowCopy() {
        return new ArrayList<>(members);
    }
}
