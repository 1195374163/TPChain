package TPOChain.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pt.unl.fct.di.novasys.network.data.Host;

import java.util.*;

public class Membership {

    private static final Logger logger = LogManager.getLogger(Membership.class);
    
    //这里可以将链分为两个链 
    //节点之间有序 存放了系统中的节点
    //同时附加一个标记hashmap显示这个节点是标记删除的吗
    //总链：前链和后链拼接
    private final List<Host> members;
    //前链
    private final List<Host>  frontChain;
    // 后链
    private final List<Host>  backChain;

    //作为缓存，存放元素的索引位置
    private  final Map<Host, Integer> frontIndexMap;
    private  final Map<Host, Integer> backIndexMap;
    private final Map<Host, Integer> indexMap;
    
    
    //对于要删除的节点，还保留着原来在list中的位置，并且将其纳入pendingRemoval集合
    //在查询得到下一个节点要跳过这个节点
    /**
     * 待处理的要删除的节点
     * */
    private final Set<Host> pendingRemoval;
    
    
    //标记着系统中最小的运行数量，也是系统中要求前链的数量
    //此字段代表着F+1
    private final int MIN_QUORUM_SIZE;

    //TODO join节点使用：复制集群中节点及其状态
    //对系统中的节点进行初始分配
    public Membership(List<Host> initial, int MIN_QUORUM_SIZE) {
        this.MIN_QUORUM_SIZE = MIN_QUORUM_SIZE;
        members = new ArrayList<>(initial);
        frontChain=new ArrayList<Host>();
        backChain=new ArrayList<Host>();
        for (int i = 0; i < initial.size(); i++) {
            if (i<MIN_QUORUM_SIZE){
                frontChain.add(members.get(i));
            }else {
                backChain.add(members.get(i));
            }
        }
        
        indexMap = new HashMap<>();
        frontIndexMap = new HashMap<>();
        backIndexMap= new HashMap<>();
        
        pendingRemoval = new HashSet<>();
        // 检查前链节点是否为F+1，不足结束进程
        checkFrontSizeAgainstQUORUM();
        //logger.info("New " + this);
        checkSizeAgainstMaxFailures();
    }
    
    
    /**
     * 返回节点在链表中的索引
     * */
    public int indexOf(Host host) {
        return indexMap.computeIfAbsent(host, members::indexOf);
    }
    public  int  frontIndexOf(Host host){
        return frontIndexMap.computeIfAbsent(host, frontChain::indexOf);
    }
    public  int   backIndexOf(Host host){
        return backIndexMap.computeIfAbsent(host, backChain::indexOf);
    }
    // 根据索引判断某个节点是否在链中
    public boolean contains(Host host) {
        return indexOf(host) >= 0;
    }
    //根据索引判断某个节点是否在前链中
    public boolean frontChainContain(Host host){
        return frontIndexOf(host) >= 0;
    }
    // 根据索引判断某个节点是否在后链中
    public boolean  backChainContain(Host host){
        return backIndexOf(host) >= 0;
    }
    

    //TODO  检测集群当前存活的节点，
    // 不需要；在链尾检测不满F+1个投票，系统会终止
    // TODO: 2023/5/19 这里真的可行吗？ 特别对于前链节点要有投票F+1
    // TODO: 2023/5/18 是放过对删除节点的刨除还是不减少标记节点的，最后收集的投票数小于F+1，直接程序退出 
    
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
    private void checkFrontSizeAgainstQUORUM(){
        if (frontChain.size()<MIN_QUORUM_SIZE){
            logger.error("Not enough nodes to continue. Current nodes: " + frontChain.size() +
                    "; min nodes: " + MIN_QUORUM_SIZE);
            throw new AssertionError("Not enough nodes to continue. Current nodes: " + frontChain.size() +
                    "; min nodes: " + MIN_QUORUM_SIZE);
        }
    }



    /**
     * 得到后链的首节点
     * */
    public Host getBackChainHead(){
        int backChainSize=backChain.size();
        if (backChainSize==0){
            return null;
        }
        int nextIndex = 0;
        Host nextHost = backChain.get(nextIndex);
        //标记节点中包括此节点 ，下一次循环
        while (pendingRemoval.contains(nextHost)) {
            nextIndex ++;
            if (nextIndex == backChainSize){
                return null;//在整个系统没有后链时，返回null
            }
            nextHost = backChain.get(nextIndex);
        }
        return nextHost;
    }

    /**
     * 得到后链的尾节点
     * */
    public Host getBackChainTail(){
        int backChainSize=backChain.size();
        if (backChainSize==0){
            return null;
        }
        int nextIndex = backChainSize-1;
        Host nextHost = backChain.get(nextIndex);
        //标记节点中包括此节点 ，下一次循环
        while (pendingRemoval.contains(nextHost)) {
            nextIndex --;
            if (nextIndex < 0){
                return null;//在整个系统没有后链时，返回null
            }
            nextHost = backChain.get(nextIndex);
        }
        return nextHost;
    }
    
    
    //后链的下一个节点为空
    /**
     * 返回当前节点在前段链的下一个存活节点s
     * */
    public Host nextLivingInFrontedChain(Host myHost) {
        assert contains(myHost);
        // 当前节点为后链节点时返回null
        if (backChainContain(myHost)){
            return null;
        }
        
        int nextIndex = (frontIndexOf(myHost) + 1) % frontChain.size();
        Host nextHost = frontChain.get(nextIndex);
        //标记节点中包括此节点 ，下一次循环
        while (pendingRemoval.contains(nextHost)){
            nextIndex = (nextIndex + 1) % frontChain.size();
            nextHost = frontChain.get(nextIndex);
        }
        return nextHost;
    }
    
    //若是前链节点的话，它的后链节点就是后链首节点
    //TODO  使用这个方法时，应该进行判定是否返回值为null，为null，标志着此节点是链尾
    // 因为这个是前链，那么在收到它的逻辑链的链头消息，对其进行decided的时候
    // 需要向链头发送ack信息
    // 在整个系统只有前链节点时，没有后链时，，即系统正好F+1各前链节点
    // 每进行节点的删除处理应该整个系统存活的节点是否满足F+1 
    /**
     * 返回当前节点在后段链的下一个存活节点s
     * */
    public Host nextLivingInBackChain(Host myHost) {
        assert contains(myHost);
        
        //特殊判定：如果是前链节点，后链的首节点一定是它的nextBackChainNode
        if (frontChainContain(myHost)){
            Host temp=getBackChainHead();
            return temp;//这个可能返回null 意味着没有后链节点了
        }
        //应该在链尾节点终止
        //后面是后链节点的流程
        Host tail=getBackChainTail();
        if (myHost.equals(tail)){//后链链尾的下一个节点是null;
            return null;
        }
        
        
        int nextIndex = (backIndexOf(myHost) + 1) % backChain.size();
        Host nextHost = backChain.get(nextIndex);
        //标记节点中包括此节点 ，下一次循环
        while (pendingRemoval.contains(nextHost)) {
            nextIndex = (nextIndex + 1) % backChain.size();
            nextHost = backChain.get(nextIndex);
        }
        return nextHost;
    }

    
    
    //TODO  判断是否是逻辑链的尾部， 
    // 针对前链和 后链节点 不同的判定标准
    //因为删除前链节点，会导入后链首节点成为替代原来位置的前链节点
    //public boolean isAfterLeader(Host me, Host leader, Host other) {
    //    if(!contains(me) || !contains(leader) || !contains(other)){
    //        logger.error("Membership does not contain: " + me + " " + leader + " " + other + ".." + members);
    //    }
    //    assert contains(me) && contains(leader) && contains(other);
    //    
    //    //if (membership.isAfterLeader(self, inst.highestAccept.getNode(), target)
    //    //上面是这个函数的调用参数
    //    if (frontedChainNode.get(me).equals(Boolean.TRUE)){//说明为前链
    //        //前链只有在没有后链节点时才为链尾
    //        if (getBackChainHead()==null) {
    //            //只有消息的leader是当前节点的前链的前继，才能说明是链尾
    //            
    //        } else {//说明还有后链节点
    //            return  false;
    //        }
    //    }else{
    //        //说明为后链
    //        Host tail=getBackChainTail();
    //        if (tail.equals(me)){
    //            return true;
    //        }else {
    //            return false;
    //        }
    //    }
    //    return  false;
    //}
    
    
    ////TODO 需要修改 因为前链和后链
    ////通用 ：对前链节点和后链:根据目标Host的类型，也返回中间的一些节点
    ////哪怕中间有节点属于pendingRemove，也会向其发送消息
    //
    ///**
    // * 返回的从当前节点(不包括当前节点)到目标节点(包括目标节点)的所有节点的Iterator
    // * */
    //public Iterator<Host> nextNodesUntil(Host self, Host h){
    //    //forward()的需要
    //    int myIdx = indexOf(self);
    //    int lastIdx = indexOf(h);
    //    if(lastIdx < 0 || myIdx < 0) {
    //        logger.error("Called nextNodesUntil with hosts not in membership");
    //        throw new RuntimeException("Called nextNodesUntil with hosts not in membership");
    //    }
    //    int dist = lastIdx - myIdx;
    //    if (dist < 0) dist += members.size();
    //
    //    List<Host> res = new ArrayList<>(dist);
    //    //暂存  目标节点的前链后链标志位
    //    Boolean tempboool=frontedChainNode.get(h);
    //    for(int i = 1 ; i <= dist ; i++){
    //        Host temp=members.get((myIdx + i)%members.size());
    //        //当和目标节点同是前链或后链的时候才进行添加
    //        if (frontedChainNode.get(temp).equals(tempboool)){
    //            res.add(temp);
    //        }
    //    }
    //    return res.iterator();
    //}
    //
    
    /**
     * 返回当前节点若是前链节点的逻辑前链的末尾节点
     * */
    //public Host frontChainLogicTail(Host myHost) {
    //    assert contains(myHost);
    //    //得到member列表的size
    //    int  membersize=members.size();
    //    
    //    int priorIndex = ((indexOf(myHost) - 1) + membersize) % membersize;
    //    Host priorHost = members.get(priorIndex);
    //    while (pendingRemoval.contains(priorHost) || frontedChainNode.get(priorHost).equals(Boolean.FALSE)) {
    //        priorIndex = ((priorIndex - 1) + membersize) % membersize;
    //        priorHost = members.get(priorIndex);
    //    }
    //    return priorHost;
    //}
    
    

    
    //这两个函数在节点增删时使用
    /**
     * 在添加节点时用
     * */
    public void addMember(Host host) {
        if (contains(host)) {
            logger.error("Trying to add already existing host: " + host);
            throw new AssertionError("Trying to add already existing host: " + host);
        }
        indexMap.clear();
        frontIndexMap.clear();
        backIndexMap.clear();
        /*
        *  void add(int index, E element);则可以在插入操作过程中指定插入的位置，
        * 此时，会自动将当前位置及只有的元素后移进行插入，需要注意的是，参数index的值不可大于当前list的容量，
        * 即在使用此方法填充一个list时，必须以0开始依次填充
        * */
        backChain.add(host);//节点添加都是后链末尾添加
        
        members.clear();
        members.addAll(frontChain);
        members.addAll(backChain);
        
        logger.debug("New " + this);
        checkFrontSizeAgainstQUORUM();
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

        members.remove(host);
        if (frontChainContain(host)){
            frontChain.remove(host);
        }
        if (backChainContain(host)){
            backChain.remove(host);
        }
        pendingRemoval.remove(host);
        
        indexMap.clear();
        frontIndexMap.clear();
        backIndexMap.clear();
        
        logger.debug("New " + this);
        checkFrontSizeAgainstQUORUM();
        checkSizeAgainstMaxFailures();
    }
    
    
    //在对于新加入节点时，可能需要这些操作
    //在接收一些删除节点操作的实例时，先进行可能移除
    //在删除操作时，先进行标记，后进行删除
    //若删除的是前链节点，需要将后链首节点与要删除的节点互换位置
    public void addToPendingRemoval(Host affectedHost) {
        //// 被删除节点是前链的话
        //if(frontChainContain(affectedHost)){
        //    Host backChainHead =getBackChainHead();
        //    if (backChainHead==null){// 没有节点备选作为前链节点，
        //        logger.error("Not enough nodes to continue. Current nodes: " + members.size() +
        //                "; 同时没有给前链节点补充了;min nodes: " + MIN_QUORUM_SIZE);
        //        throw new AssertionError("Not enough nodes to continue. Current nodes: " + members.size() +
        //                "; min nodes: " + MIN_QUORUM_SIZE);
        //    }
        //    
        //    int indexaffectedHost=frontIndexOf(affectedHost);
        //    frontChain.add(indexaffectedHost,backChainHead);
        //    backChain.remove(backChainHead);
        //    
        //    indexMap.clear();//索引的缓存清空
        //    frontIndexMap.clear();
        //    backIndexMap.clear();
        //}
        //不管是前链还是后链，都要将受影响节点加入 待移除列表中
        boolean add = pendingRemoval.add(affectedHost);
        assert add;
        if (frontChainContain(affectedHost)){//如果当前节点在前链，移至后链
            frontChain.remove(affectedHost);
            backChain.add(affectedHost);
        }
    }

    public void cancelPendingRemoval(Host affectedHost) {
        boolean remove = pendingRemoval.remove(affectedHost);
        assert remove;
    }

    
    // TODO: 2023/5/22 将后链节点附加在对应的前链节点
    public Host  appendFrontChainNode(Host self,Host leader){
        if (frontChainContain(self)){// 如果当前节点是前链的话，不需要附加
            return null;
        }
        // 将后链节点节点附加在leader后面节点的对应位置
        int  offset=(frontIndexOf(leader)+backIndexOf(self)+1)% frontChain.size();
        return frontChain.get(offset);// 因为前链是F+1个节点，后链是F个节点。跳过leader
    }
    
    
    
    /**
     * 基本无用：只是保存 返回两个主机在链表中的距离:在leader的超时时钟中使用
     * */
    public int distanceFrom(Host current, Host initial) {
        assert contains(current) && contains(initial);

        int currentIndex = indexOf(current);
        int initialIndex = indexOf(initial);
        int dist = currentIndex - initialIndex;
        if (dist < 0) dist += members.size();
        return dist;
    }
    
    
    public Host nodeAt(int pos){
        return members.get(pos);
    }
    
    
    //有使用 ：在对消息进行群发时需要调用这个
    //在tryTakeLeadership()中发送prepareMsg给其他节点
    //可能返回断开的节点
    public List<Host> getMembers() {
        return Collections.unmodifiableList(members);
    }

    
    //返回系统中节点的数量 在在tryTakeLeadership()方法中使用
    public int size() {
        return members.size();
    }
    
    
    @Override
    public String toString() {
        return "{" +
                "members=" + members +
                '}';
    }
    
    
    //新加入节点要复制对集群中的状态复制：成员列表
    //这里不需要改变，因为调用这个方法的是刚加入节点向全体广播 joinsuccessMsg,用这个方法
    //得到全体成员
    public List<Host> shallowCopy() {
        List<Host>  temp= new ArrayList<>();
        temp.addAll(frontChain);
        temp.addAll(backChain);
        return temp;
    }
    
    public  Set<Host> copyPendingRemoval(){
        return new HashSet<>(pendingRemoval);
    }
    
}
