package TPOChain.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pt.unl.fct.di.novasys.network.data.Host;

import java.util.*;

public class Membership {

    private static final Logger logger = LogManager.getLogger(Membership.class);
    
    //这里可以将链分为两个链 
    //节点之间有序 存放了系统中的节点
    //同时附加一个标记hashset显示这个节点是标记删除的吗
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
    
    
    //对于要删除的节点，如果是后链，还保留着原来在list中的位置
    //如果是前链移至后链链尾，并且将其纳入pendingRemoval集合在查询得到下一个节点要跳过这个节点
    /**
     * 待处理的要删除的节点
     * */
    private final Set<Host> pendingRemoval;
    
    //标记着系统中最小的运行数量，也是系统中要求前链的数量此字段代表着F+1
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

    
    /** 判断一个节点是否在集群里
     * */
    public boolean isAlive(Host host) {
        if(indexOf(host) >= 0){
            if (pendingRemoval.contains(host)){
                return false;
            }else {
                return true;
            }
        }else {//说明节点不存在
            return  false;
        }
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
    返回前链的链尾是从leader作为链头开始的，那么leader的前一个节点就是前链链尾 
    */
    public Host  getFrontChainTail(Host leader){
        int leaderIndex=frontIndexOf(leader);
        if (leaderIndex<0){// 如果leader不存在
            return null;
        }
        int frontChainIndex=((leaderIndex-1)+frontChain.size())%frontChain.size();
        return frontChain.get(frontChainIndex);
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
        // 当前节点为后链节点时返回null
        if (backChainContain(myHost)){
            return null;
        }
        
        int nextIndex = (frontIndexOf(myHost) + 1) % frontChain.size();
        Host nextHost = frontChain.get(nextIndex);
        //前链节点是一个循环链表 标记节点中包括此节点 ，下一次循环 
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
        //特殊判定：如果是前链节点，后链的首节点一定是它的nextBackChainNode
        if (frontChainContain(myHost)){
            Host temp=getBackChainHead();
            return temp;//这个可能返回null 意味着没有后链节点了
        }
        
        //应该在链尾节点终止,后面是后链节点的流程
        Host tail=getBackChainTail();
        if (myHost.equals(tail)){//后链链尾的下一个节点是null;
            return null;
        }
        // 如果当前节点是后链物理存在的最后一个节点，返回null
        if (backIndexOf(myHost)==backChain.size()-1){
            return  null;
        }
        
        // 后链非链尾节点
        int nextIndex = backIndexOf(myHost) + 1;
        Host nextHost = backChain.get(nextIndex);
        //标记节点中包括此节点 ，下一次循环
        while (pendingRemoval.contains(nextHost)) {
            nextIndex = nextIndex + 1;
            if (nextIndex>=backChain.size()){
                return null;
            }
            nextHost = backChain.get(nextIndex);
        }
        return nextHost;
    }
    
    
    

    
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

        members.clear();
        members.addAll(frontChain);
        members.addAll(backChain);
        
        logger.debug("New " + this);
        checkFrontSizeAgainstQUORUM();
        checkSizeAgainstMaxFailures();
    }
    
    
    
    //在对于新加入节点时，可能需要这些操作
    //在接收一些删除节点操作的实例时，先进行可能移除
    //在删除操作时，先进行标记，后进行删除
    //若删除的是前链节点，需要将后链首节点与要删除的节点互换位置
    public void addToPendingRemoval(Host affectedHost) {
        //不管是前链还是后链，都要将受影响节点加入 待移除列表中
        boolean add = pendingRemoval.add(affectedHost);
        assert add;
        if (frontChainContain(affectedHost)){//如果当前节点在前链，移至后链
            int removePosition= frontIndexMap.get(affectedHost);
            Host  head=getBackChainHead();
            if (head==null){//说明没有补充的节点
                logger.error("Not enough nodes to continue. Current nodes: " + frontChain.size() +
                        "; min nodes: " + MIN_QUORUM_SIZE);
                throw new AssertionError("Not enough nodes to continue. Current nodes: " + frontChain.size() +
                        "; min nodes: " + MIN_QUORUM_SIZE);
            }
            //用原后链链首替换要删除的元素 
            frontChain.set(removePosition, head);
            backChain.remove(head);//后链移除原链首
            
            //将要删除节点添加到后链末尾
            backChain.add(affectedHost);
            
            // 对索引进行清理
            indexMap.clear();
            frontIndexMap.clear();
            backIndexMap.clear();
            // 对members进行更新
            members.clear();
            members.addAll(frontChain);
            members.addAll(backChain);
        }else {
            //删除原位置
            backChain.remove(affectedHost);
            //将待移除节点放入后链末尾
            backChain.add(affectedHost);
            // 对索引进行清理
            indexMap.clear();
            frontIndexMap.clear();
            backIndexMap.clear();
            // 对members进行更新
            members.clear();
            members.addAll(frontChain);
            members.addAll(backChain);
        }
    }
    
    public void cancelPendingRemoval(Host affectedHost) {
        boolean remove = pendingRemoval.remove(affectedHost);
        assert remove;
    }

    
    
    // 将后链节点附加在对应的前链节点，是将后链Front层收到的消息发给前链节点来处理
    public Host  appendFrontChainNode(Host self,Host leader){
        if (frontChainContain(self)){// 如果当前节点是前链的话，不需要附加
            return null;
        }
        if (leader==null){//
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
    //在发送选举成功之后的可能返回断开的节点
    public List<Host> getMembers() {
        return Collections.unmodifiableList(members);
    }

    
    
    //返回系统中节点的数量 在在tryTakeLeadership()方法中使用
    public int size() {
        return members.size();
    }
    
    
    // 返回前链
    public List<Host> getFrontChain(){
        return new ArrayList<>(frontChain);
    }
    // 返回后链中不是被标记删除的节点
    public  List<Host>  getBackChain(){
        List<Host>  tmp=new ArrayList<>();
        for (Host em:backChain){
            if (!pendingRemoval.contains(em)){
                tmp.add(em);
            }
        }
        return  tmp;
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
