package TPOChain.utils;

import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pt.unl.fct.di.novasys.network.data.Host;

import java.util.*;

public class Membership {

    private static final Logger logger = LogManager.getLogger(Membership.class);
    
    
    //节点之间有序 存放了系统中的节点
    //以这个为主，附加一个hashmap，显示这个节点是前链还是后链，
    //同时附加一个标记hashmap显示这个节点是标记删除的吗
    private final List<Host> members;
    
    
    
    //作为缓存，存放元素的索引位置
    private final Map<Host, Integer> indexMap;
    
    
    //所有节点在前链的标识，true标识前链，false标识后链
    private  final   Map<Host,Boolean> frontedChainNode;
    
    //对于要删除的节点，还保留着原来在list中的位置，并且将其纳入pendingRemoval集合
    //在查询得到下一个节点要跳过这个节点
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
        // 初始时，对前链节点进行初始化，以顺序作为默认条件，前F+1个节点为
        // 前链节点
        frontedChainNode = new HashMap<>();
        int i=0;
        for (Host temp : initial) {
            if(i<MIN_QUORUM_SIZE){//为前链
                //frontedChainNode.put(temp,Boolean.TRUE); 
                //上面这句代码等价于下面这句代码
                setFrontChainNode(temp);
            }else{//为后链
                //frontedChainNode.put(temp,Boolean.FALSE);
                //上面这句代码等价于下面这句代码
                cancelFrontChainNode(temp);
            }
            i++;
        }
        
        //调试输出前链节点
        List<Host> frontChainNode=new ArrayList();
        for (HashMap.Entry<Host, Boolean> entry : frontedChainNode.entrySet()) {
            Host key = entry.getKey();
            Boolean value = entry.getValue();
            if (value.equals(Boolean.TRUE)){
                frontChainNode.add(key);
            }
        }
        logger.warn("前链节点 " + frontChainNode.toString());
        
        
        pendingRemoval = new HashSet<>();
        //logger.info("New " + this);
        checkSizeAgainstMaxFailures();
    }
    
    //TODO 新加入节点根据拿来的消息进行初始话自己的集群列表
    //新加入节点根据状态进行对集群全部节点以及节点的状态的恢复
    public  Membership(Pair<List<Host>, Map<Host,Boolean>> mem,int MIN_QUORUM_SIZE){
        this.MIN_QUORUM_SIZE = MIN_QUORUM_SIZE;
        members = new ArrayList<>(mem.getLeft());
        indexMap = new HashMap<>();

        frontedChainNode = mem.getRight();
        
        //调试输出前链节点
        List<Host> frontChainNode=new ArrayList();
        for (HashMap.Entry<Host, Boolean> entry : frontedChainNode.entrySet()) {
            Host key = entry.getKey();
            Boolean value = entry.getValue();
            if (value.equals(Boolean.TRUE)){
                frontChainNode.add(key);
            }
        }
        logger.warn("前链节点 " + frontChainNode.toString());


        pendingRemoval = new HashSet<>();
        //logger.info("New " + this);
        checkSizeAgainstMaxFailures();
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
        }else{//前链数少于F+1
            return false;
        }
    }
    
    
    

    //后链的下一个节点为空
    
    /**
     * 返回当前节点在前段链的下一个存活节点s
     * */
    public Host nextLivingInFrontedChain(Host myHost) {
        assert contains(myHost);
        
        //当 当前节点为后链节点时返回null
        if (frontedChainNode.get(myHost).equals(Boolean.FALSE)){
            return null;
        }
        
        int nextIndex = (indexOf(myHost) + 1) % members.size();
        Host nextHost = members.get(nextIndex);
        //当是 后链 或者  标记节点中包括此节点 ，下一次循环
        while (frontedChainNode.get(nextHost).equals(Boolean.FALSE) || pendingRemoval.contains(nextHost)){
            nextIndex = (nextIndex + 1) % members.size();
            nextHost = members.get(nextIndex);
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
        if (frontedChainNode.get(myHost).equals(Boolean.TRUE)){
            Host temp=getBackChainHead();
            return temp;
        }
        
        //后面是后链节点的流程
        int nextIndex = (indexOf(myHost) + 1) % members.size();
        Host nextHost = members.get(nextIndex);
        //当是 前链 或者  标记节点中包括此节点 ，下一次循环
        while (frontedChainNode.get(nextHost).equals(Boolean.TRUE) || pendingRemoval.contains(nextHost)) {
            nextIndex = (nextIndex + 1) % members.size();
            nextHost = members.get(nextIndex);
        }
        return nextHost;
    }

    //TODO  判断是否是逻辑链的尾部， 
    // 针对前链和 后链节点 不同的判定标准
    
    //因为删除前链节点，会导入后链首节点成为替代原来位置的前链节点
    public boolean isAfterLeader(Host me, Host leader, Host other) {
        if(!contains(me) || !contains(leader) || !contains(other)){
            logger.error("Membership does not contain: " + me + " " + leader + " " + other + ".." + members);
        }
        assert contains(me) && contains(leader) && contains(other);
        
        //if (membership.isAfterLeader(self, inst.highestAccept.getNode(), target)
        //上面是这个函数的调用参数
        if (frontedChainNode.get(me).equals(Boolean.TRUE)){//说明为前链
            //前链只有在没有后链节点时才为链尾
            if (getBackChainHead()==null) {
                //只有消息的leader是当前节点的前链的前继，才能说明是链尾
                
            } else {//说明还有后链节点
                return  false;
            }
        }else{
            //说明为后链
            Host tail=getBackChainTail();
            if (tail.equals(me)){
                return true;
            }else {
                return false;
            }
        }
        return  false;
    }
    //TODO 需要修改 因为前链和后链
    //通用 ：对前链节点和后链:根据目标Host的类型，也返回中间的一些节点
    //哪怕中间有节点属于pendingRemove，也会向其发送消息
    
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
        //暂存  目标节点的前链后链标志位
        Boolean tempboool=frontedChainNode.get(h);
        for(int i = 1 ; i <= dist ; i++){
            Host temp=members.get((myIdx + i)%members.size());
            //当和目标节点同是前链或后链的时候才进行添加
            if (frontedChainNode.get(temp).equals(tempboool)){
                res.add(temp);
            }
        }
        return res.iterator();
    }


    /**
     * 得到后链的首节点
     * */
    public Host getBackChainHead(){
        for (Host temp:members) {
            if (frontedChainNode.get(temp).equals(Boolean.FALSE) && !pendingRemoval.contains(temp)){
                return  temp;
            }
        }
        return null;//在整个系统没有后链时，返回null
    }
    
    /**
     * 得到后链的尾节点
     * */
    public Host getBackChainTail(){
        for (int i = members.size() - 1; i >= 0; i--) {
            Host temp=members.get(i);
            if (frontedChainNode.get(temp).equals(Boolean.FALSE) && !pendingRemoval.contains(temp))
                return temp;
        }
        return null;//不应该到这
    }
    
    
    /**
     * 返回当前节点若是前链节点的逻辑前链的末尾节点
     * */
    public Host frontChainLogicTail(Host myHost) {
        assert contains(myHost);
        //得到member列表的size
        int  membersize=members.size();
        
        int priorIndex = ((indexOf(myHost) - 1) + membersize) % membersize;
        Host priorHost = members.get(priorIndex);
        while (pendingRemoval.contains(priorHost) || frontedChainNode.get(priorHost).equals(Boolean.FALSE)) {
            priorIndex = ((priorIndex - 1) + membersize) % membersize;
            priorHost = members.get(priorIndex);
        }
        return priorHost;
    }
    
    
    /**
     * 判断一个节点是否为前链节点
     * */
    public Boolean  isFrontChainNode(Host temp){
        return frontedChainNode.get(temp);
    }


    //应该在前链节点被标记的时候
    /**
     * 设置某个节点为前链节点
     * */
    public  void  setFrontChainNode(Host me){
        frontedChainNode.put(me,Boolean.TRUE);
    }

    /**
     * 取消某个节点前链节点标志位即设置它为后链节点
     * */
    public void cancelFrontChainNode(Host me){
        frontedChainNode.put(me,Boolean.FALSE);
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
    
    
    
    
    //这两个函数在节点增删时使用
    //TODO 添加节点的位置
    //调用时position的位置设为后链链尾的位置
    /**
     * 在添加节点时用,
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
        //节点添加都是后链末尾添加
        frontedChainNode.put(host,Boolean.FALSE);
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
        frontedChainNode.remove(host);
        
        logger.debug("New " + this);
        checkSizeAgainstMaxFailures();
    }
    
    
    //TODO  这里还需要重新考虑：
    // 将void 改成Host，返回受影响的节点，对其进行前链节点进行frontChainNodeAction()方法
    
    
    //在对于新加入节点时，可能需要这些操作
    //在接收一些删除节点操作的实例时，先进行可能移除
    //在删除操作时，先进行标记，后进行删除
    //若删除的是前链节点，需要将后链首节点与要删除的节点互换位置
    public void addToPendingRemoval(Host affectedHost) {
        if(frontedChainNode.get(affectedHost).equals(Boolean.TRUE)){
            Host backChainHead =getBackChainHead();
            int indexbackChainHead=indexOf(backChainHead);
            int indexaffectedHost=indexOf(affectedHost);
            
            //对两个节点的前链的标志位进行更改 
            frontedChainNode.put(affectedHost,Boolean.FALSE);
            frontedChainNode.put(backChainHead,Boolean.TRUE);
            
            
            //交换位置
            Collections.swap(members, indexbackChainHead, indexaffectedHost);
            
            indexMap.clear();//索引的缓存清空
        }else{//删除的是后链节点
            //不处理
        }
        
        //不管是前链还是后链，都要将
        boolean add = pendingRemoval.add(affectedHost);
        assert add;
    }

    public void cancelPendingRemoval(Host affectedHost) {
        boolean remove = pendingRemoval.remove(affectedHost);
        assert remove;
    }

    
    //TODO 废弃
    /**
     * 返回竞选者和旧leader在链表中的距离
     * */
    public int distanceFrontFrom(Host current, Host initial) {
        assert contains(current) && contains(initial);
        
        //调用参数
        //membership.distanceFrontFrom(self, supportedLeader()) <= QUORUM_SIZE/2 +1
        //TODO 生成一个前链节点的备份List
        List<Host> frontChain=new ArrayList<>();
        for (Host temp: members) {
            if (frontedChainNode.get(temp).equals(Boolean.TRUE)){
                frontChain.add(temp);
            }
        }
        //TODO  在新leader故障后，与新leader有过交换位置。
        int currentIndex =frontChain.indexOf(current); 
        int initialIndex =frontChain.indexOf(initial);  
        int dist = currentIndex - initialIndex;
        if (dist < 0) dist += frontChain.size();
        return dist;
    }
    
    
    /**
     * 基本无用：只是保存 返回两个主机在链表中的距离
     * */
    public int distanceFrom(Host current, Host initial) {
        assert contains(current) && contains(initial);

        int currentIndex = indexOf(current);
        int initialIndex = indexOf(initial);
        int dist = currentIndex - initialIndex;
        if (dist < 0) dist += members.size();
        return dist;
    }
    





    //TODO 废弃  排序应该先前链 ，后后链
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
    
    
    // 在请求状态时向后链节点请求
    public  List <Host> copyBackChain(Host leader){
        //如果有后链的话
        //没有后链，则返回leader逻辑链的尾节点
        List <Host>  result=new ArrayList<>();     
        if (getBackChainHead()==null){//没有后链
            result.add(frontChainLogicTail(leader));
        }else {//有后链
            for (Host temp:members) {
                if (frontedChainNode.get(temp).equals(Boolean.FALSE) && !pendingRemoval.contains(temp)){
                    result.add(temp);
                }
            }
        }
        return result;
    }

    //这里不需要改变，因为调用这个方法的是刚加入节点向全体广播 joinsuccessMsg,用这个方法
    //得到全体成员
    public List<Host> shallowCopy() {
        return new ArrayList<>(members);
    }
    
    public Map<Host,Boolean>   copyFrontedChainNode(){
        return new HashMap<>(frontedChainNode);
    }
    
    public  Set<Host> copyPendingRemoval(){
        return new HashSet<>(pendingRemoval);
    }
    
    
    //TODO 对于要删除节点是否也要复制
    //对新加入节点的对集群的节点信息以及前链节点进行拷贝
    public Pair<List<Host>,Map<Host,Boolean>> deepCopy(){
        return new MutablePair<>(new ArrayList<>(members),new HashMap<>(frontedChainNode));
    }
    
}
