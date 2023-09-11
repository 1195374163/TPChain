package TPOChain;

import TPOChain.ipc.SubmitOrderMsg;
import TPOChain.messages.*;
import TPOChain.notifications.*;
import TPOChain.timers.FlushMsgTimer;
import TPOChain.timers.LeaderTimer;
import TPOChain.timers.ReconnectDataTimer;

import TPOChain.utils.*;
import common.values.AppOpBatch;
import common.values.PaxosValue;
import frontend.ipc.SubmitBatchRequest;
import io.netty.channel.EventLoopGroup;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.babel.internal.*;
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel;
import pt.unl.fct.di.novasys.channel.tcp.events.*;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TPOChainData extends GenericProtocol  implements ShareDistrubutedInstances {

    
    private static final Logger logger = LogManager.getLogger(TPOChainData.class);

    // Babel 协议标识
    public final static short PROTOCOL_ID = 300;
    public final static String PROTOCOL_NAME = "TPOChainData";
    // 下面还有一个通道的标识  上面这两个都加上通道标识号
    
    
    
    public static final String ADDRESS_KEY = "consensus_address";
    // 值等于原始协议号+通道标识
    public static final String DATA_PORT_KEY = "data_port";
    
    
    
 
    public static final String NOOP_INTERVAL_KEY = "noop_interval";
    
    public static final String RECONNECT_TIME_KEY = "reconnect_time";
    
    public static final String QUORUM_SIZE_KEY = "quorum_size";

    
    
    private final int NOOP_SEND_INTERVAL;
    
    private final int RECONNECT_TIME;
    
    private final int QUORUM_SIZE;
    
    
    
    

    /**
     *继承接口的局部分配日志表
     */

    /**
     * 继承接口实例的日志队列
     * */
    
    /**
     * 继承接口的对节点的一些配置信息，主要是各前链节点分发的实例信息和接收到accptcl的数量
     */
    

    //废弃   ---------前链节点使用：在commandleader接收ack时开启一次性闹钟发完即关闭，在第一次ack和  accept与send  相等时，关闹钟，刚来时
    private  long  frontflushMsgTimer = -1;//主要是前链什么时候发送flushMsg信息  
    // 上一次发送发送accept附带的ack的时间
    private  long  lastSendTime;
    
    // 作为前链节点上次接收ack的时间：如果长时间没有新的实例过来，那么ack信息需要重新发送
    private  long  lastReceiveAckTime;
    
    
    
    
    
    /**
     * AppOpBatch是对Front层消息的进一步包装
     * */
    //是前链但还不能处理请求的暂存队列，即在leader不存在或还没有连接成功时暂存
    private final Queue<AppOpBatch> waitingAppOps = new LinkedList<>();
    
    // 旧通道的东西需要完全处理完，才能转发新的。 如果接收新的，则暂存这个队列  
    private final Queue<AppOpBatch> waitingFrontNodeInitialAppOps = new LinkedList<>();
    
    //前链节点使用：向Leader发送申请排序的失败的，必须等待Leader重新连接之后，重发到Leader
    private final  Queue<OrderMSg>  failtoLeaderAppOps=new LinkedList<>();



    
    
    // 使用这条通道的历来节点(不包含当前节点)的链表，主要是因为前链节点可能故障，恢复时会有新的前链节点重新使用这条通道，需要重发一下ack信息
    // 添加：如果这个根据在更换前链的方法中将accpetNodeInetAddress添加其中
    // 删除：当收到这个节点的ack达到accpt数且新的通道使用者不是这个节点，这个可以删除
    private final  List<InetAddress>  oldChanelUser =new ArrayList<>();
    
    //根据从TPOChainProto传过来的前链节点分析当前通道的使用者
    private  InetAddress  channelUser;
    private  Map<Integer, InstanceState>  channelUserInstanceMap;
    private  RuntimeConfigure  channelUserRuntimeConfigure;
    
    
    
    
    
    //Data通道的标记，使用哪个通道，初始构建确定的，是不可更改的
    private  short  index;
    
    
    
    
    
    //前链
    private  List<InetAddress>  frontChain;
    //后链
    private List<InetAddress>  backChain;
    //数据端口：=初始端口+通道标识
    private int  data_port;
    
    //总链：前链加后链：元素是包含端口号的完整 ip:port
    protected List<Host> membership=new ArrayList<>();
    // TODO: 2023/8/13 不应该有Head，只要确认是链头就可以了 
    // 这个数据通道中的链首，主要是发送Ack  
    private Host head;
    //前链连接和后链连接
    private Host self;
    //下一个节点：链尾的节点将nextOk需要连接Head节点，因为链尾节点需要向Head
    private Host  nextok;
    private boolean nextokConnected;
    //leader 向leader发送排序请求，这里的端口号是TPOChainProto的端口50300而不是自己的50200 
    private Host leader; 
    //标记前链节点能否开始处理客户端的请求 其实可以废弃,用leaderConnnected标记代替
    private boolean canHandleQequest = false;
    
    
    // 根据membership推算出自己是否为前链节点
    /**
     * 标记是否为前段节点，是的话可以分发消息，并向leader发送排序
     */
    private boolean amFrontedNode;
    
    // 与其他节点建立连接的节点，根据Control层传递过来的，没有连接，则建立连接，
    private final Set<Host> establishedConnections = new HashSet<>();

    
    
    
    
    //由Control层控制，在uponOutConnection中使用
    private TPOChainProto.State state;

    
    
    
    // gc线程 ：GC回收通道使用者的旧日志    那历来使用者的日志呢  
    private  Thread gcThread;
    // 旧分发消息的ack  还有   旧执行信息(这个从配值文件找)
    private BlockingQueue<Integer> oldackqueue = new LinkedBlockingQueue<Integer>();

    
    

    // 网络层的通道
    private int peerChannel;

    /**
     * java中net框架所需要的数据结构
     */
    private EventLoopGroup workerGroup;
    
    
    public TPOChainData(Properties props,short index, EventLoopGroup workerGroup) throws UnknownHostException {
        super(PROTOCOL_NAME+index, (short)(PROTOCOL_ID+index) /*, new BetterEventPriorityQueue()*/);
        this.workerGroup = workerGroup;

        this.index=index;

        amFrontedNode = false; //默认不是前链节点

        //端口号为50200到50203
        data_port=Integer.parseInt(props.getProperty(DATA_PORT_KEY))+index;
        self = new Host(InetAddress.getByName(props.getProperty(ADDRESS_KEY)),
                data_port);
        head=null;
        nextok=null;
        nextokConnected=false;
        leader = null;
        //相应的默认不能处理请求，在leader没有连接时是这样的操作
        canHandleQequest = false;
        // 当前链节点更换时，需要将这个设置为false，
        canHandelNewBatchRequest=true;
        
        this.RECONNECT_TIME = Integer.parseInt(props.getProperty(RECONNECT_TIME_KEY));
        this.NOOP_SEND_INTERVAL = Integer.parseInt(props.getProperty(NOOP_INTERVAL_KEY));
        this.QUORUM_SIZE = Integer.parseInt(props.getProperty(QUORUM_SIZE_KEY));
        
        this.gcThread= new Thread(this::gcLoop, (PROTOCOL_NAME+index) + "-" + (short)(PROTOCOL_ID+index)+"---gc");
    }
    
    
    
    public void init(Properties props) throws HandlerRegistrationException, IOException {
        //申请网络通道
        Properties peerProps = new Properties();
        peerProps.put(TCPChannel.ADDRESS_KEY, props.getProperty(ADDRESS_KEY));
        peerProps.setProperty(TCPChannel.PORT_KEY, Integer.toString(data_port));
        peerProps.put(TCPChannel.WORKER_GROUP_KEY, workerGroup);
        peerChannel = createChannel(TCPChannel.NAME, peerProps);
        setDefaultChannel(peerChannel);
    

        registerMessageSerializer(peerChannel, AcceptAckMsg.MSG_CODE, AcceptAckMsg.serializer);
        registerMessageSerializer(peerChannel, AcceptMsg.MSG_CODE, AcceptMsg.serializer);
        // 需要加这个，因为数据层要往控制层发送这个排序请求,需要序列化
        registerMessageSerializer(peerChannel, OrderMSg.MSG_CODE, OrderMSg.serializer);

        
        registerMessageHandler(peerChannel, AcceptAckMsg.MSG_CODE, this::uponAcceptAckMsg, this::uponMessageFailed);
        registerMessageHandler(peerChannel, AcceptMsg.MSG_CODE, this::uponAcceptMsg, this::uponMessageFailed);
        

        //事实是废弃这个-------------------设置一份刷新时钟
        registerTimerHandler(FlushMsgTimer.TIMER_ID, this::onFlushMsgTimer);
        //这里的Reconnect和控制层的Reconnect重了，需要单独设置一份重连的时钟
        registerTimerHandler(ReconnectDataTimer.TIMER_ID, this::onReconnectDataTimer);
        
        
        
        //接收从front的 写 请求，读由Control层负责，
        registerRequestHandler(SubmitBatchRequest.REQUEST_ID, this::onSubmitBatch);

        
        
        // 接收上层Control层的一些控制信息
        subscribeNotification(InitializeCompletedNotification.NOTIFICATION_ID, this::onInitializeCompletedNotification);
        subscribeNotification(FrontChainNotification.NOTIFICATION_ID, this::onFrontChainNotification);
        subscribeNotification(LeaderNotification.NOTIFICATION_ID, this::onLeaderChange);
        subscribeNotification(StateNotification.NOTIFICATION_ID, this::onStateNotification);       
        
        
        // 注册通道事件
        registerChannelEventHandler(peerChannel, OutConnectionDown.EVENT_ID, this::onOutConnectionDown);
        registerChannelEventHandler(peerChannel, OutConnectionUp.EVENT_ID, this::onOutConnectionUp);
        registerChannelEventHandler(peerChannel, OutConnectionFailed.EVENT_ID, this::onOutConnectionFailed);
        registerChannelEventHandler(peerChannel, InConnectionDown.EVENT_ID, this::uponInConnectionDown);
        registerChannelEventHandler(peerChannel, InConnectionUp.EVENT_ID, this::uponInConnectionUp);
        
        //初始设定为Active
        state=TPOChainProto.State.ACTIVE;
        gcThread.start();
        logger.info("TPOChaindata"+index+"开启: ");
    }

    // 获取当前配置的锁
    public final Object  getConfigureAndInstanceMapLock=new Object();
    
    // TODO: 2023/7/27 修复错误gc线程:程序爆Null错误，一般就是这出了问题：解决办法是当换节点时清空ack表
    //  强制设定  GC线程只处理  通道使用者的回收任务,因为别的节点就算剩余也不会残余太多
    //  不使用accept使用channeluser 而且对通道使用者每一轮进行缓存到中间变量
    private void gcLoop() {
        // 解决方式是以ack为主，从队列中拿出小于ack的execute，处理gc+1 ——>execute的实例 
        //  因为取出  配置和 对应的分发实例， 
        // 应该负责清除本通道的数据分发
        // 这里其实有bug：当accept节点更换后，ack还是旧值
        // 在通道使用者更换时清楚ack旧值队列  
        int receiveack=-1;
        int  receiveexecute=-1;
        RuntimeConfigure  temp = null;
        Map<Integer, InstanceState> tempMap=null;
        // 从两个队列中peek一个值吧，
        while (true) {
            //因为通道使用者可能改变，先缓存
            synchronized (getConfigureAndInstanceMapLock){
                temp=channelUserRuntimeConfigure;
                tempMap=channelUserInstanceMap;
            }
            if (temp==null || tempMap==null){
                try {
                    Thread.sleep(2000);
                    continue;
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            // 释放锁，不要立即重新获得锁需要空闲
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            try {
                Integer newreceiveack=temp.ackFlagQueue.poll(3000, java.util.concurrent.TimeUnit.MILLISECONDS);
                if (newreceiveack==null){
                    continue;
                }
                // 不为空，赋值
                receiveack=newreceiveack;
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            // 已经得到了ack值，接下来是得到execute值,而且应该是取小于ack的值
            int  loopTimer=0;
            boolean gcFlag;
            while(true){// 确保execute队列不为空
                Integer newreceiveexecute=temp.executeFlagQueue.peek();
                if (newreceiveexecute!=null){
                    receiveexecute=newreceiveexecute;
                    if (receiveexecute<=receiveack){
                        for (int i=temp.highestGCInstance+1;i<receiveexecute;i++){
                            if (temp.highestGCInstance<receiveexecute){
                                tempMap.remove(i);
                                //将上面的++ 改为实例号的直接赋值 
                                temp.highestGCInstance=i;
                            }
                        }
                        temp.executeFlagQueue.poll();// 移除execute值
                        gcFlag=true;
                        loopTimer=0;
                    }else{
                        break;//因为execute已经超过ack，退出execute的循环，重新选择新ack
                    }
                }else {
                    gcFlag=false;
                    loopTimer++;
                }
                // 暂停3s
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                //如果连续四次没有执行回收，跳过每次对execute的等待
                if ( !gcFlag && loopTimer>=4){
                    break;
                }
            }
        }
    }



    // 暂存哪些节点回复信息
    public Map<SeqN, Set<Host>> QueryOldChannelUserMsgResponses=new HashMap<>();
    
    // TODO: 2023/8/24  在调用处排查是否已经全部连接 
    // TODO: 2023/8/22 考虑初始情况和后续前链节点更改 ,
    public  void  tryToBeFrontNode(){
        logger.info("Attempting to be FrontNode  ");
        List<AbstractMap.SimpleEntry<Host, Integer>> pairList = new ArrayList<>(oldChanelUser.size());
        //生成请求
        for (InetAddress nodeInetAddress:oldChanelUser) {
            AbstractMap.SimpleEntry<Host, Integer> pair = new AbstractMap.SimpleEntry<>(new Host(nodeInetAddress,data_port), hostConfigureMap.get(nodeInetAddress).highestAcknowledgedInstance);
            pairList.add(pair);   
        }
        QueryOldChannelUserMsg qMsg = new QueryOldChannelUserMsg(newterm, pairList);
        QueryOldChannelUserMsgResponses.put(newterm,new HashSet<>());
        //这是对所有节点包括自己发送prepare消息
        membership.forEach(h -> sendOrEnqueue(qMsg, h));
    }
    
    
    // TODO: 2023/8/22 分为初始情况 和 更改情况后
    /**
     * 接收新前链节点的关于旧通道使用者的询问消息
     * */
    public  void  uponQueryOldChannelUserMsg(QueryOldChannelUserMsg msg, Host from, short sourceProto, int channel){
        //生成答复列表
        List<AbstractMap.SimpleEntry<Host, List<AcceptedValue>>> answerList= new ArrayList<>(msg.request.size());
        for (AbstractMap.SimpleEntry<Host, Integer> pair :msg.request){
            InetAddress tempInetAddress=pair.getKey().getAddress();//具体哪个节点的日志
            int ack=pair.getValue();// 序号
            int  limit=hostConfigureMap.get(tempInetAddress).highestAcceptedInstance;
            List<AcceptedValue>  accptvaluesList=new ArrayList<>(limit-ack+1);
            Map<Integer, InstanceState> instMap=instances.get(tempInetAddress);
            for (int i=ack+1;i<=limit;i++){
                InstanceState  instemp=instMap.get(i);
                //int itemIN= instemp.iN;
                //SeqN itemSeqN= instemp.highestAccept;
                //PaxosValue itemPaxosValue= instemp.acceptedValue;
                accptvaluesList.add(new AcceptedValue(instemp.iN,instemp.highestAccept,instemp.acceptedValue));
            }
            answerList.add(new AbstractMap.SimpleEntry<>(pair.getKey(),accptvaluesList));
        }
        //生成答复消息，并发回原节点
        sendOrEnqueue(new QueryOldChannelUserOkMsg(msg.sN,answerList),from);
    }
    

    
    /**
     * 作为前链节点接收ok消息
     * */
    public  void  uponQueryOldChannelUserOkMsg(QueryOldChannelUserOkMsg msg, Host from, short sourceProto, int channel){
        
        if (QueryOldChannelUserMsgResponses.get(msg.sN)!=null){
            QueryOldChannelUserMsgResponses.get(msg.sN).add(from);
        }else {
            return;
        }
        
        //生成新的accept实例
        for (AbstractMap.SimpleEntry<Host, List<AcceptedValue>> item:msg.answerList) {
            InetAddress tempInetAddress=item.getKey().getAddress();
            Map<Integer, InstanceState> instanceStateMap=instances.get(tempInetAddress);
            RuntimeConfigure tempconfigure=hostConfigureMap.get(tempInetAddress);
            for (AcceptedValue acceptedValue : item.getValue()) {
                // 得到实例
                InstanceState instance;
                if (!instanceStateMap.containsKey(acceptedValue.instance)) {
                    instance=new InstanceState(acceptedValue.instance);
                    instanceStateMap.put(acceptedValue.instance, instance);
                }else {
                    instance=instanceStateMap.get(acceptedValue.instance);
                }
                //  候选者的别的实例为空   或   新实例的term大于存在的实例的term
                if (instance.highestAccept == null || acceptedValue.sN.greaterThan(
                        instance.highestAccept)) {
                    instance.forceAccept(acceptedValue.sN, acceptedValue.value);
                    
                    //Make sure there are no gaps between instances
                    if (instance.iN > tempconfigure.highestAcceptedInstance) {
                        //highestAcceptedInstanceCl++;// 原来是这个
                        tempconfigure.highestAcceptedInstance=instance.iN;
                    }
                }else {//不需要管
                    //到达这里 表明这个节点已经有了对应的实例，且那个实例的leader term大于
                    // 消息中的term，那么不对Msg中的消息进行更新
                }
            }
        }
        
        if (QueryOldChannelUserMsgResponses.size() == QUORUM_SIZE) {
            QueryOldChannelUserMsgResponses.remove(msg.sN);
            beacomeFrontNode();
        }
    }
    
    
    
    // 标记是否能处理新的命令，前链节点使用，
    public boolean  canHandelNewBatchRequest;
    // TODO: 2023/8/21 应该有一个成为分发节点的函数,向其他节点请求prepareOk信息。
    public  void  beacomeFrontNode(){
        //应该将上一个通道的消息转发完毕,重新转发所有ack+1 -> accept的消息
        for (InetAddress temp:oldChanelUser) {
            RuntimeConfigure runtimeConfiguretemp=hostConfigureMap.get(temp);
            Map<Integer, InstanceState> instansMap=instances.get(temp);
            for (int i=runtimeConfiguretemp.highestAcknowledgedInstance+1;i<=runtimeConfiguretemp.highestAcceptedInstance;i++){
                InstanceState  instemp=instansMap.get(i);
                SeqN  newseq=new SeqN(instemp.highestAccept.getCounter()+1,instemp.highestAccept.getNode()); 
                // FIXME: 2023/8/25  这里的Term以原来实例的
                AcceptMsg msg=new AcceptMsg(instemp.iN,newseq,(short)0,instemp.acceptedValue,runtimeConfiguretemp.highestAcknowledgedInstance);
                sendOrEnqueue(msg, self);
                // TODO: 2023/8/25  生成排序消息发往Leader 
                //OrderMSg orderMSg = new OrderMSg(new Host(temp,data_port), instemp.iN);
                //sendOrEnqueue(orderMSg, leader);
            }
        }
        // 设定可以处理新消息了
        canHandelNewBatchRequest=true;
        //处理缓存的之前的Leader不能连接的 
        while(!waitingAppOps.isEmpty()){
            AppOpBatch appOpBatch = waitingAppOps.poll();
            // 处理 appOpBatch，可以对其进行操作或输出
            sendNextAccept(appOpBatch);
        }
        //处理缓存在前链节点更改后的
        while(!waitingFrontNodeInitialAppOps.isEmpty()){
            AppOpBatch appOpBatch = waitingFrontNodeInitialAppOps.poll();
            // 处理 appOpBatch，可以对其进行操作或输出
            sendNextAccept(appOpBatch);
        }
    }


    
    // TODO: 2023/9/11   上面的操作是将旧消息重发全局，之后新消息才能接着转发

    /**---------------------------接收来自front层的分发消息 ---------**/

    /**
     * 当前时leader或正在竞选leader的情况下处理frontend的提交batch
     */
    public void onSubmitBatch(SubmitBatchRequest not, short from) {
        // TODO: 2023/8/22 应该设置一个发送旧通道使用者的所有消息之后才能转发新的消息
        if (amFrontedNode) {// 改为前链节点 
            if (canHandelNewBatchRequest){// 这是前链节点初始化完成
                if (canHandleQequest) {//这个标志意味着leader存在
                    //先将以往的消息转发，还有其他候选者节点存储的消息在连接建立时处理
                    //if (logger.isDebugEnabled()) {
                    //    logger.debug("接收来自front层的批处理并开始处理sendNextAccept():" + not.getBatch());
                    //}
                    // 在连接建立的事件中，在canHandleQequest变为true时将暂存的消息开始处理
                    sendNextAccept(new AppOpBatch(not.getBatch()));
                } else {// leader不存在，无法排序,所以暂存处理
                    //if (logger.isDebugEnabled()) {
                    //    logger.debug("因为现在还没有leader,缓存来自front的批处理:" + not.getBatch());
                    //}
                    waitingAppOps.add(new AppOpBatch(not.getBatch()));
                }
            }else {
                waitingFrontNodeInitialAppOps.add(new AppOpBatch(not.getBatch()));  
            }
        } else //忽视接受的消息
            logger.warn("Received " + not + " without being FrontedNode, ignoring.");
    }



    

    /**-----------------------------处理节点的分发信息-------------------------------**/
    
    // -----------生成accept信息---------------------在通用配置
    private InetAddress selfInetAddress;
    // TODO: 2023/8/25 同一个通道，保持相同的term，当前链节点更换时，增加其中的数，
    //  或者不需要，
    private SeqN newterm;// = new SeqN(0, self);
    private RuntimeConfigure selfRuntimeConfigure;
    private Map<Integer, InstanceState>  selfInstanceMap;

    // TODO: 2023/8/3   因为只有在分发和排序都存在才可以执行，若非leader故障
    //  对非leader的排序消息要接着转发，不然程序执行不了
    //  所以先分发，后排序
    //  处理流程先分发后排序，排序可以加上
    
    /**
     * 在当前节点是前链节点时处理，发送 或Noop 或App_Batch信息
     */
    private void sendNextAccept(PaxosValue val) {
        //设置上次的刷新时间：上次发送时间 :这里应该用配置文件的时间，而不是设置的两个时间，其实也可以
        lastSendTime = System.currentTimeMillis();
        
        
        // 得到分发消息的那个实例  //InstanceState instance = selfInstanceMap.computeIfAbsent(selfRuntimeConfigure.lastAcceptSent + 1, InstanceState::new);
        InstanceState instance;
        if (!selfInstanceMap.containsKey(selfRuntimeConfigure.lastAcceptSent + 1)) {
            instance=new InstanceState(selfRuntimeConfigure.lastAcceptSent + 1);
            selfInstanceMap.put(selfRuntimeConfigure.lastAcceptSent + 1, instance);
        }else {
            instance=selfInstanceMap.get(selfRuntimeConfigure.lastAcceptSent + 1);
        }
        
        
        // 先发给自己:这里直接使用对应方法
        //this.uponAcceptMsg(new AcceptMsg(instance.iN, newterm,
        //        (short) 0, nextValue,hostSendConfigure.highestAcknowledgedInstance), self, this.getProtoId(), peerChannel);
        sendOrEnqueue(new AcceptMsg(instance.iN, newterm,
                (short) 0, val, selfRuntimeConfigure.highestAcknowledgedInstance), self);
        //设置发送标记数
        selfRuntimeConfigure.lastAcceptSent = instance.iN;
        
        //重发所有失败的申请排序信息，在与Leader连接建立时已经完成，这里不需要做
        //同时向leader发送排序请求:排序不发noop消息改成直接向全体节点发送ack设置一个时钟,什么时候开启,什么时候关闭
        OrderMSg orderMSg = new OrderMSg(self, instance.iN);
        sendOrEnqueue(orderMSg, leader);
    }

    
    
    
    
    
    
    
    
    //------------接收Accept信息---------------下面的和通道使用者可能相同也可能不同---------------------------
    private InetAddress accpetNodeInetAddress;
    private  RuntimeConfigure  acceptRuntimeConfigure;
    private  Map<Integer, InstanceState>  acceptInstanceMap;
    
    /**
     * 处理accept信息
     */
    private void uponAcceptMsg(AcceptMsg msg, Host from, short sourceProto, int channel) {
        //对不在系统中的节点发送未定义消息让其重新加入系统
        if (!membership.contains(from)) {
            logger.warn("Received msg from unaffiliated host " + from);
            sendMessage(new UnaffiliatedMsg(), from, TCPChannel.CONNECTION_IN);
            return;
        }
        
        //if (logger.isDebugEnabled()) {
        //    logger.debug("接收到" + from + "的" + msg);
        //}
        
        // 得到分发消息的来源节点
        accpetNodeInetAddress = msg.sN.getNode().getAddress();
        acceptInstanceMap = instances.get(accpetNodeInetAddress);
        
        //得到分发消息的哪个实例日志
        InstanceState instance;
        if (!acceptInstanceMap.containsKey(msg.iN)) {
            instance=new InstanceState(msg.iN);
            acceptInstanceMap.put(msg.iN, instance);
        }else {
            instance=acceptInstanceMap.get(msg.iN);
        }

        // TODO: 2023/8/24   判断重复消息，对于编号小于已经接收的，丢弃
    
        //去重：如果消息是新生成的那么,它的投票数为0,肯定不满足下面这个条件，若是重发的则直接满足条件进行丢弃
        if (msg.sN.equals(instance.highestAccept) && msg.nodeCounter +1  <= instance.counter) {
            logger.warn("Discarding already existing acceptmsg");
            return;
        }
        // 输出日志
        
        // 先得到排序commandleader节点的配置信息
        acceptRuntimeConfigure = hostConfigureMap.get(accpetNodeInetAddress);
        
        //进行对实例接收和确认
        instance.accept(msg.sN, msg.value, (short) (msg.nodeCounter + 1));
        //进行更新领导操作时间
        acceptRuntimeConfigure.lastAcceptTime = System.currentTimeMillis();
        
        //对accept标记进行更新：更新highestAcceptedInstance信息：
        if (acceptRuntimeConfigure.highestAcceptedInstance < instance.iN) {
            acceptRuntimeConfigure.highestAcceptedInstance=instance.iN;
            //assert hostConfigureMap.get(sender).highestAcceptedInstance == instance.iN;
        }
        
        
        //下面是ack的处理，注意下面还需要完善 ：这里是对携带的ack信息的处理
        if (msg.ack > acceptRuntimeConfigure.highestAcknowledgedInstance) {
            acceptRuntimeConfigure.highestAcknowledgedInstance=msg.ack;
            //这个节点的配置表中lastReceiveAckTime时间
            acceptRuntimeConfigure.lastReceiveAckTime=System.currentTimeMillis();
            // TODO: 2023/7/28 从通道的历来使用者 
            //  判断通道的历来的使用者的ack是否等于accept值，如果则移除该节点，不止这里，
            //  还有在uponAck()也得需要这个方法
            if (oldChanelUser.contains(accpetNodeInetAddress)){
                if (acceptRuntimeConfigure.highestAcceptedInstance==acceptRuntimeConfigure.highestAcknowledgedInstance){
                    oldChanelUser.remove(accpetNodeInetAddress);
                }
            }
        }

        
        // 先转发，还是先ack，应该携带的ack是消息中自带的ack，而不是自己的
        forward(instance,accpetNodeInetAddress);
        
        //若当前节点将目前的通道使用节点加入集合中，将这条语句放入状态改变之后，不包含当前节点
        //if (!hashSetAcceptNode.contains(accpetNodeInetAddress)){
        //    hashSetAcceptNode.add(accpetNodeInetAddress);
        //}
        
        //将instance添加到对应的消息队列中:
        hostMessageQueue.get(accpetNodeInetAddress).add(instance);
        //将ack信息放入队列中，注意标注是什么节点的ack信息：已解决，将对应节点的ack，供
        acceptRuntimeConfigure.ackFlagQueue.add(msg.ack);
    }

    
    // 修复除了uponacceptMsg()用forward()，还有uponConnectionUp()建立重发ack->accept的消息
    //  所以需要传递用哪个节点的参数，需要在形参中加入使用那个节点
    /**
     * 转发accept信息给下一个节点
     */
    private void forward(InstanceState inst,InetAddress acceptNode) {
        // 如果转发ack信息，应该是消息中的ack，而不是实例中得的ack， 
        //  但因为除了uponacceptMsg()消息使用，还有uponconnectionup()使用，还是使用对应节点的配置
        Host sendHost = inst.highestAccept.getNode();
        // 拿到对应节点的分发配置信息
        RuntimeConfigure  acceptNodeRuntimeConfigure=hostConfigureMap.get(acceptNode);
        //发送ack信息
        if (nextok.equals(head)){
            //对当前通道使用者处理
            AcceptAckMsg acceptAckMsgtemp = new AcceptAckMsg(sendHost, inst.iN);
            if (head.equals(sendHost)){// 前链节点没有发生更换，那么只向他发送ack信息
                sendMessage(acceptAckMsgtemp, nextok);
            }else {// 前链节点发生更换，需要向所有节点发送ack消息
                //向集群中所有节点发送ack信息,不是历来的通道使用者
                //  因为Data是半连接，所以这里只能向连接到节点转发ack信息
                sendMessage(acceptAckMsgtemp, nextok);
            }

            // 在链尾节点对历来的通道使用者进行重新ack，
            //  作为尾节点扫描通道使用者的局部日志 ，如果这个节点的两个时间超过1s对全局节点发送ack消息
            long currentTime=System.currentTimeMillis();
            // 扫描历来的通道使用者，发送ack请求 
            // 不能是所有节点，只能是历来的通道的使用者，不能包含当前节点，而且是发送全局节点，Heade
            for (InetAddress item : oldChanelUser) {
                // 应该使用accept作为新的ack号
                AcceptAckMsg oldacceptAckMsgtemp = new AcceptAckMsg(new Host(item,data_port), hostConfigureMap.get(item).highestAcceptedInstance);
                //判断是否超时
                if (currentTime-hostConfigureMap.get(item).lastReceiveAckTime>1000){
                    // 应该向链头发送ack请求，由链头经过链转发至全部节点
                    sendMessage(oldacceptAckMsgtemp, head);
                }
            }
        }else {//正常地转发，这里使用的节点的配置中的ack序号
            AcceptMsg msg = new AcceptMsg(inst.iN, inst.highestAccept, inst.counter, inst.acceptedValue,
                    acceptNodeRuntimeConfigure.highestAcknowledgedInstance);
            sendMessage(msg,nextok);
        }
    }
    
    
    
    //-----接收ack信息-----------ack信息也能单独沿着链传播----
    
    /**
     * leader接收ack信息，对实例进行ack
     */
    private void uponAcceptAckMsg(AcceptAckMsg msg, Host from, short sourceProto, int channel) {
        //除了发送节点可以接受acceptack信息,其他节点也可以接受acceptack信息,因为发送节点可能宕机,所以开放其他节点的接受ack信息
        //if (logger.isDebugEnabled()) {
        //    logger.debug("接收" + from + "的:" + msg);
        //}
        Host ackHost = msg.node;
        InetAddress ackInetAddress=ackHost.getAddress();
        RuntimeConfigure ackHostRuntimeConfigure = hostConfigureMap.get(ackInetAddress);
        
        if (msg.instanceNumber <= ackHostRuntimeConfigure.highestAcknowledgedInstance) {
            logger.warn("Ignoring acceptAck of Data for old instance: " + msg+"from"+from);
            return;
        }
        
        // 设定上次接收ack的时间
        ackHostRuntimeConfigure.lastReceiveAckTime=System.currentTimeMillis();
        //   执行ack程序，取消++，改为直接赋值 
        ackHostRuntimeConfigure.highestAcknowledgedInstance=msg.instanceNumber;
        
        
        //在接收ack，如果是之前的通道使用者，且这个使用者的ack和accept相等，
        if (!ackInetAddress.equals(channelUser) && oldChanelUser.contains(ackInetAddress)){
            if (ackHostRuntimeConfigure.highestAcceptedInstance==ackHostRuntimeConfigure.highestAcknowledgedInstance){
                oldChanelUser.remove(ackHost);
            }
        }
        
        
        // TODO: 2023/8/14 如果自己不是ack中确认节点的发送方，那么要转发ack信息
        // TODO: 2023/8/2 只有这个节点是这条通道的历来使用者，才能使用这条通道发送ack信息 
        // FIXME: 2023/7/19 考虑故障之后，考虑将闹钟要表示是哪个节点的刷新信息
        //这里设置一个定时器,发送acceptack,一段时间没有后续要发的
        // 在发送新实例时在sendnextaccpt取消如果当前节点等于消息的发送节点,再设立闹钟
        // TODO: 2023/8/4 这里其实不太需要：因为我会 
        if (ackHost.equals(self)){
            lastReceiveAckTime=System.currentTimeMillis();
            // TODO: 2023/7/28  不能设置为noop 的0.1s  应该长一点，因为设置为0.5s甚至1s 
            //FIXME:   TODO: 2023/8/14  这里尝试条件，应该设定一定条件，才会设定闹钟
            if (lastReceiveAckTime-lastSendTime>1000){
                frontflushMsgTimer=setupTimer(FlushMsgTimer.instance,NOOP_SEND_INTERVAL);
            }
        }else {
            // 因为接收不是自己应该对这个ack信息所以需要进行转发，还有应该使用ackHost
            sendMessage(new AcceptAckMsg(ackHost, ackHostRuntimeConfigure.highestAcknowledgedInstance),nextok);
        }
    }

    
    // TODO: 2023/8/26 由链尾节点设置一个刷新时间，在一个日志一直没有新ack时，向所有节点(包括自己)发送新的ack信息
    /**
     * 对最后的此节点的消息进行发送ack信息，因为ack消息都是附着在accept消息上，如果没有accept消息，后面的ack没法发送全局  FlushMsgTimer
     */
    private void onFlushMsgTimer(FlushMsgTimer timer, long timerId) {
        // 为什么需要这个，是当没有accept请求之后，不能传送新ack请求，之后才启动的
        // 只能是前链节点才能使用这个功能，不是前链节点取消这个功能 
        if (amFrontedNode){
            // TODO: 2023/8/23 两个参数有误 
            if (lastReceiveAckTime-lastSendTime>NOOP_SEND_INTERVAL){
                //这里有问题，因为Data不是全连接的，ack信息应该沿着链传播 
                //这里有问题,应该只能发送nextok节点
                sendMessage(new AcceptAckMsg(self, selfRuntimeConfigure.highestAcknowledgedInstance),nextok);
                //membership.stream().filter(h -> !h.equals(self)).forEach(host -> sendMessage(new AcceptAckMsg(self, selfRuntimeConfigure.highestAcknowledgedInstance), host));
                if (logger.isDebugEnabled()) {
                    logger.debug("向nextok节点发送了ack为" + hostConfigureMap.get(self).highestAcknowledgedInstance + "的定时信息");
                }
            }
        }
    }




    

    /**------------------------------接收来自控制层的成员管理消息 ---------**/

    //Control层初始化完成，对自身节点的一些参数设置:设置self,主要是因为TPChain的控制层对局部分发日志和配置进行初始化
    protected  void onInitializeCompletedNotification(InitializeCompletedNotification notification,short emitterID){
        //可以将局部日志和配置表的初始化放在Data层而不是Control控制层 ？ 不能因为
        selfInetAddress=self.getAddress();
        newterm = new SeqN(0, self);
        selfRuntimeConfigure=hostConfigureMap.get(selfInetAddress);
        selfInstanceMap=instances.get(selfInetAddress);
    }
    
    // 接收control层的状态更改
    protected void onStateNotification(StateNotification notification,short emitterID){
        state=notification.getState();
    }
    
    //在前链节点和后链节点有改动时调用，初始操作时也视为成员发生改动
    protected  void onFrontChainNotification(FrontChainNotification notification,short emitterID){
        // TODO: 2023/9/11  在建立连接中如果此种没有某个节点，那么关闭和它的连接，如果没有某个节点，开启与它的连接 
        
        //logger.info("接收来的通知是"+notification.getFrontChain()+notification.getBackchain());
        frontChain=notification.getFrontChain();
        
        //根据控制层传递得到通道使用者是哪个节点
        InetAddress newchannelUser=frontChain.get(index);
        if (channelUser==null){
            channelUser=newchannelUser;
            synchronized (getConfigureAndInstanceMapLock){
                //得到通道使用者的日志表
                channelUserInstanceMap=instances.get(channelUser);
                //得到通道使用者的配置表
                channelUserRuntimeConfigure=hostConfigureMap.get(channelUser);
            }
        }else {
            //将之前的通道的使用者加入使用者列表
            if (!channelUser.equals(newchannelUser)){
                //hashSetAcceptNode.add(channelUser);
                oldChanelUser.add(channelUser);
                canHandelNewBatchRequest=false;
            }
            channelUser=newchannelUser;
            synchronized (getConfigureAndInstanceMapLock){
                //得到通道使用者的日志表
                channelUserInstanceMap=instances.get(channelUser);
                //得到通道使用者的配置表
                channelUserRuntimeConfigure=hostConfigureMap.get(channelUser);
            }
        }
        
        
        //在成员列表改动之后，清空原来的列表
        membership.clear();
        
        int size=frontChain.size();
        int loopindex=index;
        while(true){
            membership.add(new Host(frontChain.get(loopindex),data_port));
            loopindex=(loopindex+1)%size;
            if (membership.size()==size){
                break;
            }
        }//logger.info("输出前链"+membership);
        //判断若前链中包括自己，那么设置一个是amFrontedNode标记
        if (membership.contains(self)){
            amFrontedNode=true;
        }else {
            amFrontedNode=false;
        }
        //添加后链
        backChain=notification.getBackchain();
        for (InetAddress tmp:backChain) {
            membership.add(new Host(tmp,data_port));
        }//logger.info("输出总链"+membership);
        // 对所有其他未建立连接的节点连接
        membership.stream().filter(h -> !h.equals(self) && !establishedConnections.contains(h)).forEach(this::openConnection);
        //得到链首
        head =membership.get(0);
        //设置nextok节点 --------这个常用
        int memsize=membership.size();
        int selfindex=membership.indexOf(self);
        Host newnextok=membership.get((selfindex+1)%memsize);
        // 初始操作
        if (nextok==null){
            nextok=membership.get((selfindex+1)%memsize);
            return;
        }
        //如果有新的next节点连接
        if (!newnextok.equals(nextok)){
            nextok=newnextok;
            // 对消息进行转发，从ack+1->accept  以三种消息全部转发，以往的通道使用者
            // TODO: 2023/8/25  对消息进行转发
            
            
        }
        //if (nextok == null || !newnextok.equals(nextok)) {
        //    //Close old writesTo
        //    if (nextok != null && !nextok.getAddress().equals(self.getAddress())) {
        //        nextokConnected = false;
        //        closeConnection(nextok, peerChannel);
        //    }
        //    //Update and open to new writesTo
        //    nextok =newnextok;
        //    logger.info("New nextok connecting: " + nextok.getAddress());
        //    if (!nextok.getAddress().equals(self.getAddress()))
        //        openConnection(nextok, peerChannel);
        //} // 否则不改变nextok指向
    }

    
    // 以后关于成员更改要重新考虑，现在不做考虑：程序一些bug在之后再考虑 在leader改变时
    protected void  onLeaderChange(LeaderNotification notification, short emitterId){
        Host notleader=notification.getLeader();
        //Writes to changed
        if (leader == null || !notleader.equals(leader)) {
            //Close old writesTo    为什么是不等于 self，因为需要关闭连接
            if (leader != null && !leader.getAddress().equals(self.getAddress())) {
                canHandleQequest = false;
                closeConnection(leader, peerChannel);
            }
            //Update and open to new writesTo
            leader =notleader;
            logger.info("New leader connecting: " + leader.getAddress());
            if (!leader.getAddress().equals(self.getAddress()))
                openConnection(leader, peerChannel);
            // 自身是leader，开启
            if (leader.getAddress().equals(self.getAddress())){// 
                canHandleQequest=true;
            }
        }
    }

    
    
    
    
    
    
    
    //对于向Leader节点失败的消息需要暂存之后重发，向nextOk失败的不需要重发
    /**
     * 消息Failed，发出logger.warn("Failed:)
     */
    private void uponMessageFailed(ProtoMessage msg, Host host, short i, Throwable throwable, int i1) {
        // 如果是向Leader发送排序消息，因为旧leader故障，需要将失败的存档重新发新Leader
        // 直接在这里进行重发，在Leader建立新连接时，根据消息类型，而不应该是目的地来区分
        // 这里只针对申请排序消息，因为分发消息在连接建立时会向next节点重发ack——>accept的消息
        logger.warn("Failed: " + msg + ", to: " + host + ", reason: " + throwable.getMessage());
        // TODO: 2023/7/27 上面的输出日志，只是调试，测试可以成功运行之后，注释掉上面的这句话 
        
        // 如果Leader节点的刚发出  分发消息  就宕机故障，考虑这种情况
        //Notice 可能有bug：发送失败作为一个事件，后处理的话，怎么办,新Leader在失败事件之前怎么处理？
        // 大概率不会，Leader选举会耗时超过失败事件的处理
        //先发排序消息，后发分发消息
        if (msg instanceof OrderMSg){
            failtoLeaderAppOps.add((OrderMSg)msg);
        }
    }
    
    
    //data层连接到 leader节点 nextokback nextokfront节点
    /**
     * 在TCP连接writesTO节点时，将pendingWrites逐条发送到下一个节点
     */
    protected void onOutConnectionUp(OutConnectionUp event, int channel) {
        //在连接
        Host peer = event.getNode();
        establishedConnections.add(peer);
        if (peer.equals(leader)) {
            // 设置能接收客户端处理
            canHandleQequest = true;
            // 重发失败的排序消息是OrderMsg
            while(!failtoLeaderAppOps.isEmpty()){
                sendOrEnqueue(failtoLeaderAppOps.poll(),leader);
            }
            if (canHandelNewBatchRequest){
                //开始处理暂存的请求：是对Front层的batch的封装
                while(!waitingAppOps.isEmpty()){
                    AppOpBatch appOpBatch = waitingAppOps.poll();
                    // 处理 appOpBatch，可以对其进行操作或输出
                    sendNextAccept(appOpBatch);
                }
            }
            //这里将暂存的消息从暂存队列中取出来开始处理
            logger.info("already Connected to leader :"+leader.getAddress());
        }
        
        // TODO: 2023/7/31 如果当前节点属于新加入节点，不应该传输分发日志，直接return; 
        if (peer.equals(nextok)){
            nextokConnected=true;
            logger.info("already Connected to nextok :"+nextok.getAddress());
            // TODO: 2023/8/2 有三种状态： 在join状态不能转发，那在状态迁移状态下能转发实例
            if(state == TPOChainProto.State.JOINING)  // 是新加入节点，因为日志信息为空，所以不能转发
                return;
            //应该改为所有的通道的使用者,以往的节点重发，这里不用担心，
            Iterator<InetAddress> iterator = oldChanelUser.iterator();
            while (iterator.hasNext()) {
                InetAddress element = iterator.next();
                Map<Integer, InstanceState>   elementInstanceMap =instances.get(element);
                RuntimeConfigure  elementRuntimeConfigure= hostConfigureMap.get(element);
                for (int i = elementRuntimeConfigure.highestAcknowledgedInstance + 1; i <= elementRuntimeConfigure.highestAcceptedInstance; i++) {
                    forward(elementInstanceMap.get(i),element);
                }
            }
            //因为通道使用者后面才赋值，刚开始建立是null， 现在的通道使用者重发ack->accept的分发消息
            if (channelUserRuntimeConfigure!=null){//为什么要判断，当这条链初始建立时，是没有任何信息的
                for (int i = channelUserRuntimeConfigure.highestAcknowledgedInstance + 1; i <= channelUserRuntimeConfigure.highestAcceptedInstance; i++) {
                    forward(channelUserInstanceMap.get(i),channelUser);
                }
            }
        }
    }

    // 是否可以将1s的时长设置为0.1s，加速连接
    /**
     * 在与下一个节点断开后开始重连
     */
    protected void onOutConnectionDown(OutConnectionDown event, int channel) {
        Host peer = event.getNode();
        establishedConnections.remove(peer);
        if (peer.equals(leader)) {
            logger.warn("Lost connection to leader, re-connecting in 1: " + event);
            canHandleQequest=false;
            setupTimer(new ReconnectDataTimer(leader), 1000);
            return;
        }
        if (peer.equals(nextok)){
            nextokConnected=false;
            logger.warn("Lost connection to nextok, re-connecting in 1: " + event);
            setupTimer(new ReconnectDataTimer(nextok), 1000);
            return;
        }
        setupTimer(new ReconnectDataTimer(peer), 1000);
    }

    /**
     * 在连接失败时，刚开始连接会因为对方节点没启动，面临失败
     * */
    protected void onOutConnectionFailed(OutConnectionFailed<Void> event, int channel) {
        Host peer = event.getNode();
        //为什么不用这个，因为leader和nextok有可能更改，若原来的连接节点失效，那么什么也不操作
        //setupTimer(new ReconnectDataTimer(peer), 1000);
        if (peer.equals(leader)) {
            logger.warn("Connection failed to leader, re-trying in 1: " + event);
            //leaderConnected = false;
            setupTimer(new ReconnectDataTimer(leader), 1000);
            return;
        }
        if (peer.equals(nextok)){
            logger.warn("Connection failed to nextok, re-trying in 1: " + event);
            //leaderConnected = false;
            setupTimer(new ReconnectDataTimer(nextok), 1000);
            return;
        }
        setupTimer(new ReconnectDataTimer(peer), 1000);
    }

    
    /**
     * 与 相应的尝试重新建立连接
     */
    private void onReconnectDataTimer(ReconnectDataTimer timer, long timerId) {
        Host temp=timer.getHost();
        //logger.info("time is out,reconnect to:"+timer.getHost());
        //openConnection(timer.getHost());
        if (temp.equals(leader)) {
            logger.info("Date Layer Trying to reconnect to leader " + timer.getHost());
            openConnection(leader);
            return;
        }
        if (timer.getHost().equals(nextok)) {
            logger.info("Date Layer Trying to reconnect to nextOk " + timer.getHost());
            openConnection(nextok);
            return;
        }
        openConnection(temp);
    }
    
    //无实际动作
    private void uponInConnectionUp(InConnectionUp event, int channel) {
        if (logger.isDebugEnabled()){
            logger.debug(event);
        }
    }
    
    //无实际动作
    private void uponInConnectionDown(InConnectionDown event, int channel) {
        if (logger.isDebugEnabled()){
            logger.debug(event);
        }
    }
    


    
    
    //这里不需要判断Leader和nextok是否已经连接，因为是调用处是直接调用这个方法，预先已经判定成功了，即使后面有断开连接事件肯定在失败事件之前，当然也有用，发送Leader
    /**
     * 发送消息给自己和其他主机-----也就是accept和ack信息以及向Leader的申请排序信息OrderMsg
     */
    void sendOrEnqueue(ProtoMessage msg, Host destination) {
        // 判空语句，只是判断调用参数
        if (msg == null || destination == null) {
            logger.error("null: " + msg + " " + destination);
            return;
        }
        // 满足下方条件只会是排序消息，而不是分发消息
        if (leader!=null && destination.equals(leader)) {
            if (leader.getAddress().equals(self.getAddress())){// 当Leader为自身节点
                sendRequest(new SubmitOrderMsg((OrderMSg) msg), TPOChainProto.PROTOCOL_ID); 
            }else {
                sendMessage(msg, TPOChainProto.PROTOCOL_ID,destination);
            }
            return;
        }
        // 对于accept和ack信息,一般来说就是前链节点在接收自己处理的accept信息：
        if (destination.equals(self)) {//
            // 在生成accept消息时需要这个方法。
            deliverMessageIn(new MessageInEvent(new BabelMessage(msg, (short) -1, (short) -1), self, peerChannel));
        } else {
            sendMessage(msg, destination);
        }
    }
}    
