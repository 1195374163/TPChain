package TPOChain;

import TPOChain.ipc.SubmitOrderMsg;
import TPOChain.messages.AcceptAckMsg;
import TPOChain.messages.AcceptMsg;
import TPOChain.messages.OrderMSg;
import TPOChain.messages.UnaffiliatedMsg;
import TPOChain.notifications.FrontChainNotification;
import TPOChain.notifications.InitializeCompletedNotification;
import TPOChain.notifications.LeaderNotification;
import TPOChain.notifications.ThreadidamFrontNextFrontNextBackNotification;
import TPOChain.timers.FlushMsgTimer;
import TPOChain.timers.ReconnectDataTimer;
import TPOChain.timers.ReconnectTimer;
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
import pt.unl.fct.di.novasys.babel.internal.BabelMessage;
import pt.unl.fct.di.novasys.babel.internal.MessageInEvent;
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel;
import pt.unl.fct.di.novasys.channel.tcp.events.*;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

public class TPOChainData extends GenericProtocol  implements ShareDistrubutedInstances {

    //下面是字段名的String
    private static final Logger logger = LogManager.getLogger(TPOChainData.class);

    public final static short PROTOCOL_ID = 300;
    public final static String PROTOCOL_NAME = "TPOChainData";


    public static final String ADDRESS_KEY = "consensus_address";
    public static final String DATA_PORT_KEY = "data_port";
    
    //public static final String CONSENSUS_PORT_KEY = "consensus_port";


    public static final String RECONNECT_TIME_KEY = "reconnect_time";
    public static final String NOOP_INTERVAL_KEY = "noop_interval";

    public static final String QUORUM_SIZE_KEY = "quorum_size";


    
    // 下面是变量
    private final int NOOP_SEND_INTERVAL;
    
    private final int RECONNECT_TIME;
    
    private final int QUORUM_SIZE;




    
    
    //private Host nextOkFront;
    //private boolean nextOkFrontConnected;
    //private Host nextOkBack;
    //private boolean nextOkBackConnected;
    
    //自己节点对分发消息使用通道的哪种序号的线程来处理
    private short threadid;

    

    /**
     *还有继承接口的局部日志表
     */
    
    /**
     * 继承接口实例填充到哪标记
     * */

    /**
     * 继承接口的对节点的一些配置信息，主要是各前链节点分发的实例信息
     * 和 接收到accptcl的数量
     */
    //private Map<Host, RuntimeConfigure> hostConfigureMap ;
    
    
            
   
    private long frontflushMsgTimer = -1;
    //前链节点使用：在commandleader发送第一条命令的时候开启闹钟，在第一次ack和  accept与send  相等时，关闹钟，刚来时
    //主要是前链什么时候发送flushMsg信息
    private long lastSendTime;

    // 还有一个field ： System.currentTimeMillis(); 隐含了系统的当前时间

    private  long  lastReceiveAckTime;
    
    
    //是前链但还不能处理请求的暂存队列
    private final Queue<AppOpBatch> waitingAppOps = new LinkedList<>();
    

    
    // 网络层的通道
    private int peerChannel;
    
    /**
     * java中net框架所需要的数据结构
     */
    private EventLoopGroup workerGroup;
    
    
    
    
    //前链
    private  List<InetAddress>  frontChain;
    //后链
    private List<InetAddress>  backChain;
    //数据端口
    private int  data_port;
    //总链：前链加后链：元素是包含端口号的完整 ip:port
    protected List<Host> membership=new ArrayList<>();
    //Data通道的标记
    private  short  index;
    // 这个数据通道中的链首
    private Host Head;
    //前链连接和后链连接
    private Host self;
    //下一个节点
    private Host  nextok;
    private boolean nextokConnected;
    //leader 向leader发送排序请求，这里的端口号是TPOChainProto的端口50300而不是自己的50200 
    private Host leader; //private boolean leaderConnected;
   
    //标记前链节点能否开始处理客户端的请求 其实可以废弃,用leaderConnnected标记代替
    private boolean canHandleQequest = false;
    
    // 根据membership推算出自己是否为前链节点
    /**
     * 标记是否为前段节点，代表者可以发送command，并向leader发送排序
     */
    private boolean amFrontedNode;
    
    
    
    public TPOChainData(Properties props, EventLoopGroup workerGroup) throws UnknownHostException {
        super(PROTOCOL_NAME, PROTOCOL_ID /*, new BetterEventPriorityQueue()*/);
        this.workerGroup = workerGroup;

        amFrontedNode = false; //默认不是前链节点
        canHandleQequest = false;//相应的默认不能处理请求

        //端口号为50200 ：
        self = new Host(InetAddress.getByName(props.getProperty(ADDRESS_KEY)),
                Integer.parseInt(props.getProperty(DATA_PORT_KEY)));
        //不管是排序还是分发消息：标记下一个节点

        //nextOkFront = null;
        //nextOkBack = null;
        leader = null;
        //
        //nextOkFrontConnected = false;
        //nextOkBackConnected = false;
        //leaderConnected = false;


        //this.PEER_PORT = Integer.parseInt(props.getProperty(DATA_PORT_KEY));
        //this.CONSENSUS_PORT = Integer.parseInt(props.getProperty(CONSENSUS_PORT_KEY));

        this.RECONNECT_TIME = Integer.parseInt(props.getProperty(RECONNECT_TIME_KEY));
        this.NOOP_SEND_INTERVAL = Integer.parseInt(props.getProperty(NOOP_INTERVAL_KEY));

        this.QUORUM_SIZE = Integer.parseInt(props.getProperty(QUORUM_SIZE_KEY));
    }
    public TPOChainData(Properties props,short index, EventLoopGroup workerGroup) throws UnknownHostException {
        super(PROTOCOL_NAME+index, (short)(PROTOCOL_ID+index) /*, new BetterEventPriorityQueue()*/);
        this.workerGroup = workerGroup;
        
        this.index=index;
        
        amFrontedNode = false; //默认不是前链节点
        
        //端口号为50200到50203
        data_port=Integer.parseInt(props.getProperty(DATA_PORT_KEY))+index;
        self = new Host(InetAddress.getByName(props.getProperty(ADDRESS_KEY)),
                data_port);
        
        //不管是排序还是分发消息：标记下一个节点
        //nextOkFront = null;
        //nextOkBack = null;
        //nextOkFrontConnected = false;
        //nextOkBackConnected = false;

        nextok=null;
        leader = null;
        //leaderConnected = false;
        nextokConnected=false;
        canHandleQequest = false;//相应的默认不能处理请求
        
        
        //this.PEER_PORT = Integer.parseInt(props.getProperty(DATA_PORT_KEY));
        //this.CONSENSUS_PORT = Integer.parseInt(props.getProperty(CONSENSUS_PORT_KEY));

        this.RECONNECT_TIME = Integer.parseInt(props.getProperty(RECONNECT_TIME_KEY));
        this.NOOP_SEND_INTERVAL = Integer.parseInt(props.getProperty(NOOP_INTERVAL_KEY));
        this.QUORUM_SIZE = Integer.parseInt(props.getProperty(QUORUM_SIZE_KEY));


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

        

        //registerTimerHandler(ReconnectTimer.TIMER_ID,this::onReconnectTimer);
        registerTimerHandler(FlushMsgTimer.TIMER_ID, this::onFlushMsgTimer);
        //这里的Reconnect和控制层的Reconnect重了，需要单独设置一份重连的时钟
        registerTimerHandler(ReconnectDataTimer.TIMER_ID, this::onReconnectDataTimer);
        
        
        
        //接收从front的 写 请求
        registerRequestHandler(SubmitBatchRequest.REQUEST_ID, this::onSubmitBatch);

        
        //subscribeNotification(ThreadidamFrontNextFrontNextBackNotification.NOTIFICATION_ID, this::onThreadidamFrontNextFrontNextBackChange);
        subscribeNotification(LeaderNotification.NOTIFICATION_ID, this::onLeaderChange);
        subscribeNotification(FrontChainNotification.NOTIFICATION_ID, this::onFrontChainNotification);
        subscribeNotification(InitializeCompletedNotification.NOTIFICATION_ID, this::onInitializeCompletedNotification);
                
        // 注册通道事件
        registerChannelEventHandler(peerChannel, OutConnectionDown.EVENT_ID, this::onOutConnectionDown);
        registerChannelEventHandler(peerChannel, OutConnectionUp.EVENT_ID, this::onOutConnectionUp);
        registerChannelEventHandler(peerChannel, OutConnectionFailed.EVENT_ID, this::onOutConnectionFailed);
        registerChannelEventHandler(peerChannel, InConnectionDown.EVENT_ID, this::uponInConnectionDown);
        registerChannelEventHandler(peerChannel, InConnectionUp.EVENT_ID, this::uponInConnectionUp);
        
        
        logger.info("TPOChaindata"+index+"开启: ");
    }

    
    

    /**-----------------------------处理节点的分发信息-------------------------------**/
    
    // 在通用配置
    private InetAddress selfInetAddress;//=self.getAddress();
    private RuntimeConfigure selfRuntimeConfigure;//=hostConfigureMap.get(selfInetAddress);
    private  Map<Integer, InstanceState>  selfInstanceMap;//=instances.get(selfInetAddress);
    private  SeqN newterm;// = new SeqN(0, self);
    
    
    /**
     * 在当前节点是前链节点时处理，发送 或Noop 或App_Batch信息
     */
    private void sendNextAccept(PaxosValue val) {
        //设置上次的刷新时间：上次发送时间 
        lastSendTime = System.currentTimeMillis();
        
        //因为新消息会附带以往的ack消息,所以取消刷新的消息
        //cancelTimer(frontflushMsgTimer);

        // 先得到自己节点的配置信息
        //RuntimeConfigure hostSendConfigure = hostConfigureMap.get(self.getAddress());

        InstanceState instance;
        if (!selfInstanceMap.containsKey(selfRuntimeConfigure.lastAcceptSent + 1)) {
            instance=new InstanceState(selfRuntimeConfigure.lastAcceptSent + 1);
            selfInstanceMap.put(selfRuntimeConfigure.lastAcceptSent + 1, instance);
        }else {
            instance=selfInstanceMap.get(selfRuntimeConfigure.lastAcceptSent + 1);
        }
        
        
        //InstanceState instance = selfInstanceMap.computeIfAbsent(selfRuntimeConfigure.lastAcceptSent + 1, InstanceState::new);
        //assert instance.acceptedValue == null && instance.highestAccept == null;

        //PaxosValue nextValue = val;

        //对当前的生成自己的seqn,以线程通道id为count，self为标记
        
        //同时向leader发送排序请求:排序不发noop消息改成直接向全体节点发送ack设置一个时钟,什么时候开启,什么时候关闭
        OrderMSg orderMSg = new OrderMSg(self, instance.iN);
        sendOrEnqueue(orderMSg, leader);
        
        // 先发给自己:这里直接使用对应方法
        //this.uponAcceptMsg(new AcceptMsg(instance.iN, newterm,
        //        (short) 0, nextValue,hostSendConfigure.highestAcknowledgedInstance), self, this.getProtoId(), peerChannel);
        
        sendOrEnqueue(new AcceptMsg(instance.iN, newterm,
                (short) 0, val, selfRuntimeConfigure.highestAcknowledgedInstance), self);
        
        //设置发送标记数
        selfRuntimeConfigure.lastAcceptSent = instance.iN;
    }

    
    
    //private Host acceptNode;
    private InetAddress accpetNodeInetAddress;
    private  Map<Integer, InstanceState>  acceptInstanceMap;
    private  RuntimeConfigure  acceptRuntimeConfigure;
    
    /**
     * 处理accept信息
     */
    private void uponAcceptMsg(AcceptMsg msg, Host from, short sourceProto, int channel) {
        //对不在系统中的节点发送未定义消息让其重新加入系统
        //if (!membership.contains(from)) {
        //    logger.warn("Received msg from unaffiliated host " + from);
        //    sendMessage(new UnaffiliatedMsg(), from, TCPChannel.CONNECTION_IN);
        //    return;
        //}
        
        //if (logger.isDebugEnabled()) {
        //    logger.debug("接收到" + from + "的" + msg);
        //}
        
        // 得到分发消息的来源节点
        accpetNodeInetAddress = msg.sN.getNode().getAddress();
        acceptInstanceMap = instances.get(accpetNodeInetAddress);

        InstanceState instance;
        if (!acceptInstanceMap.containsKey(msg.iN)) {
            instance=new InstanceState(msg.iN);
            acceptInstanceMap.put(msg.iN, instance);
        }else {
            instance=acceptInstanceMap.get(msg.iN);
        }
        //InstanceState instance = acceptInstanceMap.computeIfAbsent(msg.iN, InstanceState::new);
        
        
        //"Discarding decided msg" 当instance
        //if (instance.isDecided()) {
        //    logger.warn("Discarding decided acceptmsg");
        //    return;
        //}

        //如果消息是新生成的那么,它的投票数为0,肯定不满足下面这个条件，若是重发的则直接满足条件进行丢弃
        if (msg.nodeCounter + 1 <= instance.counter) {
            logger.warn("Discarding 已经在局部表存在的acceptmsg");
            return;
        }
        
        
        // 先得到排序commandleader节点的配置信息
        acceptRuntimeConfigure = hostConfigureMap.get(accpetNodeInetAddress);

        //进行更新领导操作时间
        //hostSendConfigure.lastAcceptTime = System.currentTimeMillis();

        
        //进行对实例的确定
        instance.accept(msg.sN, msg.value, (short) (msg.nodeCounter + 1));


        //更新highestAcceptedInstance信息
        if (acceptRuntimeConfigure.highestAcceptedInstance < instance.iN) {
            acceptRuntimeConfigure.highestAcceptedInstance++;
            //assert hostConfigureMap.get(sender).highestAcceptedInstance == instance.iN;
        }

        //  先转发，还是先ack，应该携带的ack是消息中自带的ack，而不是自己的 
        forward(instance);//转发下一个节点
        
        
        // 前段节点没有decide ，后端节点已经decide，
        //if (!instance.isDecided() && instance.counter >= QUORUM_SIZE) //We have quorum!
        //    decideAndExecute(instance);//决定并执行实例
        if (msg.ack > acceptRuntimeConfigure.highestAcknowledgedInstance) {
            //对于之前的实例进行ack并进行垃圾收集
           // ackInstance(accpetNodeInetAddress, msg.ack);
            if (msg.ack < 0) {
                return;
            }
            //if (msg.ack <= acceptRuntimeConfigure.highestAcknowledgedInstance) {
            //    logger.info("Discarding 重复的 acceptackmsg;当前节点是"  + "当前的要确定的序号是" + instanceN + "当前的acceptack序号是" + hostSendConfigure.highestAcknowledgedInstance);
            //    return;
            //}
            acceptRuntimeConfigure.highestAcknowledgedInstance++;
        }
        
        

    }


    /**
     * 转发accept信息给下一个节点
     */
    private void forward(InstanceState inst) {
        Host sendHost = inst.highestAccept.getNode();
        //发送ack
        if (nextok.equals(Head)){
            AcceptAckMsg acceptAckMsgtemp = new AcceptAckMsg(sendHost, inst.iN);
            sendMessage(acceptAckMsgtemp, nextok);
        }else {//正常的转发
            //AcceptMsg msg = new AcceptMsg(inst.iN, inst.highestAccept, inst.counter, inst.acceptedValue,
            //        hostConfigureMap.get(sendHost.getAddress()).highestAcknowledgedInstance);
            AcceptMsg msg = new AcceptMsg(inst.iN, inst.highestAccept, inst.counter, inst.acceptedValue,
                    acceptRuntimeConfigure.highestAcknowledgedInstance);
            sendMessage(msg,nextok);
        }
        // TODO: 2023/6/15  只有前链的情况还没解除注释
        // TODO: 2023/5/29 如果分发消息的节点已经不在集群中，则对全部的节点发送对应消息的ack 
        // 这里需要修改  不能使用满足F+1 ，才能发往下一个节点，
        //  若一个节点故障，永远不会满足F+1,应该使用逻辑链前链是否走完
        //有两种情况属于链尾:一种是只有前链  一种是有后链


        // && membership.getFrontChainTail(inst.highestAccept.getNode()).equals(self)
        // 因为这个节点可能缺失
        //这是只有一种前链的情况
        //if(nextOkFront!=null && nextOkBack==null){
        //    Host  sendHost=inst.highestAccept.getNode();
        //    if (membership.isAlive(sendHost)){//节点存活
        //        if (nextOkFront.equals(sendHost)) {//发送ack信息
        //            if (inst.counter < QUORUM_SIZE) {
        //                logger.error("Last living in chain cannot decide. Are f+1 nodes dead/inRemoval? "
        //                        + inst.counter);
        //                throw new AssertionError("Last living in chain cannot decide. " +
        //                        "Are f+1 nodes dead/inRemoval? " + inst.counter);
        //            }
        //            //发送ack
        //            sendMessage(new AcceptAckMsg(inst.highestAccept.getNode(),inst.iN), inst.highestAccept.getNode());
        //            return;
        //        } else {//转发消息
        //            AcceptMsg msg = new AcceptMsg(inst.iN, inst.highestAccept, inst.counter, inst.acceptedValue,
        //                    hostConfigureMap.get(sendHost).highestAcknowledgedInstance);
        //            sendMessage(msg, nextOkFront);
        //            return;
        //        }
        //    }else {// 节点不存活
        //        if (inst.counter < QUORUM_SIZE) {
        //            AcceptMsg msg = new AcceptMsg(inst.iN, inst.highestAccept, inst.counter, inst.acceptedValue,
        //                    hostConfigureMap.get(sendHost).highestAcknowledgedInstance);
        //            sendMessage(msg, nextOkFront);
        //            return;
        //        }else{
        //            membership.getMembers().forEach(host -> sendMessage(new AcceptAckMsg(inst.highestAccept.getNode(),inst.iN),host)); 
        //            return;
        //        }
        //    }
        //}


        //下面是有后链的情况

        //这说明是有后链，而且此节点还是后链的尾节点
        //Host sendHost = inst.highestAccept.getNode();
        ////到达链尾发送acceptack信息
        //if (nextOkFront == null && nextOkBack == null) {
        //    if (inst.counter < QUORUM_SIZE) {
        //        logger.error("Last living in chain cannot decide. Are f+1 nodes dead/inRemoval? "
        //                + inst.counter);
        //        throw new AssertionError("Last living in chain cannot decide. " +
        //                "Are f+1 nodes dead/inRemoval? " + inst.counter);
        //    }
        //    // 下面已经说明投票数大于等于F+1
        //    int idsendtothread = inst.highestAccept.getCounter();
        //    //if (membership.isAlive(sendHost)) {
        //    AcceptAckMsg acceptAckMsgtemp = new AcceptAckMsg(sendHost, threadid, inst.iN);
        //    sendMessage(acceptAckMsgtemp, sendHost);
        //    if (logger.isDebugEnabled()) {
        //        logger.debug("后链末尾向" + sendHost + "发送" + acceptAckMsgtemp);
        //    }
        //
        //        return;
        //    //} else {
        //    //    AcceptAckMsg acceptAckMsgtemp = new AcceptAckMsg(sendHost, threadid, inst.iN);
        //    //    membership.getMembers().forEach(host -> sendMessage(acceptAckMsgtemp, host));
        //    //    if (logger.isDebugEnabled()) {
        //    //        logger.debug("后链末尾向全体成员" + membership.getMembers() + "发送" + acceptAckMsgtemp);
        //    //    }
        //    //    return;
        //    //}
        //}
        //
        //
        ////正常转发有两种情况： 在前链中转发  在后链中转发
        //AcceptMsg msg = new AcceptMsg(inst.iN, inst.highestAccept, inst.counter, inst.acceptedValue,
        //        hostConfigureMap.get(sendHost.getAddress()).highestAcknowledgedInstance, threadid);
        //if (nextOkFront != null) {//此节点是前链
        //    //membership.get
        //    if (inst.counter < QUORUM_SIZE) {// 投票数不满F+1，发往nextOkFront
        //        sendMessage(msg, nextOkFront);
        //        if (logger.isDebugEnabled()) {
        //            logger.debug("此节点是前链,现在向前链" + nextOkFront + "转发" + msg);
        //        }
        //    } else { //当等于inst.count==F+1 ,可以发往后链节点
        //        sendMessage(msg, nextOkBack);
        //        if (logger.isDebugEnabled()) {
        //            logger.debug("此节点是前链,现在向后链" + nextOkBack + "转发" + msg);
        //        }
        //    }
        //} else {//此节点是后链
        //    sendMessage(msg, nextOkBack);
        //    if (logger.isDebugEnabled()) {
        //        logger.debug("此节点是后链,现在向后链" + nextOkBack + "转发" + msg);
        //    }
        //}

        //显示现在关于各项数据已经存储完毕标记
        hostReceive.get(accpetNodeInetAddress).set(inst.iN);
    }
    
    
    
    /**
     * leader接收ack信息，对实例进行ack
     */
    private void uponAcceptAckMsg(AcceptAckMsg msg, Host from, short sourceProto, int channel) {
        //除了发送节点可以接受acceptack信息,其他节点也可以接受acceptack信息,因为
        // 发送节点可能宕机,所以开放其他节点的接受ack信息
        if (logger.isDebugEnabled()) {
            logger.debug("接收" + from + "的:" + msg);
        }

        Host ackHost = msg.node;
        RuntimeConfigure ackHostRuntimeConfigure = hostConfigureMap.get(ackHost.getAddress());
        
        if (msg.instanceNumber <= ackHostRuntimeConfigure.highestAcknowledgedInstance) {
            logger.warn("Ignoring acceptAck for old instance: " + msg);
            return;
        }
        // 执行ack程序
        //ackInstance(msg.node.getAddress(), msg.instanceNumber);
        if ( msg.instanceNumber < 0) {
            return;
        }
        ackHostRuntimeConfigure.highestAcknowledgedInstance++;
        lastReceiveAckTime=System.currentTimeMillis();
        //这里设置一个定时器,发送acceptack,一段时间没有后续要发的 
        // 在发送新实例时在sendnextaccpt取消
        //如果当前节点等于消息的发送节点,
        frontflushMsgTimer=setupTimer(FlushMsgTimer.instance,NOOP_SEND_INTERVAL);
    }
    
    
    
    /**
     * 对最后的此节点的消息进行发送ack信息  FlushMsgTimer
     */
    private void onFlushMsgTimer(FlushMsgTimer timer, long timerId) {
        if (lastReceiveAckTime-lastSendTime>NOOP_SEND_INTERVAL){
            membership.stream().filter(h -> !h.equals(self)).forEach(host -> sendMessage(new AcceptAckMsg(self, selfRuntimeConfigure.highestAcknowledgedInstance), host));
            if (logger.isDebugEnabled()) {
                logger.debug("向所有节点发送了acceptack为" + hostConfigureMap.get(self).highestAcknowledgedInstance + "的定时信息");
            }  
        }
        //if (selfRuntimeConfigure.highestAcknowledgedInstance==selfRuntimeConfigure.lastAcceptSent) {
        //    membership.stream().filter(h -> !h.equals(self)).forEach(host -> sendMessage(new AcceptAckMsg(self, selfRuntimeConfigure.highestAcknowledgedInstance), host));
        //    if (logger.isDebugEnabled()) {
        //        logger.debug("向所有节点发送了acceptack为" + hostConfigureMap.get(self).highestAcknowledgedInstance + "的定时信息");
        //    }
        //    //发完acceptack消息之后应该结束时钟
        //    //cancelTimer(frontflushMsgTimer);
        //}
        //if (amFrontedNode) {
        //    if (System.currentTimeMillis() - lastSendTime > NOOP_SEND_INTERVAL) {
        //        membership.stream().filter(h -> !h.equals(self)).forEach(host -> sendMessage(new AcceptAckMsg(self, threadid, hostConfigureMap.get(self.getAddress()).highestAcknowledgedInstance), host));
        //        if (logger.isDebugEnabled()) {
        //            logger.debug("向所有节点发送了acceptack为" + hostConfigureMap.get(self).highestAcknowledgedInstance + "的定时信息");
        //        }
        //        //发完acceptack消息之后应该结束时钟
        //        cancelTimer(frontflushMsgTimer);
        //    }
        //} else {
        //    logger.warn(timer + " while not FrontedChain");
        //    cancelTimer(frontflushMsgTimer);
        //}
    }

    
    
    
    // -----------------废弃
    
    /**
     * 对于ack包括以前的消息执行
     */
    private void ackInstance(RuntimeConfigure hostSendConfigure, int instanceN) {
        //初始时条件时instanceN为-1
        if (instanceN < 0) {
            return;
        }
        // 先得到排序commandleader节点的配置信息
        //RuntimeConfigure hostSendConfigure = hostConfigureMap.get(sendHost);
        //处理重复消息或过时
        if (instanceN <= hostSendConfigure.highestAcknowledgedInstance) {
            logger.info("Discarding 重复的 acceptackmsg;当前节点是"  + "当前的要确定的序号是" + instanceN + "当前的acceptack序号是" + hostSendConfigure.highestAcknowledgedInstance);
            return;
        }
        hostSendConfigure.highestAcknowledgedInstance++;
        // 对于前链节点先进行deccide
        //For nodes in the first half of the chain only
        //for (int i = hostSendConfigure.highestDecidedInstance + 1; i <= instanceN; i++) {
        //    InstanceState ins = instances.get(sendHost).get(i);
        //    if (ins!=null){
        //        //assert !ins.isDecided();
        //        decideAndExecute(hostSendConfigure,ins);
        //        //assert highestDecidedInstanceCl == i;
        //    }
        //}
    }


    /**
     * decide并执行Execute实例
     */
    private void decideAndExecute(RuntimeConfigure hostSendConfigure, InstanceState instance) {
        instance.markDecided();
        hostSendConfigure.highestDecidedInstance++;
    }


    



    /**---------------------------接收来自front层的分发消息 ---------**/

    /**
     * 当前时leader或正在竞选leader的情况下处理frontend的提交batch
     */
    public void onSubmitBatch(SubmitBatchRequest not, short from) {
        //sendNextAccept(new AppOpBatch(not.getBatch()));
        
        
        // TODO: 2023/6/24  原来是am 
        if (amFrontedNode) {// 改为前链节点  原来是amqurom
            if (canHandleQequest) {//这个标志意味着leader存在
                //先将以往的消息转发
                // 还有其他候选者节点存储的消息
                if (logger.isDebugEnabled()) {
                    logger.debug("接收来自front层的批处理并开始处理sendNextAccept():" + not.getBatch());
                }
                // fixme: 2023/6/20 在 canHandleQequest变为true时将暂存的消息开始处理
                sendNextAccept(new AppOpBatch(not.getBatch()));
            } else {// leader不存在，无法排序
                if (logger.isDebugEnabled()) {
                    logger.debug("因为现在还没有leader,缓存来自front的批处理:" + not.getBatch());
                }
                waitingAppOps.add(new AppOpBatch(not.getBatch()));
            }
        } else //忽视接受的消息
            logger.warn("Received " + not + " without being FrontedNode, ignoring.");
        // TODO: 2023/6/8 需要处理暂存的消息 
        //PaxosValue nextOp;
        //if (!waitingAppOps.isEmpty()){
        //    while ((nextOp = waitingAppOps.poll()) != null) {
        //        sendNextAccept(nextOp);
        //    }
        //}
    }
    // TODO: 2023/6/20 将发送命令改为发送 对应的data层还是直接发到控制层


    
    
    
    
    /**------------------------------接收来自控制层的成员管理消息 ---------**/

    // TODO: 2023/6/19 以后关于成员更改要重新考虑，现在不做考虑：程序一些bug在之后再考虑 在leader改变时
    protected void  onLeaderChange(LeaderNotification notification, short emitterId){
        Host notleader=notification.getLeader();
        //Writes to changed
        if (leader == null || !notleader.equals(leader)) {
            //Close old writesTo
            if (leader != null && !leader.getAddress().equals(self.getAddress())) {
                canHandleQequest = false;
                closeConnection(leader, peerChannel);
            }
            //Update and open to new writesTo
            leader =notleader;
            logger.info("New leader connecting: " + leader.getAddress());
            if (!leader.getAddress().equals(self.getAddress()))
                openConnection(leader, peerChannel);
        }
        ////是否能进行开始处理操作 
        //this.canHandleQequest=notification.isCanHandleRequest();
    }
    
    protected  void onFrontChainNotification(FrontChainNotification notification,short emitterID){
        //logger.info("接收来的通知是"+notification.getFrontChain()+notification.getBackchain());
        frontChain=notification.getFrontChain();
        membership.clear();
        int size=frontChain.size();
        int loopindex=index;
        while(true){
            membership.add(new Host(frontChain.get(loopindex),data_port));
            loopindex=(loopindex+1)%size;
            if (membership.size()==size){
                break;
            }
        }
        //logger.info("输出前链"+membership);
        
        //判断若前链中包括自己，那么设置一个标记
        if (membership.contains(self)){
            amFrontedNode=true;
        }
        //得到链首
        Head=membership.get(0);
        
        //现在是前链
        // TODO: 2023/6/26 刷新问题 
        //if (membership.contains(self)){
        //    frontflushMsgTimer = setupPeriodicTimer(FlushMsgTimer.instance, 5000,1000);
        //}else {
        //    cancelTimer(frontflushMsgTimer);
        //}
        
        backChain=notification.getBackchain();
        for (InetAddress tmp:backChain) {
            membership.add(new Host(tmp,data_port));
        }
        //logger.info("输出总链"+membership);
        
        
        int memsize=membership.size();
        int selfindex=membership.indexOf(self);
        Host newnextok=membership.get((selfindex+1)%memsize);
        if (nextok==null){
            nextok=newnextok;
            openConnection(nextok);
            return;
        }
        if (nextok!=null && !newnextok.equals(nextok)){
            closeConnection(nextok);
            nextokConnected=false;
            nextok=newnextok;
            openConnection(nextok);
        }
    }

    protected  void onInitializeCompletedNotification(InitializeCompletedNotification notification,short emitterID){
        selfInetAddress=self.getAddress();
        selfRuntimeConfigure=hostConfigureMap.get(selfInetAddress);
        selfInstanceMap=instances.get(selfInetAddress);
        newterm = new SeqN(0, self);
    }
    
    
    
    // 在包含 线程号 是否为前链标志  nextokFront nextOkBack
    protected  void  onThreadidamFrontNextFrontNextBackChange(ThreadidamFrontNextFrontNextBackNotification notification, short emitterId){
        //logger.info(notification);
        //
        //this.threadid=notification.getThreadid();
        //this.amFrontedNode=notification.isAmFront();
        //
        //
        //InetAddress  notNextFront=notification.getNextOkFront();
        //nextOkFront =new Host(notNextFront,data_port);
        //if (nextOkFront!=null)
        //    openConnection(nextOkFront, peerChannel);
        //
        //InetAddress  notNextBack=notification.getNextOkBack();
        //nextOkBack=new Host(notNextBack,data_port);   ;
        //if (nextOkBack!=null)
        //    openConnection(nextOkBack, peerChannel);
        //
        //if (nextOkFront==null && nextOkBack==null){
        //    
        //    //frontChain.forEach();
        //}
        //if (nextOkFront == null || !notNextFront.equals(nextOkFront)) {
        //    //Close old writesTo
        //    if (nextOkFront != null && !nextOkFront.getAddress().equals(self.getAddress())) {
        //        nextOkFrontConnected = false;
        //        closeConnection(nextOkFront, peerChannel);
        //    }
        //    //Update and open to new writesTo
        //    nextOkFront =notNextFront;
        //    logger.info("New nextOkFront: " + nextOkFront);
        //    if (nextOkFront!=null){
        //        openConnection(nextOkFront, peerChannel);
        //    }
        //}
        //
        //Host  notNextBack=notification.getNextOkBack();
        //if (nextOkBack == null || !notNextFront.equals(nextOkBack)) {
        //    //Close old writesTo
        //    if (nextOkBack != null && !notNextBack.getAddress().equals(self.getAddress())) {
        //        nextOkFrontConnected = false;
        //        closeConnection(nextOkBack, peerChannel);
        //    }
        //    //Update and open to new writesTo
        //    nextOkBack =notNextBack;
        //    logger.info("New nextOBack: " + nextOkBack);
        //    if (nextOkBack!=null){
        //        openConnection(nextOkBack, peerChannel);
        //    }
        //}
    }




    

    /**
     * 消息Failed，发出logger.warn("Failed:)
     */
    private void uponMessageFailed(ProtoMessage msg, Host host, short i, Throwable throwable, int i1) {
        logger.warn("Failed: " + msg + ", to: " + host + ", reason: " + throwable.getMessage());
    }


    //data层连接到 leader节点 nextokback nextokfront节点
    /**
     * 在TCP连接writesTO节点时，将pendingWrites逐条发送到下一个节点
     */
    protected void onOutConnectionUp(OutConnectionUp event, int channel) {
        Host peer = event.getNode();
        if (peer.equals(leader)) {
            //leaderConnected = true;
            canHandleQequest = true;
            if (!waitingAppOps.isEmpty()){//
                AppOpBatch appOpBatch = waitingAppOps.poll();
                // 处理 appOpBatch，可以对其进行操作或输出
                sendNextAccept(appOpBatch);
            }
            //这里将暂存的消息从暂存队列中取出来开始处理
            logger.info("already Connected to leader :"+leader.getAddress());
        }
        if (peer.equals(nextok)){
            nextokConnected=true;
            logger.info("already Connected to nextok :"+nextok.getAddress());
        }
        //if (peer.equals(nextOkFront)) {
        //    nextOkFrontConnected = true;
        //    logger.info("Connected to nextOkFront: " + event);
        //    return;
        //}
        //if (peer.equals(nextOkBack)) {
        //    nextOkBackConnected = true;
        //    logger.info("Connected to nextOkBack:" + event);
        //}
    }


    /**
     * 在与下一个节点断开后开始重连
     */
    protected void onOutConnectionDown(OutConnectionDown event, int channel) {
        Host peer = event.getNode();
        if (peer.equals(leader)) {
            logger.warn("Lost connection to leader, re-connecting in 2: " + event);
            //leaderConnected = false;
            setupTimer(new ReconnectDataTimer(leader), 2000);
            return;
        }
        if (peer.equals(nextok)){
            logger.warn("Lost connection to nextok, re-connecting in 2: " + event);
            //leaderConnected = false;
            setupTimer(new ReconnectDataTimer(nextok), 2000);
        }
        //if (peer.equals(nextOkFront)) {
        //    logger.warn("Lost connection to nextOkFront, re-connecting in 5: " + event);
        //    leaderConnected = false;
        //    setupTimer(new ReconnectDataTimer(nextOkFront), 5000);
        //    return;
        //}
        //if (peer.equals(nextOkBack)) {
        //    logger.warn("Lost connection to nextOkBack, re-connecting in 5: " + event);
        //    leaderConnected = false;
        //    setupTimer(new ReconnectDataTimer(nextOkBack), 5000);
        //}
    }

    
    protected void onOutConnectionFailed(OutConnectionFailed<Void> event, int channel) {
        Host peer = event.getNode();
        //setupTimer(new ReconnectDataTimer(peer), 3000);
        if (peer.equals(leader)) {
            logger.warn("Connection failed to leader, re-trying in 2: " + event);
            //leaderConnected = false;
            setupTimer(new ReconnectDataTimer(leader), 2000);
            return;
        }
        if (peer.equals(nextok)){
            logger.warn("Connection failed to nextok, re-trying in 2: " + event);
            //leaderConnected = false;
            setupTimer(new ReconnectDataTimer(nextok), 2000);
        }
        //if (peer.equals(leader)) {
        //    logger.warn("Connection failed to leader, re-trying in 5: " + event);
        //    setupTimer(new ReconnectTimer(leader), 1000);
        //    return;
        //}
        //if (peer.equals(nextOkFront)) {
        //    logger.warn("Connection failed to nextOkFront, re-trying in 1: " + event);
        //    setupTimer(new ReconnectTimer(nextOkFront), 1000);
        //    return;
        //}
        //if (peer.equals(nextOkBack)) {
        //    logger.warn("Connection failed to writesTo, re-trying in 1: " + event);
        //    setupTimer(new ReconnectTimer(nextOkBack), 1000);
        //}
    }

    /**
     * 与 相应的尝试重新建立连接
     */
    private void onReconnectDataTimer(ReconnectDataTimer timer, long timerId) {
        logger.info("time is out,reconnect to:"+timer.getHost());
        openConnection(timer.getHost());
        //if (timer.getHost().equals(leader)) {
        //    logger.info("Trying to reconnect to leader " + timer.getHost());
        //    openConnection(leader);
        //    return;
        //}
        //if (timer.getHost().equals(nextOkFront)) {
        //    logger.info("Trying to reconnect to nextOkFront " + timer.getHost());
        //    openConnection(nextOkFront);
        //    return;
        //}
        //if (timer.getHost().equals(nextOkBack)) {
        //    logger.info("Trying to reconnect to nextOkBack " + timer.getHost());
        //    openConnection(nextOkBack);
        //}
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
    
    private  void  onReconnectTimer(ReconnectTimer timer,long timerId){
        logger.info("时钟到了，重新连接");
        openConnection(timer.getHost());
    }

    
    
    

    /**
     * 发送消息给自己和其他主机 也就是accept和ack信息
     */
    void sendOrEnqueue(ProtoMessage msg, Host destination) {
        if (msg == null || destination == null) {
            logger.error("null: " + msg + " " + destination);
            return;
        }
        // 满足下方条件只会是排序消息
        if (leader!=null && destination.equals(leader)) {
            if (leader.getAddress().equals(self.getAddress())){
                sendRequest(new SubmitOrderMsg((OrderMSg) msg), TPOChainProto.PROTOCOL_ID); 
            }else {
                sendMessage(msg, TPOChainProto.PROTOCOL_ID,destination);
            }
            return;
        }
        // 对于accept和ack信息,一般来说就是前链节点在接收自己处理的accept信息
        if (destination.equals(self)) {//只有在收到front层的消息。处理时需要这个
            deliverMessageIn(new MessageInEvent(new BabelMessage(msg, (short) -1, (short) -1), self, peerChannel));
        } else {
            sendMessage(msg, destination);
        }
    }
}    
