package TPOChain;

import TPOChain.messages.AcceptAckMsg;
import TPOChain.messages.AcceptMsg;
import TPOChain.messages.OrderMSg;
import TPOChain.messages.UnaffiliatedMsg;
import TPOChain.timers.FlushMsgTimer;
import TPOChain.utils.InstanceState;
import TPOChain.utils.RuntimeConfigure;
import TPOChain.utils.SeqN;
import TPOChain.utils.ShareDistrubutedInstances;
import chainpaxos.timers.ReconnectTimer;
import common.values.AppOpBatch;
import common.values.PaxosValue;
import frontend.ipc.SubmitBatchRequest;
import frontend.network.PeerBatchMessage;
import frontend.notifications.MembershipChange;
import io.netty.channel.EventLoopGroup;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pt.unl.fct.di.novasys.babel.core.Babel;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel;
import pt.unl.fct.di.novasys.channel.tcp.events.OutConnectionDown;
import pt.unl.fct.di.novasys.channel.tcp.events.OutConnectionFailed;
import pt.unl.fct.di.novasys.channel.tcp.events.OutConnectionUp;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;



public class TPOChainData extends GenericProtocol  implements ShareDistrubutedInstances {
    
    private static final Logger logger = LogManager.getLogger(TPOChainData.class);

    public final static short PROTOCOL_ID = 300;
    public final static String PROTOCOL_NAME = "TPOChainData";

    
    public static final String ADDRESS_KEY = "consensus_address";
    public static final String PORT_KEY = "data_port";


    public static final String RECONNECT_TIME_KEY = "reconnect_time";
    public static final String NOOP_INTERVAL_KEY = "noop_interval";



    private final int NOOP_SEND_INTERVAL;
    private final int RECONNECT_TIME;
    

    // 系统中的成员
    protected List<InetAddress> membership; 
    
    
    //前链连接和后链连接
    private Host  self;  
    private Host nextOkFront;
    private boolean nextOkFrontConnected;
    private Host nextOkBack;
    private boolean nextOkBackConnected;

    //此协议使用哪种通道的线程来处理
    private  short  threadid;
    
    
    
    //还有继承接口的局部日志表 
    
    
    /**
     * 对节点的一些配置信息，主要是各前链节点分发的实例信息
     * 和 接收到accptcl的数量
     * */
    private  Map<Host,RuntimeConfigure>  hostConfigureMap=new HashMap<>();
    
    
    

    
    /**
 * 标记是否为前段节点，代表者可以发送command，并向leader发送排序
 * */
    private boolean amFrontedNode;

    //标记前链节点能否开始处理客户端的请求
    private boolean canHandleQequest=false;
    
    
    
    //在commandleader发送第一条命令的时候开启闹钟，
    // 在第一次ack和  accept与send  相等时，关闹钟，刚来时
    private long frontflushMsgTimer = -1;
    
    //主要是前链什么时候发送flushMsg信息
    private long lastSendTime;

    // 还有一个field ： System.currentTimeMillis(); 隐含了系统的当前时间



    
    private int peerChannel;

    
    /**
     * java中net框架所需要的数据结构
     * */
    private EventLoopGroup workerGroup;
    
    
    public TPOChainData(Properties props, EventLoopGroup workerGroup) throws UnknownHostException {
        super(PROTOCOL_NAME, PROTOCOL_ID /*, new BetterEventPriorityQueue()*/);
        this.workerGroup = workerGroup;
        
        amFrontedNode=false; //默认不是前链节点
        canHandleQequest=false;//相应的默认不能处理请求
        
        //端口号为50600
        self = new Host(InetAddress.getByName(props.getProperty(ADDRESS_KEY)),
                Integer.parseInt(props.getProperty(PORT_KEY)));
        //不管是排序还是分发消息：标记下一个节点
        nextOkFront =null;
        nextOkBack=null;
        
        
        
        this.RECONNECT_TIME = Integer.parseInt(props.getProperty(RECONNECT_TIME_KEY));
        this.NOOP_SEND_INTERVAL = Integer.parseInt(props.getProperty(NOOP_INTERVAL_KEY));
    }

    
    public void init(Properties props) throws HandlerRegistrationException, IOException {
        Properties peerProps = new Properties();
        peerProps.put(TCPChannel.ADDRESS_KEY, props.getProperty(ADDRESS_KEY));
        peerProps.setProperty(TCPChannel.PORT_KEY, props.getProperty(PORT_KEY));
        peerProps.put(TCPChannel.WORKER_GROUP_KEY, workerGroup);
        peerChannel = createChannel(TCPChannel.NAME, peerProps);
        setDefaultChannel(peerChannel);

        
    }


    
    


    


    //Notice front只连接到 writeto即 leader节点，对于其他节点不连接

    /**
     * 在TCP连接writesTO节点时，将pendingWrites逐条发送到下一个节点
     * */
    protected void onOutConnectionUp(OutConnectionUp event, int channel) {
        Host peer = event.getNode();
        if (peer.equals(writesTo)) {
            writesToConnected = true;
            logger.debug("Connected to writesTo " + event);
            /*
             * pendingWrites是Queue<Pair<Long, OpBatch>>
             * */
            pendingWrites.forEach(b -> sendBatchToWritesTo(new PeerBatchMessage(b.getRight())));
        } else {
            logger.warn("Unexpected connectionUp, ignoring and closing: " + event);
            closeConnection(peer, peerChannel);
        }
    }

    /**
     * 在与下一个节点断开后开始重连
     * */
    protected void onOutConnectionDown(OutConnectionDown event, int channel) {
        Host peer = event.getNode();
        if (peer.equals(writesTo)) {
            logger.warn("Lost connection to writesTo, re-connecting in 5: " + event);
            writesToConnected = false;
            setupTimer(new ReconnectTimer(writesTo), 5000);
        }
    }

    protected void onOutConnectionFailed(OutConnectionFailed<Void> event, int channel) {
        logger.info(event);
        Host peer = event.getNode();
        if (peer.equals(writesTo)) {
            logger.warn("Connection failed to writesTo, re-trying in 5: " + event);
            setupTimer(new ReconnectTimer(writesTo), 5000);
        } else {
            logger.warn("Unexpected connectionFailed, ignoring: " + event);
        }
    }
    
    /**
     * 与 writesTo尝试重新建立连接
     * */
    private void onReconnectTimer(ReconnectTimer timer, long timerId) {
        if (timer.getHost().equals(writesTo)) {
            logger.info("Trying to reconnect to writesTo " + timer.getHost());
            openConnection(timer.getHost());
        }
        
    }



    
    
    
    


    /**-----------------------------处理节点的分发信息-------------------------------**/


    /**
     *在当前节点是前链节点时处理，发送 或Noop 或App_Batch信息
     * */
    private void sendNextAccept(PaxosValue val) {
        //设置上次的刷新时间：上次发送时间 
        lastSendTime = System.currentTimeMillis();

        //因为新消息会附带以往的ack消息,所以取消刷新的消息
        cancelTimer(frontflushMsgTimer);

        // 先得到自己节点的配置信息
        RuntimeConfigure hostSendConfigure= hostConfigureMap.get(self);

        InstanceState instance = instances.get(self).computeIfAbsent(hostSendConfigure.lastAcceptSent+1, InstanceState::new);
        //assert instance.acceptedValue == null && instance.highestAccept == null;

        PaxosValue nextValue=val;

        //对当前的生成自己的seqn,以线程通道id为count，self为标记
        SeqN newterm=new SeqN(threadid,self);


        //同时向leader发送排序请求:排序不发noop消息改成直接向全体节点发送ack设置一个时钟,什么时候开启,什么时候关闭
        OrderMSg orderMSg=new OrderMSg(self,instance.iN);
        sendOrEnqueue(orderMSg,supportedLeader());


        // 先发给自己:这里直接使用对应方法
        //this.uponAcceptMsg(new AcceptMsg(instance.iN, newterm,
        //        (short) 0, nextValue,hostSendConfigure.highestAcknowledgedInstance), self, this.getProtoId(), peerChannel);


        sendOrEnqueue(new AcceptMsg(instance.iN, newterm,
                (short) 0, nextValue,hostSendConfigure.highestAcknowledgedInstance,threadid),self);
        //设置发送标记数
        hostSendConfigure.lastAcceptSent = instance.iN;
    }


    /**
     * 处理accept信息
     */
    private void uponAcceptMsg(AcceptMsg msg, Host from, short sourceProto, int channel) {
        //对不在系统中的节点发送未定义消息让其重新加入系统
        if(!membership.contains(from)){
            logger.warn("Received msg from unaffiliated host " + from);
            sendMessage(new UnaffiliatedMsg(), from, TCPChannel.CONNECTION_IN);
            return;
        }


        if (logger.isDebugEnabled()){
            logger.debug("接收到"+from+"的"+msg);
        }



        // 得到分发消息的来源节点
        Host sender=msg.sN.getNode();
        Map<Integer, InstanceState> sendInstanceMap=instances.get(sender);
        InstanceState instance = sendInstanceMap.computeIfAbsent(msg.iN, InstanceState::new);


        //"Discarding decided msg" 当instance
        //if (instance.isDecided()) {
        //    logger.warn("Discarding decided acceptmsg");
        //    return;
        //}

        //如果消息是新生成的那么,它的投票数为0,肯定不满足下面这个条件，若是重发的则直接满足条件进行丢弃
        if (msg.nodeCounter+1<=instance.counter){
            logger.warn("Discarding 已经在局部表存在的acceptmsg");
            return;
        }


        // 先得到排序commandleader节点的配置信息
        RuntimeConfigure hostSendConfigure= hostConfigureMap.get(sender);

        //进行更新领导操作时间
        hostSendConfigure.lastAcceptTime = System.currentTimeMillis();

        //进行对实例的确定
        instance.accept(msg.sN, msg.value, (short) (msg.nodeCounter + 1));


        //更新highestAcceptedInstance信息
        if (hostSendConfigure.highestAcceptedInstance < instance.iN) {
            hostSendConfigure.highestAcceptedInstance++;
            assert hostConfigureMap.get(sender).highestAcceptedInstance == instance.iN;
        }

        //  先转发，还是先ack，应该携带的ack是消息中自带的ack，而不是自己的 
        forward(instance,msg.threadid);//转发下一个节点


        // 前段节点没有decide ，后端节点已经decide，
        //if (!instance.isDecided() && instance.counter >= QUORUM_SIZE) //We have quorum!
        //    decideAndExecute(instance);//决定并执行实例

        if (msg.ack>hostSendConfigure.highestAcknowledgedInstance){
            //对于之前的实例进行ack并进行垃圾收集
            ackInstance(sender,msg.ack);
        }

        if (sender.equals(lacknode) && lackid==msg.iN){
            synchronized (executeLock){
                execute();
            }
        }
    }


    /**
     * 转发accept信息给下一个节点
     * */
    private void forward(InstanceState inst,short threadid) {
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
        Host sendHost=inst.highestAccept.getNode();
        //到达链尾发送acceptack信息
        if (nextOkFront==null && nextOkBack==null) {
            if (inst.counter < QUORUM_SIZE) {
                logger.error("Last living in chain cannot decide. Are f+1 nodes dead/inRemoval? "
                        + inst.counter);
                throw new AssertionError("Last living in chain cannot decide. " +
                        "Are f+1 nodes dead/inRemoval? " + inst.counter);
            }
            // 下面已经说明投票数大于等于F+1
            int  idsendtothread=inst.highestAccept.getCounter();
            if (membership.isAlive(sendHost)){
                AcceptAckMsg acceptAckMsgtemp=new AcceptAckMsg(sendHost,threadid,inst.iN);
                sendMessage(acceptAckMsgtemp,sendHost);
                if (logger.isDebugEnabled()){
                    logger.debug("后链末尾向"+sendHost+"发送"+acceptAckMsgtemp);
                }

                return;
            }else {
                AcceptAckMsg acceptAckMsgtemp=new AcceptAckMsg(sendHost,threadid,inst.iN);
                membership.getMembers().forEach(host -> sendMessage(acceptAckMsgtemp,host));
                if (logger.isDebugEnabled()){
                    logger.debug("后链末尾向全体成员"+membership.getMembers()+"发送"+acceptAckMsgtemp);
                }
                return;
            }
        }


        //正常转发有两种情况： 在前链中转发  在后链中转发
        AcceptMsg msg = new AcceptMsg(inst.iN, inst.highestAccept, inst.counter, inst.acceptedValue,
                hostConfigureMap.get(sendHost).highestAcknowledgedInstance,threadid);
        if (nextOkFront!=null){//此节点是前链
            //membership.get
            if (inst.counter < QUORUM_SIZE   ){// 投票数不满F+1，发往nextOkFront
                sendMessage(msg, nextOkFront);
                if (logger.isDebugEnabled()){
                    logger.debug("此节点是前链,现在向前链"+nextOkFront+"转发"+msg);
                }
            } else{ //当等于inst.count==F+1 ,可以发往后链节点
                sendMessage(msg, nextOkBack);
                if (logger.isDebugEnabled()){
                    logger.debug("此节点是前链,现在向后链"+nextOkBack+"转发"+msg);
                }
            }
        } else {//此节点是后链
            sendMessage(msg, nextOkBack);
            if (logger.isDebugEnabled()){
                logger.debug("此节点是后链,现在向后链"+nextOkBack+"转发"+msg);
            }
        }
    }


    /**
     * 对于ack包括以前的消息执行
     * */
    private void ackInstance(Host sendHost,int instanceN) {
        //初始时条件时instanceN为-1
        if (instanceN<0){
            return;
        }

        // 先得到排序commandleader节点的配置信息
        RuntimeConfigure  hostSendConfigure= hostConfigureMap.get(sendHost);

        //处理重复消息或过时
        if (instanceN<=hostSendConfigure.highestAcknowledgedInstance){
            logger.info("Discarding 重复的 acceptackmsg;当前节点是"+sendHost +"当前的要确定的序号是"+instanceN+"当前的acceptack序号是"+hostSendConfigure.highestAcknowledgedInstance);
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
     * */
    private void decideAndExecute(RuntimeConfigure  hostSendConfigure,InstanceState instance) {
        instance.markDecided();
        hostSendConfigure.highestDecidedInstance++;
    }


    /**
     * leader接收ack信息，对实例进行ack
     * */
    private void uponAcceptAckMsg(AcceptAckMsg msg, Host from, short sourceProto, int channel) {
        //除了发送节点可以接受acceptack信息,其他节点也可以接受acceptack信息,因为
        // 发送节点可能宕机,所以开放其他节点的接受ack信息
        if (logger.isDebugEnabled()){
            logger.debug("接收" + from + "的:"+msg);
        }

        Host ackHost=msg.node;
        RuntimeConfigure ackHostRuntimeConfigure= hostConfigureMap.get(ackHost);
        if (msg.instanceNumber<=ackHostRuntimeConfigure.highestAcknowledgedInstance){
            logger.warn("Ignoring acceptAck for old instance: "+msg);
            return;
        }

        // 执行ack程序
        ackInstance(msg.node,msg.instanceNumber);


        //这里设置一个定时器,发送acceptack,一段时间没有后续要发的 
        // 在发送新实例时在sendnextaccpt取消
        //如果当前节点等于消息的发送节点,
        if (self.equals(ackHost)){
            frontflushMsgTimer =  setupTimer(FlushMsgTimer.instance, NOOP_SEND_INTERVAL);
            if (logger.isDebugEnabled()){
                logger.debug("因为当前节点是发送节点所以开启定时刷新时钟");
            }
            lastSendTime = System.currentTimeMillis();
        }
    }


    /**
     * 对最后的此节点的消息进行发送ack信息  FlushMsgTimer
     * */
    private void onFlushMsgTimer(FlushMsgTimer timer, long timerId) {
        if (amFrontedNode) {
            if (System.currentTimeMillis() - lastSendTime > NOOP_SEND_INTERVAL){
                membership.getMembers().stream().filter(h -> !h.equals(self)).forEach(host -> sendMessage(new AcceptAckMsg(self,threadid,hostConfigureMap.get(self).highestAcknowledgedInstance),host));
                if (logger.isDebugEnabled()){
                    logger.debug("向所有节点发送了acceptack为"+hostConfigureMap.get(self).highestAcknowledgedInstance+"的定时信息");
                }
                //发完acceptack消息之后应该结束时钟
                cancelTimer(frontflushMsgTimer);
            }
        } else {
            logger.warn(timer + " while not FrontedChain");
            cancelTimer(frontflushMsgTimer);
        }
    }

    
    
    
    
    
    /**---------------------------接收来自front层的分发消息 ---------**/
    
    /**
     * 当前时leader或正在竞选leader的情况下处理frontend的提交batch
     */
    public void onSubmitBatch(SubmitBatchRequest not, short from) {
        if (amFrontedNode) {// 改为前链节点  原来是amqurom
            if (canHandleQequest){//这个标志意味着leader存在
                //先将以往的消息转发
                // 还有其他候选者节点存储的消息
                if (logger.isDebugEnabled()){
                    logger.debug("接收来自front层的批处理并开始处理sendNextAccept():"+not.getBatch());
                }
                sendNextAccept(new AppOpBatch(not.getBatch()));
            }else {// leader不存在，无法排序
                if (logger.isDebugEnabled()){
                    logger.debug("因为现在还没有leader,缓存来自front的批处理:"+not.getBatch());
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





    /* -------------------- CONSENSUS OPS ----------------------------------------------- */
    //主要是改变成员列表参数，改变节点所指向的nextokfront和nextokback节点
    /**
     * logger.info("New writesTo: " + writesTo.getAddress());
     * */
    protected void onMembershipChange(MembershipChange notification, short emitterId) {

        //update membership and responder
        membership = notification.getOrderedMembers();

        //Writes to changed
        if (writesTo == null || !notification.getWritesTo().equals(writesTo.getAddress())) {
            //Close old writesTo
            if (writesTo != null && !writesTo.getAddress().equals(self)) {
                writesToConnected = false;
                closeConnection(writesTo, peerChannel);
            }
            //Update and open to new writesTo
            writesTo = new Host(notification.getWritesTo(), PEER_PORT);
            logger.info("New writesTo: " + writesTo.getAddress());
            if (!writesTo.getAddress().equals(self))
                openConnection(writesTo, peerChannel);
        }
    }

}