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
import pt.unl.fct.di.novasys.babel.generic.ProtoReply;
import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;
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

    //下面是字段名
    private static final Logger logger = LogManager.getLogger(TPOChainData.class);

    
    public final static short PROTOCOL_ID = 300;
    public final static String PROTOCOL_NAME = "TPOChainData";
    // 下面还有一个通道的标识
    
    
    
    public static final String ADDRESS_KEY = "consensus_address";
    // 值等于原始协议号+通道标识
    public static final String DATA_PORT_KEY = "data_port";
    
    

    
 
    public static final String NOOP_INTERVAL_KEY = "noop_interval";
    
    public static final String RECONNECT_TIME_KEY = "reconnect_time";
    
    public static final String QUORUM_SIZE_KEY = "quorum_size";


    
    
    
    // 下面是变量
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
 




    //前链节点使用：在commandleader接收ack时开启一次性闹钟发完即关闭，在第一次ack和  accept与send  相等时，关闹钟，刚来时
    //主要是前链什么时候发送flushMsg信息    
    private long frontflushMsgTimer = -1;
    // 上一次发送发送accept附带的ack的时间
    private long lastSendTime;
    
    // 作为前链节点上次接收ack的时间：如果长时间没有新的实例过来，那么ack信息需要重新发送
    private  long  lastReceiveAckTime;
    
    
    
    
    
    
    //是前链但还不能处理请求的暂存队列，即在leader不存在或还没有连接成功时暂存
    private final Queue<AppOpBatch> waitingAppOps = new LinkedList<>();
    
    //前链节点使用：向Leader发送申请排序的失败的，必须等待Leader重新连接之后，重发到Leader
    private final  Queue<OrderMSg>  failtoLeaderAppOps=new LinkedList<>();



    
    
    // 使用这条通道的历来节点(不包含当前节点)的链表，主要是因为前链节点可能故障，恢复时会有新的前链节点重新使用这条通道，需要重发一下ack信息
    // 添加：如果这个根据在更换前链的方法中将accpetNodeInetAddress添加其中
    // 删除：当收到这个节点的ack达到accpt数且新的通道使用者不是这个节点，这个可以删除
    private final Set<InetAddress> hashSetAcceptNode = new HashSet<>();

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

    // 这个数据通道中的链首
    private Host Head;
    //前链连接和后链连接
    private Host self;
    //下一个节点：链尾的节点将nextOk需要连接Head节点，因为链尾节点需要向Head
    private Host  nextok;
    private boolean nextokConnected;
    //leader 向leader发送排序请求，这里的端口号是TPOChainProto的端口50300而不是自己的50200 
    private Host leader; //private boolean leaderConnected;
    //标记前链节点能否开始处理客户端的请求 其实可以废弃,用leaderConnnected标记代替
    private boolean canHandleQequest = false;
    
    // 根据membership推算出自己是否为前链节点
    /**
     * 标记是否为前段节点，是的话可以分发消息，并向leader发送排序
     */
    private boolean amFrontedNode;


    
    // TODO: 2023/7/28  GC回收通道使用者的旧日志    那历来使用者的日志呢  
    // gc线程 ：
    private  Thread gcThread;
    // 旧的分发消息的ack  还有   旧的执行信息(这个从配值文件找)
    private BlockingQueue<Integer> oldackqueue    = new LinkedBlockingQueue<Integer>();




    

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
        
        nextok=null;
        leader = null;
        nextokConnected=false;
        canHandleQequest = false;//相应的默认不能处理请求
        

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

        

        //设置一份刷新时钟
        registerTimerHandler(FlushMsgTimer.TIMER_ID, this::onFlushMsgTimer);
        //这里的Reconnect和控制层的Reconnect重了，需要单独设置一份重连的时钟
        registerTimerHandler(ReconnectDataTimer.TIMER_ID, this::onReconnectDataTimer);
        
        
        
        //接收从front的 写 请求
        registerRequestHandler(SubmitBatchRequest.REQUEST_ID, this::onSubmitBatch);

        
        
        // 接收上层Control层的一些控制信息
        subscribeNotification(LeaderNotification.NOTIFICATION_ID, this::onLeaderChange);
        subscribeNotification(FrontChainNotification.NOTIFICATION_ID, this::onFrontChainNotification);
        subscribeNotification(InitializeCompletedNotification.NOTIFICATION_ID, this::onInitializeCompletedNotification);
               
        
        
        // 注册通道事件
        registerChannelEventHandler(peerChannel, OutConnectionDown.EVENT_ID, this::onOutConnectionDown);
        registerChannelEventHandler(peerChannel, OutConnectionUp.EVENT_ID, this::onOutConnectionUp);
        registerChannelEventHandler(peerChannel, OutConnectionFailed.EVENT_ID, this::onOutConnectionFailed);
        registerChannelEventHandler(peerChannel, InConnectionDown.EVENT_ID, this::uponInConnectionDown);
        registerChannelEventHandler(peerChannel, InConnectionUp.EVENT_ID, this::uponInConnectionUp);
        
        
        gcThread.start();
        logger.info("TPOChaindata"+index+"开启: ");
        
    }

    
    // TODO: 2023/7/27 修复错误gc线程:程序爆Null错误，一般就是这出了问题：解决办法是当换节点时清空ack表
    //  强制设定  GC线程只处理  通道使用者的回收任务,因为别的节点就算剩余也不会残余太多
    // FIXME: 2023/7/28   不使用accept使用channeluser 而且对通道使用者每一轮进行缓存到中间变量
    private void gcLoop() {
        // 应该负责清除本通道的数据分发
        // FIXME: 2023/7/27 这里其实有bug：当accept节点更换后，ack还是旧值
        // TODO: 2023/7/27  在通道使用者更换时清楚ack旧值队列  
        int receiveack;
        int  receiveexecute;
        while (true) {
            //因为通道使用者可能改变，先缓存
            RuntimeConfigure  temp=channelUserRuntimeConfigure;
            Map<Integer, InstanceState> tempMap=channelUserInstanceMap;
            // TODO: 2023/7/28  因为取出来，就放不回去，实际有效值就其中一个，而且考虑两者谁大谁小，重新组织一下顺序关系 
            while(true){
                try {
                    //todo 使用poll(,) 加上时间   还是    take(),
                    receiveack=oldackqueue.take();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                if (temp.highestGCInstance<receiveack){
                    break;
                }
            }
            while(true){
                try {
                    receiveexecute=temp.executeFlagQueue.take();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                if (temp.highestGCInstance<receiveexecute){
                    break;
                }
            }
            int min=Math.min(receiveack,receiveexecute);
            for (int i=temp.highestGCInstance+1;i<min;i++){
                if (temp.highestGCInstance< min){
                    tempMap.remove(i);
                    temp.highestGCInstance++;
                }
            }
        }
    }



    
    

    /**---------------------------接收来自front层的分发消息 ---------**/

    /**
     * 当前时leader或正在竞选leader的情况下处理frontend的提交batch
     */
    public void onSubmitBatch(SubmitBatchRequest not, short from) {
        if (amFrontedNode) {// 改为前链节点 
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
        } else //忽视接受的消息
            logger.warn("Received " + not + " without being FrontedNode, ignoring.");
    }
    
    
    

    /**-----------------------------处理节点的分发信息-------------------------------**/
    
    // -----------生成accept信息---------------------在通用配置
    private InetAddress selfInetAddress;
    private SeqN newterm;// = new SeqN(0, self);
    private RuntimeConfigure selfRuntimeConfigure;
    private Map<Integer, InstanceState>  selfInstanceMap;

    
    
    /**
     * 在当前节点是前链节点时处理，发送 或Noop 或App_Batch信息
     */
    private void sendNextAccept(PaxosValue val) {
        //设置上次的刷新时间：上次发送时间 :这里应该用配置文件的时间，而不是设置的两个时间，其实也可以
        lastSendTime = System.currentTimeMillis();
        
        
        // 得到分发消息的那个实例
        InstanceState instance;
        if (!selfInstanceMap.containsKey(selfRuntimeConfigure.lastAcceptSent + 1)) {
            instance=new InstanceState(selfRuntimeConfigure.lastAcceptSent + 1);
            selfInstanceMap.put(selfRuntimeConfigure.lastAcceptSent + 1, instance);
        }else {
            instance=selfInstanceMap.get(selfRuntimeConfigure.lastAcceptSent + 1);
        }
        

        //InstanceState instance = selfInstanceMap.computeIfAbsent(selfRuntimeConfigure.lastAcceptSent + 1, InstanceState::new);
        //assert instance.acceptedValue == null && instance.highestAccept == null;
        //对当前的生成自己的seqn,以线程通道id为count，self为标记
        
        
        // 先发给自己:这里直接使用对应方法
        //this.uponAcceptMsg(new AcceptMsg(instance.iN, newterm,
        //        (short) 0, nextValue,hostSendConfigure.highestAcknowledgedInstance), self, this.getProtoId(), peerChannel);
        sendOrEnqueue(new AcceptMsg(instance.iN, newterm,
                (short) 0, val, selfRuntimeConfigure.highestAcknowledgedInstance), self);

        //设置发送标记数
        selfRuntimeConfigure.lastAcceptSent = instance.iN;
        
        
        //重发所有失败的申请排序信息
        if(!failtoLeaderAppOps.isEmpty()){
            while(!failtoLeaderAppOps.isEmpty()){
                sendOrEnqueue(failtoLeaderAppOps.poll(),leader);
            }
        }
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

        InstanceState instance;
        if (!acceptInstanceMap.containsKey(msg.iN)) {
            instance=new InstanceState(msg.iN);
            acceptInstanceMap.put(msg.iN, instance);
        }else {
            instance=acceptInstanceMap.get(msg.iN);
        }
        
        
        //"Discarding decided msg" 当instance
        //if (instance.isDecided()) {
        //    logger.warn("Discarding decided acceptmsg");
        //    return;
        //}

        //如果消息是新生成的那么,它的投票数为0,肯定不满足下面这个条件，若是重发的则直接满足条件进行丢弃
        if (msg.nodeCounter +1  <= instance.counter) {
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

        
        // TODO: 2023/7/28  下面是ack的处理，注意下面还需要完善 ：这里是对携带的ack信息的处理
        // 前段节点没有decide ，后端节点已经decide，
        //if (!instance.isDecided() && instance.counter >= QUORUM_SIZE) //We have quorum!
        //    decideAndExecute(instance);//决定并执行实例
        if (msg.ack > acceptRuntimeConfigure.highestAcknowledgedInstance) {
            //对于之前的实例进行ack并进行垃圾收集
           // ackInstance(accpetNodeInetAddress, msg.ack);
            //if (msg.ack <= acceptRuntimeConfigure.highestAcknowledgedInstance) {
            //    logger.info("Discarding 重复的 acceptackmsg;当前节点是"  + "当前的要确定的序号是" + instanceN + "当前的acceptack序号是" + hostSendConfigure.highestAcknowledgedInstance);
            //    return;
            //}
            acceptRuntimeConfigure.highestAcknowledgedInstance++;
        }
        
        //若当前节点将目前的通道使用节点加入集合中，将这条语句放入状态改变之后
        if (!hashSetAcceptNode.contains(accpetNodeInetAddress)){
            hashSetAcceptNode.add(accpetNodeInetAddress);
        }

        // TODO: 2023/7/27 将ack信息放入队列中，注意标注是什么节点的ack信息。 
        
        //将instance添加到消息队列中:
        hostMessageQueue.get(accpetNodeInetAddress).add(instance);
    }


    /**
     * 转发accept信息给下一个节点
     */
    private void forward(InstanceState inst) {
        Host sendHost = inst.highestAccept.getNode();
        //发送ack信息
        if (nextok.equals(Head)){
            AcceptAckMsg acceptAckMsgtemp = new AcceptAckMsg(sendHost, inst.iN);
            sendMessage(acceptAckMsgtemp, nextok);
        }else {//正常的转发
            AcceptMsg msg = new AcceptMsg(inst.iN, inst.highestAccept, inst.counter, inst.acceptedValue,
                    acceptRuntimeConfigure.highestAcknowledgedInstance);
            sendMessage(msg,nextok);
        }
    }
    
    
    
    
    //-------------接收ack信息---------------------------------------
    
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
        RuntimeConfigure ackHostRuntimeConfigure = hostConfigureMap.get(ackHost.getAddress());
        
        if (msg.instanceNumber <= ackHostRuntimeConfigure.highestAcknowledgedInstance) {
            logger.warn("Ignoring acceptAck of Data for old instance: " + msg+"from"+from);
            return;
        }
        
        // 执行ack程序
        ackHostRuntimeConfigure.highestAcknowledgedInstance++;


        //在接收ack，如果是之前的通道使用者，且这个使用者的ack和accept相等，
        if (!ackInetAddress.equals(accpetNodeInetAddress) && hashSetAcceptNode.contains(ackInetAddress)){
            if (ackHostRuntimeConfigure.highestAcceptedInstance==ackHostRuntimeConfigure.highestAcknowledgedInstance){
                hashSetAcceptNode.remove(ackHost);
            }
        }

        // FIXME: 2023/7/19 考虑故障之后，考虑将闹钟要表示是哪个节点的刷新信息
        //这里设置一个定时器,发送acceptack,一段时间没有后续要发的
        // 在发送新实例时在sendnextaccpt取消如果当前节点等于消息的发送节点,再设立闹钟
        if (ackHost.equals(self)){
            lastReceiveAckTime=System.currentTimeMillis();
            // TODO: 2023/7/28  不能设置为noop 的0.1s  应该长一点，因为设置为0.5s甚至1s 
            frontflushMsgTimer=setupTimer(FlushMsgTimer.instance,NOOP_SEND_INTERVAL);
        }
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
            cancelTimer(frontflushMsgTimer);
        }
    }




    
    
    

    /**------------------------------接收来自控制层的成员管理消息 ---------**/

    //Control层初始化完成，对自身节点的一些参数设置:设置self,主要是因为TPChain的控制层对局部分发日志和配置进行初始化
    protected  void onInitializeCompletedNotification(InitializeCompletedNotification notification,short emitterID){
        // TODO: 2023/7/27 可以将局部日志和配置表的初始化放在Data层而不是Control控制层 
        selfInetAddress=self.getAddress();
        newterm = new SeqN(0, self);

        selfRuntimeConfigure=hostConfigureMap.get(selfInetAddress);
        selfInstanceMap=instances.get(selfInetAddress);
    }

    //在前链节点和后链节点有改动时调用，初始操作时也视为成员发生改动
    protected  void onFrontChainNotification(FrontChainNotification notification,short emitterID){
        //logger.info("接收来的通知是"+notification.getFrontChain()+notification.getBackchain());
        frontChain=notification.getFrontChain();
        
        //根据控制层传递得到通道使用者是哪个节点
        channelUser=frontChain.get(index);
        //得到通道使用者的日志表
        channelUserInstanceMap=instances.get(channelUser);
        //得到通道使用者的配置表
        channelUserRuntimeConfigure=hostConfigureMap.get(channelUser);
        
        //将之前的通道的使用者加入使用者列表
        if (accpetNodeInetAddress!=null){
            hashSetAcceptNode.add(accpetNodeInetAddress);
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
        
        //得到链首
        Head=membership.get(0);

        //添加后链
        backChain=notification.getBackchain();
        for (InetAddress tmp:backChain) {
            membership.add(new Host(tmp,data_port));
        }//logger.info("输出总链"+membership);


        //设置nextok节点 --------这个常用
        int memsize=membership.size();
        int selfindex=membership.indexOf(self);
        Host newnextok=membership.get((selfindex+1)%memsize);
        if (nextok == null || !newnextok.equals(nextok)) {
            //Close old writesTo
            if (nextok != null && !nextok.getAddress().equals(self.getAddress())) {
                nextokConnected = false;
                closeConnection(nextok, peerChannel);
            }
            //Update and open to new writesTo
            nextok =newnextok;
            logger.info("New nextok connecting: " + nextok.getAddress());
            if (!nextok.getAddress().equals(self.getAddress()))
                openConnection(nextok, peerChannel);
        }
    }

    
    // 以后关于成员更改要重新考虑，现在不做考虑：程序一些bug在之后再考虑 在leader改变时
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
        Host peer = event.getNode();
        if (peer.equals(leader)) {
            // 设置能接收客户端处理
            canHandleQequest = true;
            // 重发失败的
            while(!failtoLeaderAppOps.isEmpty()){
                sendOrEnqueue(failtoLeaderAppOps.poll(),leader);
            }
            //开始处理暂存的
            while(!waitingAppOps.isEmpty()){
                AppOpBatch appOpBatch = waitingAppOps.poll();
                // 处理 appOpBatch，可以对其进行操作或输出
                sendNextAccept(appOpBatch);
            }
            //这里将暂存的消息从暂存队列中取出来开始处理
            logger.info("already Connected to leader :"+leader.getAddress());
        }
  
        if (peer.equals(nextok)){
            nextokConnected=true;
            //应该改为所有的通道的使用者,以往的节点重发
            Iterator<InetAddress> iterator = hashSetAcceptNode.iterator();
            while (iterator.hasNext()) {
                InetAddress element = iterator.next();
                Map<Integer, InstanceState>   elementInstanceMap =instances.get(element);
                RuntimeConfigure  elementRuntimeConfigure= hostConfigureMap.get(element);
                for (int i = elementRuntimeConfigure.highestAcknowledgedInstance + 1; i <= elementRuntimeConfigure.highestAcceptedInstance; i++) {
                    forward(elementInstanceMap.get(i));
                }
            }
            //channelUser=frontChain.get(index);
            //channelUserInstanceMap=instances.get(channelUser);
            //channelUserRuntimeConfigure=hostConfigureMap.get(channelUser);
            // 现在的通道使用者重发ack->accept的分发消息
            if (channelUserRuntimeConfigure!=null){
                for (int i = channelUserRuntimeConfigure.highestAcknowledgedInstance + 1; i <= channelUserRuntimeConfigure.highestAcceptedInstance; i++) {
                    forward(channelUserInstanceMap.get(i));
                }
            }
            logger.info("already Connected to nextok :"+nextok.getAddress());
        }
    }

    // 是否可以将1s的时长设置为0.1s，加速连接
    /**
     * 在与下一个节点断开后开始重连
     */
    protected void onOutConnectionDown(OutConnectionDown event, int channel) {
        Host peer = event.getNode();
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
            if (leader.getAddress().equals(self.getAddress())){
                sendRequest(new SubmitOrderMsg((OrderMSg) msg), TPOChainProto.PROTOCOL_ID); 
            }else {
                sendMessage(msg, TPOChainProto.PROTOCOL_ID,destination);
            }
            return;
        }
        // 对于accept和ack信息,一般来说就是前链节点在接收自己处理的accept信息：
        if (destination.equals(self)) {//只有在收到front层的消息。处理时需要这个
            // 在生成accept消息时需要这个方法。
            deliverMessageIn(new MessageInEvent(new BabelMessage(msg, (short) -1, (short) -1), self, peerChannel));
        } else {
            sendMessage(msg, destination);
        }
    }
}    
