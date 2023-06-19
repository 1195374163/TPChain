package TPOChain;

import TPOChain.ipc.SubmitReadRequest;
import TPOChain.utils.*;
import common.values.*;
import io.netty.handler.codec.redis.FixedRedisMessagePool;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import TPOChain.ipc.ExecuteReadReply;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import TPOChain.messages.*;
import TPOChain.timers.*;
import pt.unl.fct.di.novasys.babel.internal.BabelMessage;
import pt.unl.fct.di.novasys.babel.internal.MessageInEvent;
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel;
import pt.unl.fct.di.novasys.channel.tcp.events.*;
import frontend.ipc.DeliverSnapshotReply;
import frontend.ipc.GetSnapshotRequest;
import frontend.ipc.SubmitBatchRequest;
import frontend.notifications.*;
import io.netty.channel.EventLoopGroup;
import pt.unl.fct.di.novasys.network.data.Host;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

public class TPOChainProto extends GenericProtocol  implements ShareDistrubutedInstances{
    private static final Logger logger = LogManager.getLogger(TPOChainProto.class);

    public final static short PROTOCOL_ID = 200;
    public final static String PROTOCOL_NAME = "TPOChainProto";

    
    public static final String ADDRESS_KEY = "consensus_address";
    public static final String PORT_KEY = "consensus_port";

    
    public static final String LEADER_TIMEOUT_KEY = "leader_timeout";
  
    public static final String RECONNECT_TIME_KEY = "reconnect_time";
    public static final String NOOP_INTERVAL_KEY = "noop_interval";
    public static final String JOIN_TIMEOUT_KEY = "join_timeout";
    public static final String STATE_TRANSFER_TIMEOUT_KEY = "state_transfer_timeout";
    
    
    public static final String INITIAL_STATE_KEY = "initial_state";
    public static final String INITIAL_MEMBERSHIP_KEY = "initial_membership";
    public static final String QUORUM_SIZE_KEY = "quorum_size";

    
    /**
     * 各项超时设置
     */
    private final int LEADER_TIMEOUT;
    private final int NOOP_SEND_INTERVAL;
    private final int QUORUM_SIZE;
    private final int RECONNECT_TIME;
    private final int JOIN_TIMEOUT;
    private final int STATE_TRANSFER_TIMEOUT;
    
    
    //  对各项超时之间的依赖关系：
    //   noOp_sended_timeout  leader_timeout  reconnect_timeout
    //下面是具体取值
    /**
     * leader_timeout=5000
     * noop_interval=100
     * join_timeout=3000
     * state_transfer_timeout=5000
     * reconnect_time=1000
     * */

    
    //打算废弃？
    // 不能废弃，因为 在系统处于状态不稳定(即无leader时)时，暂存一些重要命令，如其他节点发过来的
    // 成员添加命令
    /**
     * 在竞选leader时即候选者时暂存的命令
     * */
    private final Queue<AppOpBatch> waitingAppOps = new LinkedList<>();
    private final Queue<MembershipOp> waitingMembershipOps = new LinkedList<>();

    
    
    

    /**
     * 节点的状态：在加入整个系统中的状态变迁
     * */
    enum State {JOINING, WAITING_STATE_TRANSFER, ACTIVE}
    private TPOChainProto.State state;

    /**
     * 代表自身，左右相邻主机
     * */
    private final Host self;
    
    //对于leader的排序消息和分发消息都是同一个消息传播通道，
    // 关键是能否正确的传达到下一个节点，不以消息类型进行区分
    
    /**
     * 消息的下一个节点
     * */
    private Host nextOkFront;
    private Host nextOkBack;
    
    
    
    
    /**
     * 这是对系统全体成员的映射包含状态的成员列表，随时可以更新
     * */
    private Membership membership;
    
    /**
     * 已经建立连接的主机
     * */
    private final Set<Host> establishedConnections = new HashSet<>();
    
    
    
    /**
     * 代表着leader，有term 和  Host 二元组组成
     * */
    private Map.Entry<Integer, SeqN> currentSN;
    //SeqN是一个term,由<int,host>组成，而currentSN前面的Int，是term刚开始的实例号
    

    
    
    /**
     * 是否为leader,职能排序
     * */
    private boolean amQuorumLeader;

    /**
     * leader使用，隔断时间发送no-op命令
     * */
    private long noOpTimerCL = -1;

    /**
     * 保持leader的心跳信息，用系统当前时间减去上次时间
     * */
    private long lastAcceptTimeCl;
    
    // 还有一个field ： System.currentTimeMillis(); 隐含了系统的当前时间
    
    
    
    
    /**
     *计时 非leader前链节点计算与leader之间的呼吸
     * */
    private long leaderTimeoutTimer;
    //在每个节点标记上次leader命令的时间
    private long lastLeaderOp;

    // 还有一个field ： System.currentTimeMillis(); 隐含了系统的当前时间

    
    

    /**
     * 标记是否为前段节点，代表者可以发送command，并向leader发送排序
     * */
    private boolean amFrontedNode;
    
    //在commandleader发送第一条命令的时候开启闹钟，
    // 在第一次ack和  accept与send  相等时，关闹钟，刚来时
    private long frontflushMsgTimer = -1;
    //主要是前链什么时候发送flushMsg信息
    private long lastSendTime;
    
    // 还有一个field ： System.currentTimeMillis(); 隐含了系统的当前时间
    
    
    
    //标记前链节点能否开始处理客户端的请求
    private boolean canHandleQequest=false;
    
    
    
    

    /**
     * 在各个节点暂存的信息和全局存储的信息
     */
    private static final int INITIAL_MAP_SIZE = 4000;

    /**
     * 局部日志
     */
    private final Map<Host,Map<Integer, InstanceState>> instances = new HashMap<>(INITIAL_MAP_SIZE);

    /**
     * 全局日志
     */
    private final Map<Integer, InstanceStateCL> globalinstances=new HashMap<>(INITIAL_MAP_SIZE);

    
    // 加一个锁能对下面数据的访问:
    // 对全局日志参数的访问
    private final Object executeLock = new Object();
    
    /**
     * leader这是主要排序阶段使用
     * */
    private int highestAcknowledgedInstanceCl = -1;
    // Notice 因为两份日志需要都齐全的话，才能执行命令，
    //  缺少全局日志，那么保证不了所有命令的一致性
    //  缺少局部日志，那么无法知道命令的内容
    private int highestExecuteInstanceCl= -1;
    private int highestDecidedInstanceCl = -1;
    private int highestAcceptedInstanceCl = -1;
    //leader使用，即使发生leader交换时，由新leader接棒
    private int lastAcceptSentCl = -1;
    
    
    

    // 分发时的配置信息
    /**
     * 对节点的一些配置信息，主要是各前链节点分发的实例信息
     * 和 接收到accptcl的数量
     * */
    private  Map<Host,RuntimeConfigure>  hostConfigureMap=new HashMap<>();
    
    private  short  threadid;

    
    
    //TODO 新加入节点除了获取系统的状态，还要获取系统的membership，以及前链节点，以及哪些节点正在被删除状态
    // 在joinsuccessMsg添加这些信息
    
    /**
     * 加入节点时需要的一些配置
     * */

    /**
     * 在节点加入系统中的那个实例
     */
    private int joiningInstanceCl;
    private long joinTimer = -1;
    private long stateTransferTimer = -1;

    
    /**
     * 暂存一些系统的节点状态，好让新加入节点装载
     * */
     //Dynamic membership
    //TODO eventually forget stored states... (irrelevant for experiments)
    //Waiting for application to generate snapshot (boolean represents if we should send as soon as ready)
    private final Map<Host, MutablePair<Integer, Boolean>> pendingSnapshots = new HashMap<>();
    //Application-generated snapshots
    private final Map<Host, Pair<Integer, byte[]>> storedSnapshots = new HashMap<>();
    //给我发送 joinSuccess消息的节点
    //List of JoinSuccess (nodes who generated snapshots for me)
    private final Queue<Host> hostsWithSnapshot = new LinkedList<>();


    
    //这个需要 这里存储的是全局要处理的信息
    /**
     *暂存节点处于joining，暂存从前驱节点过来的批处理信息，等待状态变为active，再进行处理
     */
    private final Queue<SortValue> bufferedOps = new LinkedList<>();
    private Map.Entry<Integer, byte[]> receivedState = null;
    private  List<Host>  canForgetSoredStateTarget;




    /**
     *关于节点的tcp通道信息
     */
    private int peerChannel;

    /**
     * java中net框架所需要的数据结构
     * */
    private final EventLoopGroup workerGroup;

    /**
     *主机链表
     * */
    private final LinkedList<Host> seeds;


    //todo 以消息的投票数决定发到下一个前链节点(少于F+1)，还是发往后链节点(>=F+1)
    // 在commandleader被替换时注意(即进行mark之后)，断开前的那个节点要往新的节点以及ack到accept的
    // 全局日志，以及所有局部日志，重发ack信息


    //todo
    // 删除节点时，进行了标记，那么在标记后会发生什么？特别是删除的前链，会做什么？
    // 除了因为故障死机，还有因为网络堵塞，没连上集群节点的情况


    //todo
    // 若在链尾节点，中发现一个分发消息的发送者不在集群中，那么对全体广播ack，对消息进行一个确认
    // 全部节点对于这个消息进行ack


    //todo
    // 新加入节点会在刚和前末尾节点连接时，前末尾节点检测输出口，前末尾节点会进行所有消息的转发
    // 若新加入节点是后链的链首
    // 若新加入节点是后链的其他节点
    // 不会出现新加入节点是前链的情况，因为那是因为系统节点不满足F+1已经终止了
    // 新加入节点，在加入时，先取得状态还是先取得前继节点发来的消息


    //todo  
    // 新加入节点也要 申请一份局部日志 和他的 局部日志表
    // 先判断是否已经存在：出现这种情况是 被删除节点重新加入集群


    //todo 在leader宕机时，只是前链节点转发新的客户端消息不能进行，老消息可以继续进行
    
    
    /**
     *构造函数
     */
    public TPOChainProto(Properties props, EventLoopGroup workerGroup) throws UnknownHostException {
        super(PROTOCOL_NAME, PROTOCOL_ID /*, new BetterEventPriorityQueue()*/);
        
        this.workerGroup = workerGroup;
        // Map.Entry<Integer, SeqN> currentSN   是一个单键值对
        currentSN = new AbstractMap.SimpleEntry<>(-1, new SeqN(-1, null));
        
        amQuorumLeader = false;//默认不是leader
        amFrontedNode=false; //默认不是前链节点
        canHandleQequest=false;//相应的默认不能处理请求
        
        self = new Host(InetAddress.getByName(props.getProperty(ADDRESS_KEY)),
                Integer.parseInt(props.getProperty(PORT_KEY)));

        //不管是排序还是分发消息：标记下一个节点
        nextOkFront =null;
        nextOkBack=null;
        
        
        this.QUORUM_SIZE = Integer.parseInt(props.getProperty(QUORUM_SIZE_KEY));
        this.RECONNECT_TIME = Integer.parseInt(props.getProperty(RECONNECT_TIME_KEY));
        //interval  间隔
        this.LEADER_TIMEOUT = Integer.parseInt(props.getProperty(LEADER_TIMEOUT_KEY));
        if (props.containsKey(NOOP_INTERVAL_KEY))
            this.NOOP_SEND_INTERVAL = Integer.parseInt(props.getProperty(NOOP_INTERVAL_KEY));
        else
            this.NOOP_SEND_INTERVAL = LEADER_TIMEOUT / 3;

        this.JOIN_TIMEOUT = Integer.parseInt(props.getProperty(JOIN_TIMEOUT_KEY));
        this.STATE_TRANSFER_TIMEOUT = Integer.parseInt(props.getProperty(STATE_TRANSFER_TIMEOUT_KEY));

        this.state = TPOChainProto.State.valueOf(props.getProperty(INITIAL_STATE_KEY));
        seeds = readSeeds(props.getProperty(INITIAL_MEMBERSHIP_KEY));//返回的是LinkedList<Host>
    }

    /**
     * 获得初始集群所有节点
     * */
    private LinkedList<Host> readSeeds(String membershipProp) throws UnknownHostException {
        LinkedList<Host> peers = new LinkedList<>();
        String[] initialMembership = membershipProp.split(",");
        for (String s : initialMembership) {
            peers.add(new Host(InetAddress.getByName(s), self.getPort()));
        }
        return peers;
    }

    @SuppressWarnings("DuplicatedCode")
    @Override
    public void init(Properties props) throws HandlerRegistrationException, IOException {

        Properties peerProps = new Properties();
        peerProps.put(TCPChannel.ADDRESS_KEY, props.getProperty(ADDRESS_KEY));
        peerProps.setProperty(TCPChannel.PORT_KEY, props.getProperty(PORT_KEY));
        peerProps.put(TCPChannel.WORKER_GROUP_KEY, workerGroup);
        peerChannel = createChannel(TCPChannel.NAME, peerProps);
        setDefaultChannel(peerChannel);

        
        registerMessageSerializer(peerChannel, AcceptAckMsg.MSG_CODE, AcceptAckMsg.serializer);
        //新加
        registerMessageSerializer(peerChannel, AcceptAckCLMsg.MSG_CODE, AcceptAckCLMsg.serializer);
        registerMessageSerializer(peerChannel, AcceptMsg.MSG_CODE, AcceptMsg.serializer);
        //新加
        registerMessageSerializer(peerChannel, AcceptCLMsg.MSG_CODE, AcceptCLMsg.serializer);
        registerMessageSerializer(peerChannel, DecidedMsg.MSG_CODE, DecidedMsg.serializer);
        //新加
        registerMessageSerializer(peerChannel, DecidedCLMsg.MSG_CODE, DecidedCLMsg.serializer);
        registerMessageSerializer(peerChannel, JoinRequestMsg.MSG_CODE, JoinRequestMsg.serializer);
        registerMessageSerializer(peerChannel, JoinSuccessMsg.MSG_CODE, JoinSuccessMsg.serializer);
        registerMessageSerializer(peerChannel, MembershipOpRequestMsg.MSG_CODE, MembershipOpRequestMsg.serializer);
        registerMessageSerializer(peerChannel, PrepareMsg.MSG_CODE, PrepareMsg.serializer);
        registerMessageSerializer(peerChannel, PrepareOkMsg.MSG_CODE, PrepareOkMsg.serializer);
        registerMessageSerializer(peerChannel, StateRequestMsg.MSG_CODE, StateRequestMsg.serializer);
        registerMessageSerializer(peerChannel, StateTransferMsg.MSG_CODE, StateTransferMsg.serializer);
        registerMessageSerializer(peerChannel, UnaffiliatedMsg.MSG_CODE, UnaffiliatedMsg.serializer);
        //新加
        registerMessageSerializer(peerChannel, OrderMSg.MSG_CODE, OrderMSg.serializer);

        
        
        registerMessageHandler(peerChannel, UnaffiliatedMsg.MSG_CODE, this::uponUnaffiliatedMsg, this::uponMessageFailed);
        registerMessageHandler(peerChannel, AcceptAckMsg.MSG_CODE, this::uponAcceptAckMsg, this::uponMessageFailed);
        //新加
        registerMessageHandler(peerChannel, AcceptAckCLMsg.MSG_CODE, this::uponAcceptAckCLMsg, this::uponMessageFailed);
        registerMessageHandler(peerChannel, AcceptMsg.MSG_CODE, this::uponAcceptMsg, this::uponMessageFailed);
        //新加
        registerMessageHandler(peerChannel, AcceptCLMsg.MSG_CODE, this::uponAcceptCLMsg, this::uponMessageFailed);
        //registerMessageHandler(peerChannel, DecidedMsg.MSG_CODE, this::uponDecidedMsg, this::uponMessageFailed);
        //新加
        registerMessageHandler(peerChannel, DecidedCLMsg.MSG_CODE, this::uponDecidedCLMsg, this::uponMessageFailed);
        registerMessageHandler(peerChannel, JoinRequestMsg.MSG_CODE,
                this::uponJoinRequestMsg, this::uponJoinRequestOut, this::uponMessageFailed);
        registerMessageHandler(peerChannel, JoinSuccessMsg.MSG_CODE,
                this::uponJoinSuccessMsg, this::uponMessageFailed);
        registerMessageHandler(peerChannel, MembershipOpRequestMsg.MSG_CODE,
                this::uponMembershipOpRequestMsg, this::uponMessageFailed);
        registerMessageHandler(peerChannel, PrepareMsg.MSG_CODE, this::uponPrepareMsg, this::uponMessageFailed);
        registerMessageHandler(peerChannel, PrepareOkMsg.MSG_CODE, this::uponPrepareOkMsg, this::uponMessageFailed);
        registerMessageHandler(peerChannel, StateRequestMsg.MSG_CODE,
                this::uponStateRequestMsg, this::uponMessageFailed);
        registerMessageHandler(peerChannel, StateTransferMsg.MSG_CODE,
                this::uponStateTransferMsg, this::uponMessageFailed);
        //新加
        registerMessageHandler(peerChannel, OrderMSg.MSG_CODE,
                this::uponOrderMSg, this::uponMessageFailed);

        
        
        registerChannelEventHandler(peerChannel, InConnectionDown.EVENT_ID, this::uponInConnectionDown);
        registerChannelEventHandler(peerChannel, InConnectionUp.EVENT_ID, this::uponInConnectionUp);
        registerChannelEventHandler(peerChannel, OutConnectionDown.EVENT_ID, this::uponOutConnectionDown);
        registerChannelEventHandler(peerChannel, OutConnectionUp.EVENT_ID, this::uponOutConnectionUp);
        registerChannelEventHandler(peerChannel, OutConnectionFailed.EVENT_ID, this::uponOutConnectionFailed);

        
        
        registerTimerHandler(JoinTimer.TIMER_ID, this::onJoinTimer);
        registerTimerHandler(StateTransferTimer.TIMER_ID, this::onStateTransferTimer);
        registerTimerHandler(LeaderTimer.TIMER_ID, this::onLeaderTimer);
        registerTimerHandler(NoOpTimer.TIMER_ID, this::onNoOpTimer);
        registerTimerHandler(ReconnectTimer.TIMER_ID, this::onReconnectTimer);
        //新加   FlushMsgTimer
        registerTimerHandler(FlushMsgTimer.TIMER_ID, this::onFlushMsgTimer);
        // 新加  ForgetStateTimer  删除节点存储的state
        registerTimerHandler(ForgetStateTimer.TIMER_ID, this:: onForgetStateTimer);
        
        
        
        //接收来自front的 状态 答复
        registerReplyHandler(DeliverSnapshotReply.REPLY_ID, this::onDeliverSnapshot);

        //接收从front的 写  和  读请求
        registerRequestHandler(SubmitBatchRequest.REQUEST_ID, this::onSubmitBatch);
        registerRequestHandler(SubmitReadRequest.REQUEST_ID, this::onSubmitRead);

        //根据初始设置：新加入节点是激活的还是等待加入的
        if (state == TPOChainProto.State.ACTIVE) {
            if (!seeds.contains(self)) {
                logger.error("Non seed starting in active state");
                throw new AssertionError("Non seed starting in active state");
            }
            //设置初始成员membership
            setupInitialState(seeds, -1);
        } else if (state == TPOChainProto.State.JOINING) {
            joinTimer = setupTimer(JoinTimer.instance, 1000);
        }

        logger.info("TPOChainProto: " + membership + " qs " + QUORUM_SIZE);
    }
    

    /**
     * 初始状态为Active开始启动节点
     * */
    private void setupInitialState(List<Host> members, int instanceNumber) {
        //传进来的参数setupInitialState(seeds, -1);
        //这里根据传进来的顺序， 已经将前链节点和后链节点分清楚出了
        membership = new Membership(members, QUORUM_SIZE);
        
        //nextOkCl = membership.nextLivingInChain(self);
        // 对排序的下一个节点准备，打算在这里
        nextOkFront =membership.nextLivingInFrontedChain(self);
        nextOkBack=membership.nextLivingInBackChain(self);
        logger.info("setupInitialState()方法中nextOkFront是"+nextOkFront+";nextOkBack是"+nextOkBack);
        
        //next   应该调用了outConnectionUp事件
        members.stream().filter(h -> !h.equals(self)).forEach(this::openConnection);
        //对全局的排序消息进行配置  -1
        joiningInstanceCl = highestAcceptedInstanceCl = highestExecuteInstanceCl =highestAcknowledgedInstanceCl = highestDecidedInstanceCl =
                instanceNumber;// instanceNumber初始为-1,但对于
        //对命令分发进行初始化配置
        for (Host temp:members) {
            //对分发的配置进行初始化  hostConfigureMap
            //  Map<Host,RuntimeConfigure>
            RuntimeConfigure runtimeConfigure=new RuntimeConfigure();
            hostConfigureMap.put(temp,runtimeConfigure);

            //局部日志进行初始化 
            //Map<Host,Map<Integer, InstanceState>> instances 
            ConcurrentMap<Integer, InstanceState>  ins=new ConcurrentHashMap<>();
            instances.put(temp,ins);
        }
        //当判断当前节点是否为前链节点
        if(membership.frontChainContain(self)){
            if (logger.isDebugEnabled()){
                logger.debug("我是前链节点，开始做前链初始操作frontChainNodeAction()");
            }
            frontChainNodeAction();
        }
    }
    
    
    /**
     *前链节点执行的pre操作
    */
    private  void  frontChainNodeAction(){
        //拥有了设置选举leader的资格
        //设置领导超时处理
        leaderTimeoutTimer = setupPeriodicTimer(LeaderTimer.instance, LEADER_TIMEOUT, LEADER_TIMEOUT / 3);
        lastLeaderOp = System.currentTimeMillis();

        //标记为前段节点
        amFrontedNode = true;
        //无leader时，不能处理事务
        canHandleQequest=false;
        threadid= (short) (membership.frontIndexOf(self)+1);
        threadID =threadid;
        
        //对下一个消息节点进行重设 
        nextOkFront =membership.nextLivingInFrontedChain(self);
        nextOkBack=membership.nextLivingInBackChain(self);
        if (logger.isDebugEnabled()){
            logger.debug("在frontChainNodeAction()nextOkFront是"+nextOkFront+"; nextOkBack是"+nextOkBack);
        }
    }


    //// 下面这些功能在设置leader处实现
    ///**
    // * 成为前链节点，这里不应该有cl后缀
    // * */
    //private  void   becomeFrontedChainNode(){
    //    logger.info("I am FrontedChain now! ");
    //    //标记为前段节点
    //    amFrontedNode = true;
    //    frontflushMsgTimer = setupPeriodicTimer(FlushMsgTimer.instance, NOOP_SEND_INTERVAL, Math.max(NOOP_SEND_INTERVAL / 3,1));
    //    //这里不需要立即发送noop消息
    //    lastSendTime = System.currentTimeMillis();
    //}
    
    //TODO  新加入节点对系统中各个分发节点也做备份，
    // 新加入节点执行这个操作
    /**
     * 新节点加入成功后 执行的方法
     * */  
    private void setupJoinInitialState(List<Host> members, int instanceNumber) {
        //传进来的参数setupInitialState(seeds, -1);
        //这里根据传进来的顺序， 已经将前链节点和后链节点分清楚出了
        membership = new Membership(members, QUORUM_SIZE);

        //nextOkCl = membership.nextLivingInChain(self);
        // 对排序的下一个节点准备，打算在这里
        nextOkFront =membership.nextLivingInFrontedChain(self);
        nextOkBack=membership.nextLivingInBackChain(self);
        //next
        members.stream().filter(h -> !h.equals(self)).forEach(this::openConnection);
        //对全局的排序消息进行配置  -1
        joiningInstanceCl = highestAcceptedInstanceCl = highestAcknowledgedInstanceCl = highestDecidedInstanceCl =
                instanceNumber;// instanceNumber初始为-1
        //对命令分发进行初始化配置
        for (Host temp:members) {
            //对分发的配置进行初始化  hostConfigureMap
            //  Map<Host,RuntimeConfigure>
            RuntimeConfigure runtimeConfigure=new RuntimeConfigure();
            hostConfigureMap.put(temp,runtimeConfigure);

            //局部日志进行初始化 
            //Map<Host,Map<Integer, InstanceState>> instances 
            ConcurrentMap<Integer, InstanceState>  ins=new ConcurrentHashMap<>();
            instances.put(temp,ins);
        }
        //当判断当前节点是否为前链节点
        if(membership.frontChainContain(self)){
            frontChainNodeAction();
        } else {//成为后链节点
        }
    }
    
    
    //TODO 考虑 ： 节点不仅可能出现故障，可能出现节点良好但网络延迟造成的相似故障，需要考虑这个
    
    
    //TODO  考虑删除节点  在删除节点时，leader故障   在删除节点时，它又新加入集群

    
    // TODO: 2023/6/6 leader故障，需要添加一个节点到前链节点,是前链节点
    //  pull协议 在故障在leader节点和非leader节点
    
    
    
    //  抑制一些候选举leader的情况：只有在与leader相聚(F+1)/2 +1
    // 有问题：在leader故障时，leader与后链首节点交换了位置
    // 除了初始情况下supportedLeader()=null;正常超时怎么解决
    // 对上面情况做出解答： 所有节点都可以竞选新leader
    /**
     * 在leadertimer超时争取当leader
     */
    private void onLeaderTimer(LeaderTimer timer, long timerId) {
        // 在进入超时,说明失去leader之间的联系
        if (!amQuorumLeader && (System.currentTimeMillis() - lastLeaderOp > LEADER_TIMEOUT) &&
                (supportedLeader() == null //初始为空  ，或新加入节点
                        //Not required, avoids joining nodes from ending in the first half of
                        // the chain and slowing everything down. Only would happen with leader
                        // election simultaneous with nodes joining.
                        //下面这个条件发生在系统运行中，这个满足，上面那个节点只有在系统刚加入时才满足
                         ||  membership.distanceFrom(self, supportedLeader()) <= 2*QUORUM_SIZE
                        )) {
            tryTakeLeadership();
        }
    }
    
    

    /**
     * Attempting to take leadership...
     * */
    private void tryTakeLeadership() { //Take leadership, send prepare
        logger.info("Attempting to take leadership...");
        assert !amQuorumLeader;
        
        //这instances是Map<Integer, InstanceState> instances
        //currentSN是Map.Entry<Integer, SeqN>类型数据
        //为什么是ack信息，保证日志的连续
        InstanceStateCL instance = globalinstances.computeIfAbsent(highestAcknowledgedInstanceCl + 1, InstanceStateCL::new);
        
        //private Map.Entry<Integer, SeqN> currentSN是
        //currentSN初始值是new AbstractMap.SimpleEntry<>(-1, new SeqN(-1, null));
        SeqN newSeqN = new SeqN(currentSN.getValue().getCounter() + 1, self);
        instance.prepareResponses.put(newSeqN, new HashSet<>());
        
        
        PrepareMsg pMsg = new PrepareMsg(instance.iN, newSeqN);
        //这是对所有节点包括自己发送prepare消息
        membership.getMembers().forEach(h -> sendOrEnqueue(pMsg, h));
    }

    
    //TODO 当一个前链节点故障时，要对其转发的command进行重新转发，可以由新leader收集并，保证局部日志不为空，
    // 否则全局日志有，而局部日志没有，导致系统进行不下去
    //  这里不需要管，因为系统中会自动进行
    
    
    // Notice 
    //  消息的去重复处理：根据id和SN对相同的消息直接丢弃
    
    
    
    //Notice
    // 消息的时效性 ：某个节点发送某个消息之后，会等待此消息的特定回复
    // 但要是此节点此时接收到其他消息导致此节点状态往前更近一步，发生了状态的迁移
    // 那么之前的消息回复是过时的
    // 还有一种情况
    // 此节点会接收term比当前tem小的过时消息，此消息已经失效
    
    
    //Notice
    // 消息的有效性
    // 发送消息的节点一定是自己集群中的节点
    
    
    
    // Notice 
    //  消息的顺序性： 
    
    
    
    /*
     * 处理PrepareMsg消息
     */
    /**
     * logger.info(msg + " 来自: " + from);
     * */
    private void uponPrepareMsg(PrepareMsg msg, Host from, short sourceProto, int channel) {
        //若集群没有它这个节点，发送其UnaffiliatedMsg，让其重新加入集群
        //logger.debug(msg + " : " + from);
        logger.info(msg + " 来自: " + from);
        if(!membership.contains(from)){
            logger.warn("Received msg from unaffiliated host " + from);
            sendMessage(new UnaffiliatedMsg(), from, TCPChannel.CONNECTION_IN);
            return;
        }
        
        if (msg.iN > highestAcknowledgedInstanceCl) {
            // currentSN消息是private Map.Entry<Integer, SeqN> currentSN;
            assert msg.iN >= currentSN.getKey();
            //在msg中大于此节点的选举领导信息
            if (!msg.sN.lesserOrEqualsThan(currentSN.getValue())) {
                //Accept - Change leader
                setNewInstanceLeader(msg.iN, msg.sN);//此函数已经选择 此msg为支持的领导

                //Gather list of accepts (if they exist)
                List<AcceptedValueCL> values = new ArrayList<>(Math.max(highestAcceptedInstanceCl - msg.iN + 1, 0));
                for (int i = msg.iN; i <= highestAcceptedInstanceCl; i++) {
                    InstanceStateCL acceptedInstance = globalinstances.get(i);
                    assert acceptedInstance.acceptedValue != null && acceptedInstance.highestAccept != null;
                    values.add(new AcceptedValueCL(i, acceptedInstance.highestAccept, acceptedInstance.acceptedValue));
                }
                sendOrEnqueue(new PrepareOkMsg(msg.iN, msg.sN, values), from);
                lastLeaderOp = System.currentTimeMillis();
            } else//当msg的SN小于等于当前leader标记，丢弃
                logger.warn("Discarding prepare since sN <= hP");
        } else { //Respond with decided message  主要对于系统中信息滞后的节点进行更新
            /*
             若发送prepare消息的节点比接收prepare消息的节点信息落后，可以发送已经DecidedMsg信息
             */
            logger.info("Responding with decided");
            List<AcceptedValueCL> values = new ArrayList<>(highestDecidedInstanceCl - msg.iN + 1);
            for (int i = msg.iN; i <= highestDecidedInstanceCl; i++) {
                InstanceStateCL decidedInstance = globalinstances.get(i);
                assert decidedInstance.isDecided();
                values.add(new AcceptedValueCL(i, decidedInstance.highestAccept, decidedInstance.acceptedValue));
            }
            sendOrEnqueue(new DecidedCLMsg(msg.iN, msg.sN, values), from);
        }
    }

    
    /**
     *  logger.info("New highest instance leader: iN:" + iN + ", " + sN);
     * */
    private void setNewInstanceLeader(int iN, SeqN sN) {
        assert iN >= currentSN.getKey();
        assert sN.greaterThan(currentSN.getValue());
        assert iN >= currentSN.getKey();

        currentSN = new AbstractMap.SimpleEntry<>(iN, sN);
        logger.info("New highest instance leader: iN:" + iN + ", " + sN);

        //若为领导则退出领导
        if (amQuorumLeader && !sN.getNode().equals(self)) {
            amQuorumLeader = false;
            cancelTimer(noOpTimerCL);
            
            waitingAppOps.clear();
            waitingMembershipOps.clear();
        }
        //不为leader的就更新个leader的标志SN


        // TODO: 2023/5/18 改变节点的挂载，将后链节点挂载在前链除了leader节点上
        //    下面是改变前面协议对leader的指向
        triggerMembershipChangeNotification();
    }

    
    /**
     * 发送成员改变通知
     * */
    private void triggerMembershipChangeNotification() {
        //调用这里说明supportleader肯定不为null
        Host  host=membership.appendFrontChainNode(self,supportedLeader());
        if(host!=null){// 说明是后链节点，有附加的前链节点
            triggerNotification(new MembershipChange(
                    membership.getMembers().stream().map(Host::getAddress).collect(Collectors.toList()),
                    null, host.getAddress(), null));
            logger.warn("当前节点是后链"+self.toString()+"挂载在"+host.toString());
        }else{// 自己就是前链节点
            triggerNotification(new MembershipChange(
                    membership.getMembers().stream().map(Host::getAddress).collect(Collectors.toList()),
                    null, self.getAddress(), null));
            logger.warn("当前节点是前链"+self.toString());
        }
    }
    
    
    //此节点落后于其他节点，对排序信息进行处理
    /**
     * 收到DecidedCLMsg
     */
    private void uponDecidedCLMsg(DecidedCLMsg msg, Host from, short sourceProto, int channel) {
        if (logger.isDebugEnabled()){
            logger.debug(msg + " from:" + from);
        }
       
        
        InstanceStateCL instance = globalinstances.get(msg.iN);
        //在实例被垃圾收集  或    msg的实例小于此节点的Decide   或    现在的leader大于msg的leader
        if (instance == null || msg.iN <= highestDecidedInstanceCl || currentSN.getValue().greaterThan(msg.sN)) {
            logger.warn("Late decided... ignoring");
            return;
        }
        //下面的代码对应序号实例若存在的话
        
        
        //这个说明此节点在系统中属于落后状态，之前的选举leader先废弃
        instance.prepareResponses.remove(msg.sN);
        
        
        //Update decided values
        for (AcceptedValueCL decidedValue : msg.decidedValues) {
            InstanceStateCL decidedInst = globalinstances.computeIfAbsent(decidedValue.instance, InstanceStateCL::new);
            if (logger.isDebugEnabled()){
                logger.debug("Deciding:" + decidedValue + ", have: " + instance);
            }
            
            decidedInst.forceAccept(decidedValue.sN, decidedValue.value);
            
            //Make sure there are no gaps between instances
            assert instance.iN <= highestAcceptedInstanceCl + 1;
            if (instance.iN > highestAcceptedInstanceCl) {
                highestAcceptedInstanceCl++;
                assert instance.iN == highestAcceptedInstanceCl;
            }
            if (!decidedInst.isDecided())
                decideAndExecuteCL(decidedInst);
        }
        
        //No-one tried to be leader after me, trying again
        //若leader还是没有变化，还是重试，此处在更新节点的信息之后，结果可能不一样
        //当  当前的leader和之前发送的一样重新进行选举
        // 意思即为：还是没有新节点发送prepare
        if (currentSN.getValue().equals(msg.sN))
            tryTakeLeadership();
    }

    // Notice 
    /*
     *  一种情况是：
     *  当一个处于中间节点状态的节点向比它先进的节点  或  落后节点发送prepare消息，先进节点回复
     *  decideCLMsg，落后节点回复  prepareoK消息，怎么处理
     * /
    
    
    // Notice 
    /**
     *  对prepareokMsg来的消息进行学习
     *  因为PrepareokMsg携带的信息是那个节点accept信息
     * */
    
    
    /**
     *接收PrepareOkMsg消息
     * */
    private void uponPrepareOkMsg(PrepareOkMsg msg, Host from, short sourceProto, int channel) {
        InstanceStateCL instance = globalinstances.get(msg.iN);
        if (logger.isDebugEnabled()){
            logger.debug(msg + " from:" + from);
        }
        
        //在实例被垃圾收集  或   已经进行更高的ack  或   现在的leader比msg中的leader更大时
        if (instance == null || msg.iN <= highestAcknowledgedInstanceCl || currentSN.getValue().greaterThan(msg.sN)) {
            logger.warn("Late prepareOk... ignoring");
            return;
        }

        //在满足F+1个投票之后，这个响应就被删除了
        Set<Host> okHosts = instance.prepareResponses.get(msg.sN);
        if (okHosts == null) {
            if (logger.isDebugEnabled()){
                logger.debug("PrepareOk ignored, either already leader or stopped trying");
            }
            
            return;
        }
        
        okHosts.add(from);

        //Update possible accepted values
        for (AcceptedValueCL acceptedValue : msg.acceptedValueCLS) {
            InstanceStateCL acceptedInstance = globalinstances.computeIfAbsent(acceptedValue.instance, InstanceStateCL::new);
            //acceptedInstance的SeqN highestAccept属性;

            //  候选者的别的实例为空   或   新实例的term大于存在的实例的term
            if (acceptedInstance.highestAccept == null || acceptedValue.sN.greaterThan(
                    acceptedInstance.highestAccept)) {
                //可能会重新发送删除某个节点的成员管理消息，所以需要重新
                maybeCancelPendingRemoval(instance.acceptedValue);
                acceptedInstance.forceAccept(acceptedValue.sN, acceptedValue.value);
                maybeAddToPendingRemoval(instance.acceptedValue);

                //Make sure there are no gaps between instances
                assert acceptedInstance.iN <= highestAcceptedInstanceCl + 1;
                if (acceptedInstance.iN > highestAcceptedInstanceCl) {
                    highestAcceptedInstanceCl++;
                    assert acceptedInstance.iN == highestAcceptedInstanceCl;
                }
            }else {//不需要管
                //到达这里 表明这个节点已经有了对应的实例，且那个实例的leader term大于
                // 消息中的term，那么不对Msg中的消息进行更新
            }
        }

        //Notice
        /**
         * 最坏的情况下：
         * 若每次都有新的节点打破原来的prepareok的消息，那最后Host的IP地址最大将是leader
         * **/

        //TODO maybe only need the second argument of max?
        //if (okHosts.size() == Math.max(QUORUM_SIZE, membership.size() - QUORUM_SIZE + 1)) {
        // 当回复prepareok的节点达到系统的大多数
        if (okHosts.size() == QUORUM_SIZE) {
            //已经准备好了，清空答复
            instance.prepareResponses.remove(msg.sN);
            
            assert currentSN.getValue().equals(msg.sN);
            assert supportedLeader().equals(self);
            //开始成为leader
            becomeLeader(msg.iN);
        }
    }
    
    //Notice
    // 在leader选举成功，前链节点才能开始工作
    
    /**
     * I am leader now! @ instance
     * */
    private void becomeLeader(int instanceNumber) {
        amQuorumLeader = true;
        noOpTimerCL = setupPeriodicTimer(NoOpTimer.instance, NOOP_SEND_INTERVAL, Math.max(NOOP_SEND_INTERVAL / 3,1));
        
        //新leader会发送noop消息，激活前链节点的转发消息
        logger.info("I am leader now! @ instance " + instanceNumber);
        
        
        /**
         * 传递已经接收到accpted消息    Propagate received accepted ops
         * */
        for (int i = instanceNumber; i <= highestAcceptedInstanceCl; i++) {
            if (logger.isDebugEnabled()) logger.debug("Propagating received operations: " + i);
            
            InstanceStateCL aI = globalinstances.get(i);
            
            assert aI.acceptedValue != null;
            assert aI.highestAccept != null;
            //goes to the end of the queue
            this.deliverMessageIn(new MessageInEvent(new BabelMessage(new AcceptCLMsg(i, currentSN.getValue(), (short) 0,
                    aI.acceptedValue, highestAcknowledgedInstanceCl), (short)-1, (short)-1), self, peerChannel));
        }

        //标记leader发送到下一个节点到哪了
        lastAcceptSentCl = highestAcceptedInstanceCl;
        
        
        //TODO  若在之前的命令中就有要删除节点
        //   现在再对其删除，是不是应该进行判定，当leader删除节点不存在系统中，则跳过此条消息
        //   已经解决了，在leader的sendnextaccept中，如果删除不存在节点或添加已有节点，则
        //  转换为Noop消息
        /**
         * 对集群中不能连接的节点进行删除
         */
        membership.getMembers().forEach(h -> {
            if (!h.equals(self) && !establishedConnections.contains(h)) {
                logger.info("Will propose remove " + h);
                uponMembershipOpRequestMsg(new MembershipOpRequestMsg(MembershipOp.RemoveOp(h)),
                        self, getProtoId(), peerChannel);
            }
        });
        
        //设置为0立马给其他节点发送消息,因为上面的条件不一定全部满足,
        // 需要立马发送noop消息
        lastAcceptTimeCl = 0;
        
        
        //上面的都是旧消息，下面是新消息
        
        //这里的暂存的成员消息都是添加节点
        /**
         * 对咱暂存在成员操作和App操作进行发送
         * */
        PaxosValue nextOp;
        if (!waitingMembershipOps.isEmpty()){
            while ((nextOp = waitingMembershipOps.poll()) != null) {
                sendNextAcceptCL(nextOp);
            } 
        }
        //while ((nextOp = waitingAppOps.poll()) != null) {
        //    sendNextAccept(nextOp);
        //}
        // 还有其他候选者节点存储的消息
        
        // 解决办法：
        // 是不是可以暂存排序消息，在成为leader可以开始处理 
        //  如果没有称为leader，那么排序消息将丢失
        
        // leader是前链在这里进行设置，其余
        canHandleQequest=true;
        // 废弃
        ////发送竞选成功消息
        //ElectionSuccessMsg pMsg=new  ElectionSuccessMsg(instanceNumber,currentSN.getValue());
        //membership.getMembers().forEach(h -> sendOrEnqueue(pMsg, h));
    }
    
    
    // 在成功选举后，发送选举成功消息
    //private void uponElectionSuccessMsg(ElectionSuccessMsg msg, Host from, short sourceProto, int channel) {
    //    
    //}


    
    /**
     * 处理leader和其他节点呼吸事件
     * */
    private void onNoOpTimer(NoOpTimer timer, long timerId) {
        if (amQuorumLeader) {
            assert waitingAppOps.isEmpty() && waitingMembershipOps.isEmpty();
            if (System.currentTimeMillis() - lastAcceptTimeCl > NOOP_SEND_INTERVAL)
                if (logger.isDebugEnabled()){
                    logger.debug("leader 时钟超时自动发送noop消息");
                }
                
                sendNextAcceptCL(new NoOpValue());
        } else {
            logger.warn(timer + " while not quorumLeader");
            cancelTimer(noOpTimerCL);
        }
    }
    
    
    //TODO 因为只有在分发和排序都存在才可以执行，若非leader故障
    // 对非leader的排序消息要接着转发，不然程序执行不了
    // 所以先分发，后排序
    // 处理流程先分发后排序，若分发
    
    
    //TODO  当前链节点的accpt与ack标志相当时，cancel  flushMsg闹钟
    // 每当accpet+1时设置  flushMsg时钟
    
    

    
    
    
    
    
/**------leader的重新发送排序命令，也可以包括成员添加，删除操作--**/

    /**
     * 处理OrderMsg信息
     */
    private void uponOrderMSg(OrderMSg msg, Host from, short sourceProto, int channel) {
        if (amQuorumLeader){//只有leader才能处理这个排序请求
            if (logger.isDebugEnabled()){
                logger.debug("我是leader,收到"+from+"的"+msg+"开始sendNextAcceptCL()");
            }
            sendNextAcceptCL(new SortValue(msg.node,msg.iN));
        }else {//对消息进行转发,转发到leader
            sendOrEnqueue(msg,supportedLeader());
        }
    }
    
    
    /**
     * 生成排序消息发给
     * **/
    private void sendNextAcceptCL(PaxosValue val) {
        InstanceStateCL instance = globalinstances.computeIfAbsent(lastAcceptSentCl + 1, InstanceStateCL::new);
        
        PaxosValue nextValue = null;
        if (val.type == PaxosValue.Type.MEMBERSHIP) {
            MembershipOp next = (MembershipOp) val;
            if ((next.opType == MembershipOp.OpType.REMOVE && !membership.contains(next.affectedHost)) ||
                    (next.opType == MembershipOp.OpType.ADD && membership.contains(next.affectedHost))) {
                logger.warn("Duplicate or invalid mOp: " + next);
                nextValue = new NoOpValue();
            } else {//若是正常的添加，删除
                // fixme 这里需要修改，直接添加吧，不需要指定添加的位置，由集群节点映射表自己处理
                if (next.opType == MembershipOp.OpType.ADD) {
                    //这里指定了添加位置: 后链链尾位置，其实应该是后链链尾 的后面
                    next = MembershipOp.AddOp(next.affectedHost,membership.indexOf(membership.getBackChainTail()));
                }
                nextValue = next;
            }
        } else if ( val.type== PaxosValue.Type.NO_OP){
            //nextValue = new NoOpValue();
            nextValue=val;
        } else if (val.type== PaxosValue.Type.SORT){
            nextValue =val;  
        }

        //this.uponAcceptCLMsg(new AcceptCLMsg(instance.iN, currentSN.getValue(),
        //        (short) 0, nextValue, highestAcknowledgedInstanceCl), self, this.getProtoId(), peerChannel);

        sendOrEnqueue(new AcceptCLMsg(instance.iN, currentSN.getValue(),
                (short) 0, nextValue, highestAcknowledgedInstanceCl),self);
        
        //更新发送标记
        lastAcceptSentCl = instance.iN;
        //更新上次操作时间
        lastAcceptTimeCl = System.currentTimeMillis();
    }
    
    
    private void uponAcceptCLMsg(AcceptCLMsg msg, Host from, short sourceProto, int channel) {
        //无效信息：对不在系统中的节点发送未定义消息让其重新加入系统
        if(!membership.contains(from)){
            logger.warn("Received msg from unaffiliated host " + from);
            sendMessage(new UnaffiliatedMsg(), from, TCPChannel.CONNECTION_IN);
            return;
        }

        if (logger.isDebugEnabled()){
            logger.debug("收到"+from+"的:"+msg);
        }
        
        InstanceStateCL instance = globalinstances.computeIfAbsent(msg.iN, InstanceStateCL::new);
        
        
        //"Discarding decided msg"  重复而且消息已经决定了
        if (instance.isDecided() && msg.sN.equals(instance.highestAccept)) {
            logger.warn("Discarding decided msg");
            return;
        }
        
        
        // 消息的term小于当前的term
        if (msg.sN.lesserThan(currentSN.getValue())) {
            logger.warn("Discarding accept since sN < hP: " + msg);
            return;
        } //隐含着之后新消息msg.SN大于等于现在的SN
        
        

        //是对重复消息的处理：这里对投票数少的消息丢弃，考虑宕机重发的情况-————就是断掉节点前继会重发所有ack以上的消息
        // 什么时候触发：当term不变，非leader故障，要重发一些消息，那已经接收到消息因为count小被丢弃；没有接收到的就接收
        if (msg.sN.equals(instance.highestAccept) && (msg.nodeCounter <= instance.counter)) {
            logger.warn("Discarding since same sN & leader, while counter <=");
            return;
        }
        // 重发消息的话,term 会大于当前实例的
        
        
        //设置msg.sN.getNode()为新支持的leader
        if (msg.sN.greaterThan(currentSN.getValue())){
            setNewInstanceLeader(msg.iN, msg.sN);
        }

        
        //其实在这里可以设置前链是否可以处理客户端消息
        //可以接受处理客户端的消息了
        canHandleQequest=true;
        
        
        //接收了此次消息 进行更新领导操作时间
        lastLeaderOp = System.currentTimeMillis();
        
        
        assert msg.sN.equals(currentSN.getValue());

        //在这里可能取消了删除节点，后续真的删除节点时也在后面的mark中重新加入节点
        maybeCancelPendingRemoval(instance.acceptedValue);
        instance.accept(msg.sN, msg.value, (short) (msg.nodeCounter + 1));//进行对实例的投票确定
        
        
        
        //更新highestAcceptedInstance信息
        if (highestAcceptedInstanceCl < instance.iN) {
            highestAcceptedInstanceCl++;
            assert highestAcceptedInstanceCl == instance.iN;
        }

        
        if ((instance.acceptedValue.type == PaxosValue.Type.MEMBERSHIP) &&
                (((MembershipOp) instance.acceptedValue).opType == MembershipOp.OpType.REMOVE))
            markForRemoval(instance);//对节点进行标记
        
        
        forwardCL(instance);//转发下一个节点
        
        
        // 为什么需要前者，因为新leader选举出来会重发一些消息，所以需要判断instance.isDecided()
        if (!instance.isDecided() && instance.counter >= QUORUM_SIZE) //We have quorum!
            decideAndExecuteCL(instance);//决定并执行实例
        
        if (msg.ack>highestAcknowledgedInstanceCl)
             ackInstanceCL(msg.ack);//对于之前的实例进行ack并进行垃圾收集
    }

    // TODO: 2023/6/16 这里将内容 
    /**
     *标记要删除的节点
     * */
    private void markForRemoval(InstanceStateCL inst) {
        MembershipOp op = (MembershipOp) inst.acceptedValue;
        // TODO: 2023/5/18  2023年5月18日16:05:09考虑消息的重发：是不是所有消息都要重发，有些可能不受影响 
        //   具体来说  哪一个next节点改动就转发对应的消息，不需要全部转发

        // 当删除节点是前链节点
        if (membership.frontChainContain(op.affectedHost) ){
            if (!membership.frontChainContain(self)){//是后链
                Host  backTail=membership.getBackChainHead();
                if (backTail.equals(self)){//当前节点是后链链首
                    // TODO: 2023/5/18  要进行转换成前链节点标志
                    // 标记删除节点
                    membership.addToPendingRemoval(op.affectedHost);
                    // 修改next节点
                    nextOkBack=membership.nextLivingInBackChain(self);
                    nextOkFront=membership.nextLivingInFrontedChain(self);

                    //修改成前链节点需要的操作
                    //设置时钟
                    leaderTimeoutTimer = setupPeriodicTimer(LeaderTimer.instance, LEADER_TIMEOUT, LEADER_TIMEOUT / 3);
                    lastLeaderOp = System.currentTimeMillis();
                    //标记为前段节点
                    amFrontedNode = true;
                    //此处有leader时，能处理事务
                    canHandleQequest=true;
                }else {// 当前节点是后链非链首
                    membership.addToPendingRemoval(op.affectedHost);
                    return;
                }
            }else{//当前节点是前链
                if (nextOkFront.equals(op.affectedHost)){//next节点正好是要被删除节点
                    membership.addToPendingRemoval(op.affectedHost);
                    // TODO: 2023/5/18 应该重发所有到nextOkFront的消息，对于nextOkBack 
                    nextOkFront=membership.nextLivingInFrontedChain(self);
                    nextOkBack=membership.nextLivingInBackChain(self);
                    // 重发可能丢失的消息
                    for (int i = highestAcknowledgedInstanceCl + 1; i < inst.iN; i++) {
                        forwardCL(globalinstances.get(i));
                    }
                    Iterator<Map.Entry<Host, Map<Integer, InstanceState>>> outerIterator = instances.entrySet().iterator();
                    while (outerIterator.hasNext()) {
                        // 获取外层 Map 的键值对
                        Map.Entry<Host, Map<Integer, InstanceState>> outerEntry = outerIterator.next();
                        Host host = outerEntry.getKey();
                        //Map<Integer, InstanceState> innerMap = outerEntry.getValue();
                        for (int i = hostConfigureMap.get(host).highestAcknowledgedInstance + 1; i <=  hostConfigureMap.get(host).highestAcceptedInstance; i++) {
                            short threadid=(short)(membership.frontIndexOf(host)+1);
                            forward(instances.get(host).get(i),threadid);
                        }
                    }
                }else{//不是自己的next节点
                    //Notice 2023/5/18 对于其他不受影响的前链节点，它的nextOkBack也要发生改变 
                    membership.addToPendingRemoval(op.affectedHost);
                    nextOkBack=membership.nextLivingInBackChain(self);
                }
            }
        }else {// 当删除节点是后链节点
            Host  backTail=membership.getBackChainHead();
            if (backTail.equals(op.affectedHost)){//当前删除的是后链链首
                if (membership.frontChainContain(self)){// 当前节点是前链
                    // 标记删除节点
                    membership.addToPendingRemoval(op.affectedHost);
                    // 修改next节点
                    nextOkBack=membership.nextLivingInBackChain(self);
                    // 重发可能丢失的消息
                    for (int i = highestAcknowledgedInstanceCl + 1; i < inst.iN; i++) {
                        forwardCL(globalinstances.get(i));
                    }
                    Iterator<Map.Entry<Host, Map<Integer, InstanceState>>> outerIterator = instances.entrySet().iterator();
                    while (outerIterator.hasNext()) {
                        // 获取外层 Map 的键值对
                        Map.Entry<Host, Map<Integer, InstanceState>> outerEntry = outerIterator.next();
                        Host host = outerEntry.getKey();
                        //Map<Integer, InstanceState> innerMap = outerEntry.getValue();
                        for (int i = hostConfigureMap.get(host).highestAcknowledgedInstance + 1; i <=  hostConfigureMap.get(host).highestAcceptedInstance; i++) {
                            short threadid=(short)(membership.frontIndexOf(host)+1);
                            forward(instances.get(host).get(i),threadid);
                        }
                    }
                }else {//当前节点是后链其他节点
                    membership.addToPendingRemoval(op.affectedHost);
                    return;
                }
            }else {//删除的不是后链链首
                if (membership.frontChainContain(self)){
                    return;//若当前节点是前链，不受影响
                }else{//当前节点是后链
                    if (nextOkBack.equals(op.affectedHost)){//当next节点是要删除节点
                        // 标记此节点
                        membership.addToPendingRemoval(op.affectedHost);
                        // 修改next节点
                        nextOkBack=membership.nextLivingInBackChain(self);
                        // 重发可能丢失的消息
                        for (int i = highestAcknowledgedInstanceCl + 1; i < inst.iN; i++) {
                            forwardCL(globalinstances.get(i));
                        }
                        Iterator<Map.Entry<Host, Map<Integer, InstanceState>>> outerIterator = instances.entrySet().iterator();
                        while (outerIterator.hasNext()) {
                            // 获取外层 Map 的键值对
                            Map.Entry<Host, Map<Integer, InstanceState>> outerEntry = outerIterator.next();
                            Host host = outerEntry.getKey();
                            //Map<Integer, InstanceState> innerMap = outerEntry.getValue();
                            for (int i = hostConfigureMap.get(host).highestAcknowledgedInstance + 1; i <=  hostConfigureMap.get(host).highestAcceptedInstance; i++) {
                                short threadid=(short)(membership.frontIndexOf(host)+1);
                                forward(instances.get(host).get(i),threadid);
                            }
                        }
                    }else {
                        membership.addToPendingRemoval(op.affectedHost);
                        return;
                    }
                }
            }
        }


        //在这里对后链链首元素进行更改：转变成前链节点
        if (membership.nextLivingInBackChain(op.affectedHost).equals(self)){
            //拥有了设置选举leader的资格
            //设置领导超时处理
            leaderTimeoutTimer = setupPeriodicTimer(LeaderTimer.instance, LEADER_TIMEOUT, LEADER_TIMEOUT / 3);
            lastLeaderOp = System.currentTimeMillis();

            //标记为前段节点
            amFrontedNode = true;
            //无leader时，不能处理事务
            canHandleQequest=true;

            membership.addToPendingRemoval(op.affectedHost);


            //对下一个消息节点进行重设 
            nextOkFront =membership.nextLivingInFrontedChain(self);
            nextOkBack=membership.nextLivingInBackChain(self);

            for (int i = highestAcknowledgedInstanceCl + 1; i < inst.iN; i++) {
                forwardCL(globalinstances.get(i));
            }

            Iterator<Map.Entry<Host, Map<Integer, InstanceState>>> outerIterator = instances.entrySet().iterator();
            while (outerIterator.hasNext()) {
                // 获取外层 Map 的键值对
                Map.Entry<Host, Map<Integer, InstanceState>> outerEntry = outerIterator.next();
                Host host = outerEntry.getKey();
                //Map<Integer, InstanceState> innerMap = outerEntry.getValue();
                //System.out.println("Host: " + host);
                for (int i = hostConfigureMap.get(host).highestAcknowledgedInstance + 1; i <=  hostConfigureMap.get(host).highestAcceptedInstance; i++) {
                    short threadid=(short)(membership.frontIndexOf(host)+1);
                    forward(instances.get(host).get(i),threadid);
                }
            }
            return;
        }else {
            membership.addToPendingRemoval(op.affectedHost);
        }


        // 前链节点 nextOkFront  和   nextOkBack  都不为空
        // 后链节点非链尾的话  nextOkFront为空  nextOkBack不为空
        // 后链链尾的话，nextOkFront  nextOkBack 都为空


        //一个节点故障只会影响nextOkFront nextOkBack 其中之一，不会两个都影响

        // 下面节点只在前链非leader触发
        // 对当前之前的消息进行转发，不包含当前信息，因为之后的forward会对当前的消息进行转发
        if (nextOkFront!=null && nextOkFront.equals(op.affectedHost)){
            nextOkFront=membership.nextLivingInFrontedChain(self);
            // 对排序信息进行重发
            // 为什么是ack+1 开始，因为若ack序号的信息没有发往下一个节点，
            // 就不会有对应节点的ack信息
            for (int i = highestAcknowledgedInstanceCl + 1; i < inst.iN; i++) {
                forwardCL(globalinstances.get(i));
            }
            // 对分发消息进行重发
            //Map<Host,Map<Integer, InstanceState>> instances
            // 局部配置表 hostConfigureMap.get();
            Iterator<Map.Entry<Host, Map<Integer, InstanceState>>> outerIterator = instances.entrySet().iterator();
            while (outerIterator.hasNext()) {
                // 获取外层 Map 的键值对
                Map.Entry<Host, Map<Integer, InstanceState>> outerEntry = outerIterator.next();
                Host host = outerEntry.getKey();
                //Map<Integer, InstanceState> innerMap = outerEntry.getValue();
                //System.out.println("Host: " + host);
                for (int i = hostConfigureMap.get(host).highestAcknowledgedInstance + 1; i <=  hostConfigureMap.get(host).highestAcceptedInstance; i++) {
                    short threadid=(short)(membership.frontIndexOf(host)+1);
                    forward(instances.get(host).get(i),threadid);
                }
            }

            //因为将原链首元素移至删除节点的位置
            nextOkBack=membership.nextLivingInBackChain(self);
        }

        //下面在后链链首 后链中间节点  后链链尾
        if (nextOkBack !=null && nextOkBack.equals(op.affectedHost)){
            nextOkBack=membership.nextLivingInBackChain(self);
            // 对排序信息进行重发
            // 为什么是ack+1 开始，因为若ack序号的信息没有发往下一个节点，
            // 就不会有对应节点的ack信息
            for (int i = highestAcknowledgedInstanceCl + 1; i < inst.iN; i++) {
                forwardCL(globalinstances.get(i));
            }
            // 对分发消息进行重发
            //Map<Host,Map<Integer, InstanceState>> instances
            // 局部配置表 hostConfigureMap.get();
            Iterator<Map.Entry<Host, Map<Integer, InstanceState>>> outerIterator = instances.entrySet().iterator();
            while (outerIterator.hasNext()) {
                // 获取外层 Map 的键值对
                Map.Entry<Host, Map<Integer, InstanceState>> outerEntry = outerIterator.next();
                Host host = outerEntry.getKey();
                Map<Integer, InstanceState> innerMap = outerEntry.getValue();
                //System.out.println("Host: " + host);
                for (int i = hostConfigureMap.get(host).highestAcknowledgedInstance + 1; i <=  hostConfigureMap.get(host).highestAcceptedInstance ; i++) {
                    short threadid=(short)(membership.frontIndexOf(host)+1);
                    forward(instances.get(host).get(i),threadid);
                }
            }
        }else{
        }
    }
    
    
    /**
     * 转发accept信息给下一个节点
     * */
    private void forwardCL(InstanceStateCL inst) {
        // some cases no longer happen (like leader being removed)
        if (!membership.contains(inst.highestAccept.getNode())) {
            logger.error("Received accept from removed node (?)");
            throw new AssertionError("Received accept from removed node (?)");
        }
        
        // 这里需要修改  不能使用满足F+1 ，才能发往下一个节点，
        //  若一个节点故障，永远不会满足F+1,应该使用逻辑链前链是否走完

        //有两种情况属于链尾:一种是只有前链  一种是有后链

        //这是只有一种前链的情况
        //if(nextOkFront!=null && nextOkBack==null){
        //    if (inst.counter>=QUORUM_SIZE){
        //        sendMessage(new AcceptAckCLMsg(inst.iN), supportedLeader());
        //        return;
        //    }else {
        //        AcceptCLMsg msg = new AcceptCLMsg(inst.iN, inst.highestAccept, inst.counter, inst.acceptedValue,
        //                highestAcknowledgedInstanceCl);
        //        sendMessage(msg, nextOkFront);
        //        return;
        //    }
        //}
        
        
        //这说明是有后链，而且还是后链的尾节点
        if (nextOkFront==null && nextOkBack==null) {
            if (inst.counter < QUORUM_SIZE) {
                logger.error("Last living in chain cannot decide. Are f+1 nodes dead/inRemoval? "
                        + inst.counter);
                throw new AssertionError("Last living in chain cannot decide. " +
                        "Are f+1 nodes dead/inRemoval? " + inst.counter);
            }
            if (logger.isDebugEnabled()){
                logger.debug("是后链链尾,向"+ supportedLeader()+"发送AcceptAckCLMsg("+inst.iN+")");
            }
            
            sendMessage(new AcceptAckCLMsg(inst.iN), supportedLeader());
            return;
        }
        
        
        //正常转发有两种情况： 在前链中转发  在后链中转发
        AcceptCLMsg msg = new AcceptCLMsg(inst.iN, inst.highestAccept, inst.counter, inst.acceptedValue,
                highestAcknowledgedInstanceCl);
        if (nextOkFront!=null){//是前链
            if (inst.counter < QUORUM_SIZE   ){// 投票数不满F+1，发往nextOkFront
                if (logger.isDebugEnabled()){
                    logger.debug("当前节点是前链,向前链节点"+nextOkBack+"转发"+msg);
                }
               
                sendMessage(msg, nextOkFront);
            } else{
                if (logger.isDebugEnabled()){
                    logger.debug("当前节点是前链,向后链节点"+nextOkBack+"转发"+msg);
                }
                sendMessage(msg, nextOkBack);
            }
        } else {//是后链
            if (logger.isDebugEnabled()){
                logger.debug("当前节点是后链,向后链节点"+nextOkBack+"转发"+msg);
            }
            
            sendMessage(msg, nextOkBack);
        }
    }
    
    
    /**
     * decide并执行Execute实例
     * */
    private void decideAndExecuteCL(InstanceStateCL instance) {
        instance.markDecided();
        highestDecidedInstanceCl++;
        if (logger.isDebugEnabled()){
            logger.debug("Decided: " + instance.iN + " - " + instance.acceptedValue);
        }
        //Actually execute message
        if (instance.acceptedValue.type == PaxosValue.Type.SORT) {
            if (state == TPOChainProto.State.ACTIVE){
                synchronized (executeLock){
                    execute();
                }
            } else  //在节点处于加入join之后，暂存存放的批处理命令
                // TODO: 2023/6/1 这里不对，在加入节点时，不能执行这个 
                bufferedOps.add((SortValue) instance.acceptedValue);
        } else if (instance.acceptedValue.type == PaxosValue.Type.MEMBERSHIP) {
            executeMembershipOp(instance);
            synchronized (executeLock){
                execute();
            }
        } else if (instance.acceptedValue.type == PaxosValue.Type.NO_OP) {
            //nothing
            synchronized (executeLock){
                execute();
            }
        }
    }
    
    //Notice 读是ack时执行而不是decide时执行
    // FIXME: 2023/5/22 读应该是在附加的日志项被执行前执行，而不是之后或ack时执行
    /**
     * 对于ack包括以前的消息执行
     * */
    private void ackInstanceCL(int instanceN) {
        //处理初始情况
        if (instanceN<0){
            return ;
        }

        //处理重复消息或过时
        if (instanceN<=highestAcknowledgedInstanceCl){
            logger.info("Discarding 重复 acceptackclmsg"+instanceN+"当前highestAcknowledgedInstanceCl是"+highestAcknowledgedInstanceCl);
            return;
        }

        //For nodes in the first half of the chain only
        for (int i = highestDecidedInstanceCl + 1; i <= instanceN; i++) {
            InstanceStateCL ins = globalinstances.get(i);
            assert !ins.isDecided();
            decideAndExecuteCL(ins);
            assert highestDecidedInstanceCl == i;
        }
        
        // 进行ack信息的更新
        // 使用++ 还是直接赋值好 
        highestAcknowledgedInstanceCl++;
        
        
        //当ack小于等于exec，进行GC清理
        if (highestAcknowledgedInstanceCl<=highestExecuteInstanceCl){
            InstanceStateCL ins = globalinstances.get(highestAcknowledgedInstanceCl);
            if (ins.acceptedValue.type!= PaxosValue.Type.SORT){
                gcAndRead(highestAcknowledgedInstanceCl,ins,null,-1);
            }else {
                SortValue sortTarget= (SortValue)ins.acceptedValue;
                Map<Integer, InstanceState>  tagetMap=instances.get(sortTarget.getNode());
                int  iNtarget=sortTarget.getiN();
                gcAndRead(highestAcknowledgedInstanceCl,ins,tagetMap,iNtarget);
            }
        }
    }
    
    
    /**
     * leader接收ack信息，对实例进行ack
     * */
    private void uponAcceptAckCLMsg(AcceptAckCLMsg msg, Host from, short sourceProto, int channel) {
        if (logger.isDebugEnabled()){
            logger.debug("接收"+from+"的"+msg);
        }
        
        
        if (msg.instanceNumber <= highestAcknowledgedInstanceCl) {
            logger.warn("Ignoring acceptAckCL for old instance: " + msg);
            return;
        }
        
        //TODO never happens?
        InstanceStateCL inst = globalinstances.get(msg.instanceNumber);
        if (!amQuorumLeader || !inst.highestAccept.getNode().equals(self)) {
            logger.error("Received Ack without being leader...");
            throw new AssertionError("Received Ack without being leader...");
        }
        
        //这里下面两行调了顺序
        if (msg.instanceNumber>highestAcknowledgedInstanceCl)
            ackInstanceCL(msg.instanceNumber);
        // FIXME: 2023/6/15 取消下面这个一直发送的noop消息，在低负载情况下开启
        //if (inst.acceptedValue.type != PaxosValue.Type.NO_OP)
        //    lastAcceptTimeCl = 0; //Force sending a NO-OP (with the ack)
    }
    
    
    /**
     * 执行成员Removed OR  addMember操作
     * */
    private void executeMembershipOp(InstanceStateCL instance) {
        MembershipOp o = (MembershipOp) instance.acceptedValue;
        Host target = o.affectedHost;
        if (o.opType == MembershipOp.OpType.REMOVE) {
            logger.info("Removed from membership: " + target + " in inst " + instance.iN);
            membership.removeMember(target);
            triggerMembershipChangeNotification();
            closeConnection(target);
        } else if (o.opType == MembershipOp.OpType.ADD) {
            logger.info("Added to membership: " + target + " in inst " + instance.iN);
            membership.addMember(target);
            triggerMembershipChangeNotification();
            //对next  重新赋值
            nextOkFront=membership.nextLivingInFrontedChain(self);
            nextOkBack=membership.nextLivingInBackChain(self);
            
            openConnection(target);
            
            if (!hostConfigureMap.containsKey(target)){
                //添加成功后，需要添加一新加节点的局部日志表 和  局部配置表
                RuntimeConfigure runtimeConfigure=new RuntimeConfigure();
                hostConfigureMap.put(target,runtimeConfigure);  
            }

            if (!instances.containsKey(target)){
                //局部日志进行初始化 
                //Map<Host,Map<Integer, InstanceState>> instances 
                ConcurrentMap<Integer, InstanceState>  ins=new ConcurrentHashMap<>();
                instances.put(target,ins);
            }

            
            if (state == TPOChainProto.State.ACTIVE) {
                //运行到这个方法说明已经满足大多数了
                sendMessage(new JoinSuccessMsg(instance.iN, instance.highestAccept, membership.shallowCopy()), target);
                assert highestDecidedInstanceCl == instance.iN;
                //TODO need mechanism for joining node to inform nodes they can forget stored state
                pendingSnapshots.put(target, MutablePair.of(instance.iN, instance.counter == QUORUM_SIZE));
                sendRequest(new GetSnapshotRequest(target, instance.iN), TPOChainFront.PROTOCOL_ID_BASE);
            }
        }
    }
    
    //TODO 一个命令可以回复客户端，只有在它以及它之前所有实例被ack时，才能回复客户端
    // 这是分布式系统的要求，因为原程序已经隐含了这个条件，通过判断出队列结构，FIFO，保证一个batch执行了，那么
    // 它之前的batch也执行了
    //TODO 只有一个实例的排序消息和分发消息都到齐了，才能进行执行
    // 所以不是同步的，所以有时候可能稍后延迟，需要两者齐全
    // 所以每次分发消息或 排序消息达到执行要求后，都要对从
    // 全局日志的已经ack到当前decided之间的消息进行扫描，达到命令执行的要求
    // 从execute到ack的开始执行
    
    
    private  Host lacknode;
    private  int   lackid;
    //按照全局日志表执行命令:执行的是用户请求命令
    private void execute(){
        // 是使用调用处循环，还是方法内部使用循环?  外部使用
        for (int i=highestExecuteInstanceCl+1;i<= highestDecidedInstanceCl;i++){
            InstanceStateCL  globalInstanceTemp=globalinstances.get(i);
            if (globalInstanceTemp.acceptedValue.type!= PaxosValue.Type.SORT){
                // 那消息可能是成员管理消息  也可能是noop消息
                highestExecuteInstanceCl++;
                if (logger.isDebugEnabled()){
                    logger.debug("当前执行的序号为"+i+"; 当前全局实例为"+globalInstanceTemp.acceptedValue);
                }
                
                if (i<highestAcknowledgedInstanceCl){
                    gcAndRead(i,globalInstanceTemp,null,-1);
                }
            }else {// 是排序消息
                SortValue sortTarget= (SortValue)globalInstanceTemp.acceptedValue;
                Host  tempHost=sortTarget.getNode();
                Map<Integer, InstanceState> tagetMap=instances.get(tempHost);
                int  iNtarget=sortTarget.getiN();
                InstanceState ins= tagetMap.get(iNtarget);
                //如果分发消息不为空就可以执行
                if (ins == null ||  ins.acceptedValue ==null){
                    lacknode=tempHost;
                    lackid=iNtarget;
                //if (ins == null || !ins.isDecided()){
                    if (ins==null){
                        if (logger.isDebugEnabled()){
                            logger.debug("当前执行的序号为"+i+"; 当前全局实例为"+sortTarget+";对应的分发实例还没有到位");
                        }
                    } else{
                        if (logger.isDebugEnabled()){
                            logger.debug("当前执行的序号为"+i+"; 当前全局实例为"+sortTarget+";对应的分发实例的值为空，还在填充中");
                        }
                    }
                    return ;// 不能进行执行,结束执行
                }
                triggerNotification(new ExecuteBatchNotification(((AppOpBatch) ins.acceptedValue).getBatch()));
                highestExecuteInstanceCl++;
                if (i<highestAcknowledgedInstanceCl){
                    gcAndRead(i,globalInstanceTemp,tagetMap,iNtarget);
                }
                //这里有bug,不能在分发实例被decide时删除
                //应该在ack时才能删除
            }
        }
    }
    
    //这里也包括了GC(垃圾收集)模块 垃圾收集 既对局部日志GC也对全局日志进行GC，即是使用对应数据结构的remove方法 对单个实例在进行ack时调用这个垃圾收集并进行触发读处理
    private  void  gcAndRead(int iN,InstanceStateCL globalInstanceTemp,Map<Integer, InstanceState> targetMap,int targetid){
        // 触发读
        globalInstanceTemp.getAttachedReads().forEach((k, v) -> sendReply(new ExecuteReadReply(v, globalInstanceTemp.iN), k));
        
        // 删除全局日志对应日志项
        globalinstances.remove(iN);
        
        // 下面若局部日志存在对方,则也进行GC
        if (targetMap==null){// 不存在局部日志的话，不应执行那下面几步
            return ;
        }
        
        //删除局部日志对应的日志项
        targetMap.remove(targetid);
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
        RuntimeConfigure  hostSendConfigure= hostConfigureMap.get(self);
        
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
    
    
    
    
    
    
    
    
    

    // TODO: 2023/6/2 新加入节点如果局部配置信息有新加入节点,应该重新导入,
    //  他的lastsent需要等于accept信息
    
    // TODO: 2023/5/22 对于新加入节点的命令执行的标识怎么设置   decide  accept  ack  execute
    // TODO: 2023/5/22 对于接收ack()怎么处理 ,accept(),decide()怎么处理的
    //TODO  新加入的currentSN怎么解决，会不会触发leader的选举
    //  应该不会，不是前链节点 
    // 那原协议，怎么解决：
    // TODO: 2023/5/22 除了全局日志 
    
    
    
    //请求加入集群的客户端方法
    /**
     * 当处理到UnaffiliatedMsg，将节点重新加入集群
     * */
    private void uponUnaffiliatedMsg(UnaffiliatedMsg msg, Host from, short sourceProto, int channel) {
        if (state == TPOChainProto.State.ACTIVE && membership.contains(from)){
            logger.error("Looks like I have unknowingly been removed from the membership, rejoining");
            //全局日志清空
            globalinstances.clear();
            //局部日志清空
            instances.clear();
            // 局部日志配置表也清空
            hostConfigureMap.clear();
            
            // 如果是前链
            if (membership.frontChainContain(self)){
                cancelfrontChainNodeAction();
            } //后链的话，什么也不做
            //关闭所有节点连接
            membership.getMembers().stream().filter(h -> !h.equals(self)).forEach(this::closeConnection);
            //对系统的成员列表进行清空
            membership = null;
            
            //对节点的状态改为join状态
            state = TPOChainProto.State.JOINING;
            joinTimer = setupTimer(JoinTimer.instance, 1000);
        }
    }
    // 失去前链节点
    private  void  cancelfrontChainNodeAction(){
        //取消拥有了设置选举leader的资格
        //取消设置领导超时处理
        cancelTimer(leaderTimeoutTimer)   ;
        lastLeaderOp = System.currentTimeMillis();
        
        //标记前链节点不能处理
        canHandleQequest=false;
        //标记为后段节点
        amFrontedNode = false;
        
        //取消作为前链节点的定时刷新时钟 
        cancelTimer(frontflushMsgTimer); 
    }

    
    //TODO  新加入节点应该联系后链节点，不应该联系前链节点
    // 需要吗？  不需要，因为系统从哪个节点(前链，还是后链) 称为 复制节点
    // 已经得到复制节点已经接收的，未接收的终将接收到，因为它是添加在链的尾端，消息会传达
    // 到链尾
    /**
     * 处理新节点的join
     * */
    private void onJoinTimer(JoinTimer timer, long timerId) {
        if (state == TPOChainProto.State.JOINING) {
            //这里用到了seeds，是系统初始输入
            Optional<Host> seed = seeds.stream().filter(h -> !h.equals(self)).findAny();
            if (seed.isPresent()) {
                openConnection(seed.get());
                sendMessage(new JoinRequestMsg(), seed.get());
                logger.info("Sending join msg to: " + seed.get());
                joinTimer = setupTimer(JoinTimer.instance, JOIN_TIMEOUT);
            } else {
                throw new IllegalStateException("No seeds to communicate with...");
            }
        } else
            logger.warn("Unexpected JoinTimer");
    }

    /**
     * 消息发送出去后,关闭连接
     * */
    private void uponJoinRequestOut(JoinRequestMsg msg, Host to, short destProto, int channelId) {
        closeConnection(to);
    }
    
    // Notice 在第F+1个节点发送joinsuccessMsg，后续F个，以及前链前F个节点都会发joinsuccessMsg
    /**
     * 收到添加操作被执行的节点的反馈：先是后链节点 ，后是前链节点发送
     * */
    private void uponJoinSuccessMsg(JoinSuccessMsg msg, Host from, short sourceProto, int channel) {
        // 在收到第F+1时执行这个
        if (state == TPOChainProto.State.JOINING) {
            hostsWithSnapshot.add(from);
            cancelTimer(joinTimer);
            logger.info("Join successful");
            //TODO 这里也需要membership的状态信息
            //构造函数也得变
            
            //setupInitialState(msg.membership, msg.iN);
            setupJoinInitialState(msg.membership,msg.iN);
            // TODO: 2023/5/29 对局部日志的初始化，和参数的配置
            setNewInstanceLeader(msg.iN, msg.sN);
            if (receivedState != null) {//第F+1节点传递过来StateTransferMsg消息
                assert receivedState.getKey().equals(msg.iN);
                triggerNotification(new InstallSnapshotNotification(receivedState.getValue()));
                state = TPOChainProto.State.ACTIVE;
                // TODO: 2023/5/23  关于加入成功后，就执行暂存的命令，合适吗？ 
                //   还有对bufferedOps的清空
                //bufferedOps.forEach(o -> triggerNotification(new ExecuteBatchNotification(o.getBatch())));
            } else {//在没有接收到F+1节点传递过来的状态
                state = TPOChainProto.State.WAITING_STATE_TRANSFER;
                stateTransferTimer = setupTimer(StateTransferTimer.instance, STATE_TRANSFER_TIMEOUT);
            }
        } else if (state == TPOChainProto.State.WAITING_STATE_TRANSFER) {
            //在收到第F+1 到2F+1,以及前链F个时
            hostsWithSnapshot.add(from);
        } else
            logger.warn("Ignoring " + msg);
    }

    /**
     *时钟  重新请求快照
     * */
    private void onStateTransferTimer(StateTransferTimer timer, long timerId) {
        if (state == TPOChainProto.State.WAITING_STATE_TRANSFER) {
            Host poll = hostsWithSnapshot.poll();
            if (poll == null) {
                logger.error("StateTransferTimeout and have nobody to ask a snapshot for...");
                throw new AssertionError();
            } else {
                sendMessage(new StateRequestMsg(joiningInstanceCl), poll);
            }
        } else
            logger.warn("Unexpected StateTransferTimer");
    }

    /**
     * 后收到  从决定加入得到节点得到快照，来进行转换
     * */
    private void uponStateTransferMsg(StateTransferMsg msg, Host from, short sourceProto, int channel) {
        cancelTimer(stateTransferTimer);
        if (state == TPOChainProto.State.JOINING) {
            //如果此时节点还没收到 joinSuccessMsg，先将state存储在备份表中
            receivedState = new AbstractMap.SimpleEntry<>(msg.instanceNumber, msg.state);
        } else if (state == TPOChainProto.State.WAITING_STATE_TRANSFER) {
            assert msg.instanceNumber == joiningInstanceCl;
            triggerNotification(new InstallSnapshotNotification(msg.state));
            // 
            state = TPOChainProto.State.ACTIVE;
            // TODO: 2023/5/22 根据怎么存在 bufferedOps ，这里就怎么处理 
            while (!bufferedOps.isEmpty()) {
                SortValue element = bufferedOps.poll(); // 出队一个元素并返回
                execute();
                // 处理队头元素
                // ...
                // 在此处可以对队头元素进行处理，可以根据具体需求编写处理逻辑
                //bufferedOps.forEach(o -> triggerNotification(new ExecuteBatchNotification(o.getBatch())));
            }
            // TODO: 2023/5/17 bufferedOPs的清空  
            /*
            highestExecuteInstanceCl +=bufferedOps.size();
            bufferedOps.clear();
            */
        }
    }

    //TODO  新加入节点也要对局部日志和节点配置表进行更新


    // TODO: 2023/5/29 因为joinsuccessage和store  state是配套的，
    //  用哪个节点的store state就用那个 joinMessage
    //加入节点的接收服务端处理方法

    
    /**
     * 所有节点都可能收到新节点的请求加入信息，收到之后将添加节点信息发给supportedLeader()
     * */
    private void uponJoinRequestMsg(JoinRequestMsg msg, Host from, short sourceProto, int channel) {
        if (state == TPOChainProto.State.ACTIVE)
            if (supportedLeader() != null)
                sendOrEnqueue(new MembershipOpRequestMsg(MembershipOp.AddOp(from)), supportedLeader());
            else //无leader的情况下
                logger.warn("Nobody to re-propagate to");
        else
            logger.warn("Ignoring joinRequest while in " + state + " state: " + msg);
    }


    /**
     * 请求StateRequestMsg
     * */
    private void uponStateRequestMsg(StateRequestMsg msg, Host from, short sourceProto, int channel) {
        Pair<Integer, byte[]> storedState = storedSnapshots.get(from);
        if (storedState != null) {
            // 如果存储的状态列表中存在，直接发送
            sendMessage(new StateTransferMsg(storedState.getKey(), storedState.getValue()), from);
        } else if (pendingSnapshots.containsKey(from)) {
            // 节点还在请求状态的过程中，还没有处理完毕
            // 设置标记位为true，可以给新加入节点发送状态
            pendingSnapshots.get(from).setRight(true);
        } else {
            logger.error("Received stateRequest without having a pending snapshot... " + msg);
            throw new AssertionError("Received stateRequest without having a pending snapshot... " + msg);
        }
    }

    // 所有执行添加节点的节点都会存储一个状态
    /**
     *接收到Frontend发来的Snapshot，转发到target.
     * */
    public void onDeliverSnapshot(DeliverSnapshotReply not, short from) {
        //  移除等待状态队列中  一个是GC  
        MutablePair<Integer, Boolean> pending = pendingSnapshots.remove(not.getSnapshotTarget());
        assert not.getSnapshotInstance() == pending.getLeft();

        // TODO: 2023/5/23 需要机制来对存储的状态进行垃圾收集 
        //  可以设置一个时钟多长时间进行清除状态
        storedSnapshots.put(not.getSnapshotTarget(), Pair.of(not.getSnapshotInstance(), not.getState()));
        
        //当此节点正好是F+1，正好是第qurom个节点，其余的不满足条件为false
        // 或者  某个节点收到来自加入节点的状态请求消息，这个会设置为true 
        if (pending.getRight()) {
            sendMessage(new StateTransferMsg(not.getSnapshotInstance(), not.getState()), not.getSnapshotTarget());
        }
    }
   
    /**
     * 设置一个时钟对超时的状态进行删除操作 ：这个时间和状态转换的时间有关系，比他大一点 
     * */
    private void onForgetStateTimer(ForgetStateTimer timer, long timerId) {
        // 如果超时，进行删除对应
        // TODO: 2023/5/29 设置节点状态删除条件，如果ack发送的是节点存储的的那个实例号，
        //  进行删除存储的状态
        //storedSnapshots.remove();
    }



    




    
    

    //TODO 在节点候选时
    // waitingAppOps   waitingMembershipOps接收正常操作 
    // 不应该接收，因为当前的候选节点不是总能成为leader，那么暂存的这些消息将一直保留在这里
    // 但直接丢弃也不行，直接丢弃会损失数据
    

/**----------------涉及读 写  成员更改--------------------------------------------------- */
/**----------接收上层中间件来的消息---------------------*/    

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


    /**
     *处理frontend发来的批处理读请求
     * */
    public void onSubmitRead(SubmitReadRequest not, short from) {
        int readInstance = highestAcceptedInstanceCl + 1;
        globalinstances.computeIfAbsent(readInstance, InstanceStateCL::new).attachRead(not);
    }


    /**
     *接收处理成员改变Msg
     */
    private void uponMembershipOpRequestMsg(MembershipOpRequestMsg msg, Host from, short sourceProto, int channel) {
        if (amQuorumLeader)//不改，成员管理还是由leader负责
            sendNextAcceptCL(msg.op);
        else if (supportedLeader().equals(self))
            waitingMembershipOps.add(msg.op);
        else
            logger.warn("Received " + msg + " without being leader, ignoring.");
    }







    
    
    
    
    
    

    /**
     * 消息Failed，发出logger.warn("Failed:)
     */
    private void uponMessageFailed(ProtoMessage msg, Host host, short i, Throwable throwable, int i1) {
        logger.warn("Failed: " + msg + ", to: " + host + ", reason: " + throwable.getMessage());
    }

    
    /**
     * 当向外的连接 建立时的操作：重新向nexotok发断点因为宕机漏发的信息
     * */
    //这是系统的自动修正
    //TODO 将ack到accept的消息全部转发到下一个节点
    // 节点可能是nextokfront 或  nextback
    // 系统本来就会发重复消息: 在接收端对消息进行筛选处理，对重复消息丢弃
    //Notice  新加入节点会触发这个，
    // 新加入节点的前继将所有东西进行转发
    // todo 会 or不会 ？因为初始时，这是outconnetion？
    private void uponOutConnectionUp(OutConnectionUp event, int channel) {
        if (logger.isDebugEnabled()){
            logger.debug(event);
        }
        //logger.info("初始化时有没有这条消息，有的话说明向外输出通道已经建立，是openconnection()调用建立的");
        if(state == TPOChainProto.State.JOINING)
            return;
        //todo 新加入节点会用到了这个吗，所有节点向新加入节点发送以往的实例
        // 本意是在节点重连下一个节点时，发送数据
        // 会不会造成消息的投票信息不正确
        // TODO: 2023/6/1 新加入节点，会触发这个吗？这样是否满足状态的后续更新
        // TODO: 2023/6/1 其他节点在什么时候开启和新加入节点的连接 
        //   如果是新加入节点，不止从ack开始发，应该从execute开始转发
        if (membership.contains(event.getNode())) {// 若集群节点含有这个
            establishedConnections.add(event.getNode());
            
            for (int i = highestAcknowledgedInstanceCl + 1; i <= highestAcceptedInstanceCl; i++) {
                forwardCL(globalinstances.get(i));
            }
            // FIXME: 2023/6/9 还需要转发分发消息
            //// TODO: 2023/6/1 这里只转发ack及以上消息对吗？ 
            //Iterator<Map.Entry<Host, Map<Integer, InstanceState>>> outerIterator = instances.entrySet().iterator();
            //while (outerIterator.hasNext()) {
            //    // 获取外层 Map 的键值对
            //    Map.Entry<Host, Map<Integer, InstanceState>> outerEntry = outerIterator.next();
            //    Host host = outerEntry.getKey();
            //    for (int i = hostConfigureMap.get(host).highestAcknowledgedInstance + 1; i <=  hostConfigureMap.get(host).highestAcceptedInstance; i++) {
            //        forward(instances.get(host).get(i));
            //    }
            //}
        }
    }

    /**
     * 当向外连接断掉时的操作，发出删除节点的消息
     * */
    private void uponOutConnectionDown(OutConnectionDown event, int channel) {
        logger.warn(event);
        establishedConnections.remove(event.getNode());

        if (membership.contains(event.getNode())) {
            setupTimer(new ReconnectTimer(event.getNode()), RECONNECT_TIME);

            if (amQuorumLeader)//若是leader,触发删除节点操作
                uponMembershipOpRequestMsg(new MembershipOpRequestMsg(MembershipOp.RemoveOp(event.getNode())),
                        self, getProtoId(), peerChannel);
            else if (supportedLeader().equals(event.getNode()))
                lastLeaderOp = 0;// 强制进行leader选举
        }
    }
    
    
    /**
     * 当向外的连接失败时，设置重连计时器
     * */
    private void uponOutConnectionFailed(OutConnectionFailed<ProtoMessage> ev, int ch) {
        logger.warn("Connection failed to " + ev.getNode() + ", cause: " + ev.getCause().getMessage());
        if (membership.contains(ev.getNode()))
            setupTimer(new ReconnectTimer(ev.getNode()), RECONNECT_TIME);
    }
    
    
    /**
     * 重连的具体操作
     * */
    private void onReconnectTimer(ReconnectTimer timer, long timerId) {
        if (membership.contains(timer.getHost()))
            openConnection(timer.getHost());
    }

    //无实际动作
    private void uponInConnectionUp(InConnectionUp event, int channel) {
        if (logger.isDebugEnabled()){
            logger.debug(event);
        }
    }
    //无实际动作
    private void uponInConnectionDown(InConnectionDown event, int channel) {
        logger.info(event);
    }


    



    // todo 正因为这样在执行删除操作时，因进行 可能取消删除某节点操作  后面又添加这节点操作
    //  maybeCancelPendingRemoval   maybeAddToPendingRemoval
    //  因为新leader的选举成功  特别是消息的重发时，因为term变了，还得接受这个消息
    //  对删除消息可能要重复执行

    // TODO: 2023/6/7 因为 maybeAddToPendingRemoval()方法实际改变了节点的排布情况
    
    
    //  Utils   工具方法
    /**
     * 先取消要删除的节点，将删除节点加入节点列表
     * */
    private void maybeCancelPendingRemoval(PaxosValue value) {
        if (value != null && value.type == PaxosValue.Type.MEMBERSHIP) {
            MembershipOp o = (MembershipOp) value;
            if (o.opType == MembershipOp.OpType.REMOVE)
                membership.cancelPendingRemoval(o.affectedHost);
        }
    }

    /**
     * 将要删除节点重新加入要删除列表
     * */
    private void maybeAddToPendingRemoval(PaxosValue value) {
        if (value != null && value.type == PaxosValue.Type.MEMBERSHIP) {
            MembershipOp o = (MembershipOp) value;
            if (o.opType == MembershipOp.OpType.REMOVE)
                membership.addToPendingRemoval(o.affectedHost);
        }
    }


    /**
     * 返回当前实例的leader
     * */
    private Host supportedLeader() {
        return currentSN.getValue().getNode();
    }
    
    
    /**
     * 发送消息给自己和其他主机
     */
    void sendOrEnqueue(ProtoMessage msg, Host destination) {
        if (logger.isDebugEnabled()){
            logger.debug("Destination: " + destination);
        }
        if (msg == null || destination == null) {
            logger.error("null: " + msg + " " + destination);
        } else {
            if (destination.equals(self)) deliverMessageIn(new MessageInEvent(new BabelMessage(msg, (short)-1, (short)-1), self, peerChannel));
            else sendMessage(msg, destination);
        }
    }
}
