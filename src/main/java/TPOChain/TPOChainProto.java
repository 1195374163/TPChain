package TPOChain;

import TPOChain.ipc.SubmitReadRequest;
import TPOChain.utils.*;
import common.values.*;
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
import java.util.stream.Collectors;

public class TPOChainProto extends GenericProtocol {
    private static final Logger logger = LogManager.getLogger(TPOChainProto.class);

    public final static short PROTOCOL_ID = 200;
    public final static String PROTOCOL_NAME = "TPOChainProto";

    public static final String ADDRESS_KEY = "consensus_address";
    public static final String PORT_KEY = "consensus_port";
    public static final String QUORUM_SIZE_KEY = "quorum_size";
    public static final String LEADER_TIMEOUT_KEY = "leader_timeout";
    public static final String JOIN_TIMEOUT_KEY = "join_timeout";
    public static final String STATE_TRANSFER_TIMEOUT_KEY = "state_transfer_timeout";
    public static final String INITIAL_STATE_KEY = "initial_state";
    public static final String INITIAL_MEMBERSHIP_KEY = "initial_membership";
    public static final String RECONNECT_TIME_KEY = "reconnect_time";
    public static final String NOOP_INTERVAL_KEY = "noop_interval";

    
    /**
     * 各项超时设置
     */
    private final int LEADER_TIMEOUT;
    private final int JOIN_TIMEOUT;
    private final int STATE_TRANSFER_TIMEOUT;
    private final int NOOP_SEND_INTERVAL;
    private final int QUORUM_SIZE;
    private final int RECONNECT_TIME;

    //  Notice 对各项超时之间的依赖关系：
    //   noOp_sended_timeout  leader_timeout  reconnect_timeout
    //下面是具体取值
    /**
     * leader_timeout=5000
     * noop_interval=100
     * join_timeout=3000
     * state_transfer_timeout=5000
     * reconnect_time=1000
     * */

    
    //TODO 打算废弃？
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

    
    // 废弃，使用下面那两个进行转发消息
    //private Host nextOkCl;
   
    //对于leader的排序消息和分发消息都是同一个消息传播通道，
    // 关键是能否正确的传达到下一个节点，不以消息类型进行区分
    
    /**
     * 排序消息的下一个节点
     * */
    private Host nextOkFront;
    private Host nextOkBack;
    
    //todo 以消息的投票数决定发到下一个前链节点(少于F+1)，还是发往后链节点(>=F+1)
    // 在commandleader被替换时注意(即进行mark之后)，断开前的那个节点要往新的节点以及ack到accept的
    // 全局日志，以及所有局部日志，重发ack信息
    
    
    
    //todo
    // 删除节点时，进行了标记，那么在标记后会发生什么？特别是删除的前链，会做什么？
    // 除了因为故障死机，还有因为网络堵塞，没连上集群节点的情况
    
    //todo
    // 若在链尾节点，中发现一个分发消息不在集群中，那么对全体广播ack，对消息进行一个确认
    //
    
    
    //todo
    // 新加入节点会在刚和前末尾节点连接时，前末尾节点检测输出口，前末尾节点会进行所有消息的转发
    // 若新加入节点是后链的链首
    // 若新加入节点是后链的其他节点
    // 不会出现新加入节点是前链的情况，因为那是因为系统节点不满足F+1已经终止了
    // 新加入节点，在加入时，先取得状态还是先取得前继节点发来的消息
    
    //todo  
    // 新加入节点也要 申请一份局部日志 和他的 局部日志表
    // 先判断是否已经存在：出现这种情况是 被删除节点重新加入集群
    
    
    //todo 在leader宕机时，只是前链节点转发新的消息不能进行，老消息可以继续进行
    
    
    
    /**
     * 这是对系统全体成员的映射包含状态的成员列表，随时可以更新
     * */
    private Membership membership;
    
    /**
     * 已经建立连接的主机
     * */
    private final Set<Host> establishedConnections = new HashSet<>();
    
    
    // 也是视图
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
     *计时 非leader计算与leader之间的呼吸
     * 只有前链节点有
     * */
    //多长时间没接收到leader消息
    //前链节点闹钟----计时与leader之间的上一次的通信时间
    private long leaderTimeoutTimer;
    //在每个节点标记上次leader命令的时间
    private long lastLeaderOp;

    // 还有一个field ： System.currentTimeMillis(); 隐含了系统的当前时间

    
    

    /**
     * 标记是否为前段节点，代表者可以发送command，并向leader发送排序
     * */
    private boolean amFrontedNode;
    
    //Option 在commandleader发送第一条命令的时候开启闹钟，在发完三次flushMsg之后进行关闭
    // 闹钟的开启和关闭 不进行关闭
    //   在第一次ack和accept相等时，关闹钟，刚来时
    private long frontflushMsgTimer = -1;
    //主要是前链什么时候发送
    private long lastSendTime;
    
    // 还有一个field ： System.currentTimeMillis(); 隐含了系统的当前时间
    
    
    
    
    private boolean canHandleQequest=false;
    
    

    /**
     * 在各个节点暂存的信息和全局存储的信息
     */
    private static final int INITIAL_MAP_SIZE = 4000;

    /**
     * 局部日志
     */
//    private final Map<Integer, InstanceState> instances = new HashMap<>(INITIAL_MAP_SIZE);
    private final Map<Host,Map<Integer, InstanceState>> instances = new HashMap<>(INITIAL_MAP_SIZE);

    /**
     * 全局日志
     */
    private final Map<Integer, InstanceStateCL> globalinstances=new HashMap<>(INITIAL_MAP_SIZE);

    
    
    
    /**
     *leader的排序字段
     * */
    /**
     * 这是主要排序阶段使用
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


    
    
    //TODO 新加入节点除了获取系统的状态，还要获取系统的membership，以及前链节点，以及哪些节点正在被删除状态
    // 正因为这样在执行删除操作时，因进行 可能取消删除某节点操作  后面又添加这节点操作
    // maybeCancelPendingRemoval   maybeAddToPendingRemoval
    // 因为新leader的选举成功  特别是消息的重发时，因为term变了，还得接受这个消息
    
    
    
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

    
    
    /**
     *构造函数
     */
    public TPOChainProto(Properties props, EventLoopGroup workerGroup) throws UnknownHostException {
        super(PROTOCOL_NAME, PROTOCOL_ID /*, new BetterEventPriorityQueue()*/);
        
        this.workerGroup = workerGroup;
        // Map.Entry<Integer, SeqN> currentSN   是一个单键值对
        currentSN = new AbstractMap.SimpleEntry<>(-1, new SeqN(-1, null));
        
        amQuorumLeader = false;//
        amFrontedNode=false; //
        
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
        registerMessageHandler(peerChannel, DecidedMsg.MSG_CODE, this::uponDecidedMsg, this::uponMessageFailed);
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
        //next   应该调用了outConnectionUp事件
        members.stream().filter(h -> !h.equals(self)).forEach(this::openConnection);
        //对全局的排序消息进行配置  -1
        joiningInstanceCl = highestAcceptedInstanceCl = highestExecuteInstanceCl =highestAcknowledgedInstanceCl = highestDecidedInstanceCl =
                instanceNumber;// instanceNumber初始为-1
        //对命令分发进行初始化配置
        for (Host temp:members) {
            //对分发的配置进行初始化  hostConfigureMap
            //  Map<Host,RuntimeConfigure>
            RuntimeConfigure runtimeConfigure=new RuntimeConfigure();
            hostConfigureMap.put(temp,runtimeConfigure);

            //局部日志进行初始化 
            //Map<Host,Map<Integer, InstanceState>> instances 
            Map<Integer, InstanceState>  ins=new HashMap<>();
            instances.put(temp,ins);
        }
        //当判断当前节点是否为前链节点
        if(membership.isFrontChainNode(self).equals(Boolean.TRUE)){
            frontChainNodeAction();
        } else {//成为后链节点
        }
    }
    
    
    //TODO 当一个前链节点被删除时，后链首节点也要执行这个操作的uodo：取消前链特权
    
    
    
    
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
        //对下一个消息节点进行重设 
        nextOkFront =membership.nextLivingInFrontedChain(self);
        nextOkBack=membership.nextLivingInBackChain(self);
    }
    
    
    //TODO  新加入节点对系统中各个分发节点也做备份，
    // 新加入节点执行这个操作
    /**
     * 新节点加入成功后 执行的方法
     * */  
    private void setupJoinInitialState(Pair<List<Host>, Map<Host,Boolean>> members, int instanceNumber) {
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
            Map<Integer, InstanceState>  ins=new HashMap<>();
            instances.put(temp,ins);
        }
        //当判断当前节点是否为前链节点
        if(membership.isFrontChainNode(self).equals(Boolean.TRUE)){
            frontChainNodeAction();
        } else {//成为后链节点
        }
    }
    
    
    
    //TODO 考虑 ： 节点不仅可能出现故障，可能出现节点良好但网络延迟造成的相似故障，需要考虑这个
    
    
    //TODO  考虑删除节点  在删除节点时，leader故障   在删除节点时，它又新加入集群
    
    

    
    
    //  抑制一些候选举leader的情况：只有在与leader相聚(F+1)/2 +1
    // 有问题：在leader故障时，leader与后链首节点交换了位置
    // 除了初始情况下supportedLeader()=null;正常超时怎么解决
    // 对上面情况做出解答： 所有节点都可以竞选新leader
    /**
     * 在leadertimer超时争取当leader
     */
    private void onLeaderTimer(LeaderTimer timer, long timerId) {
        //无leader时，不能处理事务
        canHandleQequest=false;
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
        triggerNotification(new MembershipChange(
                membership.getMembers().stream().map(Host::getAddress).collect(Collectors.toList()),
                null, supportedLeader().getAddress(), null));
    }
    
    
    
    //此节点落后于其他节点，对排序信息进行处理
    /**
     * 收到DecidedCLMsg
     */
    private void uponDecidedCLMsg(DecidedCLMsg msg, Host from, short sourceProto, int channel) {
        logger.debug(msg + " from:" + from);
        
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
            logger.debug("Deciding:" + decidedValue + ", have: " + instance);
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
        logger.debug(msg + " from:" + from);
        //在实例被垃圾收集  或   已经进行更高的ack  或   现在的leader比msg中的leader更大时
        if (instance == null || msg.iN <= highestAcknowledgedInstanceCl || currentSN.getValue().greaterThan(msg.sN)) {
            logger.warn("Late prepareOk... ignoring");
            return;
        }


        Set<Host> okHosts = instance.prepareResponses.get(msg.sN);
        if (okHosts == null) {
            logger.debug("PrepareOk ignored, either already leader or stopped trying");
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
        if (okHosts.size() ==QUORUM_SIZE) {
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
    
    //NOTE 
    
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
        
        //设置为0立马给其他节点发送消息
        lastAcceptTimeCl = 0;


        
        //上面的都是旧消息，下面是新消息
        
        /**
         * 对咱暂存在成员操作和App操作进行发送
         * */
        PaxosValue nextOp;
        while ((nextOp = waitingMembershipOps.poll()) != null) {
            sendNextAcceptCL(nextOp);
        }
        //while ((nextOp = waitingAppOps.poll()) != null) {
        //    sendNextAccept(nextOp);
        //}
        // 还有其他候选者节点存储的消息

        canHandleQequest=true;
        // 废弃
        ////发送竞选成功消息
        //ElectionSuccessMsg pMsg=new  ElectionSuccessMsg(instanceNumber,currentSN.getValue());
        //membership.getMembers().forEach(h -> sendOrEnqueue(pMsg, h));
    }
    

    
    private void uponElectionSuccessMsg(ElectionSuccessMsg msg, Host from, short sourceProto, int channel) {
        
    }




    /**
     * 处理leader和其他节点呼吸事件
     * */
    private void onNoOpTimer(NoOpTimer timer, long timerId) {
        if (amQuorumLeader) {
            assert waitingAppOps.isEmpty() && waitingMembershipOps.isEmpty();
            if (System.currentTimeMillis() - lastAcceptTimeCl > NOOP_SEND_INTERVAL)
                sendNextAcceptCL(new NoOpValue());
        } else {
            logger.warn(timer + " while not quorumLeader");
            cancelTimer(noOpTimerCL);
        }
    }



    
    
    //TODO 因为只有在分发和排序都存在才可以执行，若非leader故障
    // 对非leader的排序消息要接着转发，不然程序执行不了
    // 所以先分发，后排序
    
    //TODO  
    
    // 下面这些功能在设置leader处实现
    /**
     * 成为前链节点，这里不应该有cl后缀
     * */
    private  void   becomeFrontedChainNode(){
        logger.info("I am FrontedChain now! ");
        //标记为前段节点
        amFrontedNode = true;
        frontflushMsgTimer = setupPeriodicTimer(FlushMsgTimer.instance, NOOP_SEND_INTERVAL, Math.max(NOOP_SEND_INTERVAL / 3,1));
        //这里不需要立即发送noop消息
        lastSendTime = System.currentTimeMillis();
    }
 
    
    //TODO  当前链节点的accpt与ack标志相当时，cancel  flushMsg闹钟
    // 每当accpet+1时设置  flushMsg时钟
    
    
    /**
     * 对最后的此节点的消息进行发送ack信息  FlushMsgTimer
     * */
    private void onFlushMsgTimer(FlushMsgTimer timer, long timerId) {
        if (amFrontedNode) {
            if (System.currentTimeMillis() - lastSendTime > NOOP_SEND_INTERVAL)
                sendNextAccept(new NoOpValue());
        } else {
            logger.warn(timer + " while not FrontedChain");
            cancelTimer(frontflushMsgTimer);
        }
    }
    
    
    
    
    
/**------leader的重新发送排序命令，也可以包括成员添加，删除操作--**/

    
     //leader负责接收其他前链节点发过来的请求排序消息
     
    /**
     * 处理OrderMsg信息
     */
    private void uponOrderMSg(OrderMSg msg, Host from, short sourceProto, int channel) {
        if (amQuorumLeader){//只有leader才能处理这个排序请求
            sendNextAcceptCL(new SortValue(msg.node,msg.iN));
        }else {//对消息进行转发,转发到leader
            sendOrEnqueue(msg,supportedLeader());
        }
    }
    
    // leader接收来自前段的消息准备发送到 
    /**
     * 生成排序消息发给
     * **/
    private void sendNextAcceptCL(PaxosValue val) {
        assert supportedLeader().equals(self) && amQuorumLeader;
        
        InstanceStateCL instance = globalinstances.computeIfAbsent(lastAcceptSentCl + 1, InstanceStateCL::new);
        assert instance.acceptedValue == null && instance.highestAccept == null;

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
            nextValue = new NoOpValue();
        }
        else if (val.type== PaxosValue.Type.SORT){
            nextValue =val;  
        }

        this.uponAcceptCLMsg(new AcceptCLMsg(instance.iN, currentSN.getValue(),
                (short) 0, nextValue, highestAcknowledgedInstanceCl), self, this.getProtoId(), peerChannel);
        
        lastAcceptSentCl = instance.iN;
        lastAcceptTimeCl = System.currentTimeMillis();
    }
    
    
    //上面那两个方法是leader独有的，只有leader才能访问
    //下面的关于排序的方法是所有节点都能访问处理的
    
    
    //Notice
    //注意 对过时消息的处理

    private void uponAcceptCLMsg(AcceptCLMsg msg, Host from, short sourceProto, int channel) {
        //无效信息：对不在系统中的节点发送未定义消息让其重新加入系统
        if(!membership.contains(from)){
            logger.warn("Received msg from unaffiliated host " + from);
            sendMessage(new UnaffiliatedMsg(), from, TCPChannel.CONNECTION_IN);
            return;
        }

        InstanceStateCL instance = globalinstances.computeIfAbsent(msg.iN, InstanceStateCL::new);
        
        //"Discarding decided msg"  重复消息
        if (instance.isDecided() && msg.sN.equals(instance.highestAccept)) {
            logger.warn("Discarding decided msg");
            return;
        }
        
        // 消息的term小于当前的term
        if (msg.sN.lesserThan(currentSN.getValue())) {
            logger.warn("Discarding accept since sN < hP: " + msg);
            return;
        }
        //隐含着之后新消息msg.SN大于等于现在的SN

        
        //设置msg.sN.getNode()为新支持的leader
        if (msg.sN.greaterThan(currentSN.getValue())){
            setNewInstanceLeader(msg.iN, msg.sN);
            if (amFrontedNode==true){
                //可以接受处理客户端的消息了
                canHandleQequest=true;
            }
        }
        
        
        //这下面的代码是在更新leader之后的
        
        
        
        //Notice 是对重复消息的处理
        // TODO: 2023/5/18 这里对投票数少的消息丢弃，考虑宕机重发
        // 什么时候触发：当term不变，非leader故障，要重发一些消息，那已经接收到消息因为
        // count小被丢弃；没有接收到的就接收
        if (msg.sN.equals(instance.highestAccept) && (msg.nodeCounter <= instance.counter)) {
            logger.warn("Discarding since same sN & leader, while counter <=");
            return;
        }
        
        
        lastLeaderOp = System.currentTimeMillis();//进行更新领导操作时间

        
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
        
        ackInstanceCL(msg.ack);//对于之前的实例进行ack并进行垃圾收集
    }

  
    /**
     *标记要删除的节点
     * */
    private void markForRemoval(InstanceStateCL inst) {
        MembershipOp op = (MembershipOp) inst.acceptedValue;
        // TODO: 2023/5/18 考虑消息的重发：是不是所有消息都要重发，有些可能不受影响 
        //   比如 
        
        // 当删除节点是前链节点
        if (membership.isFrontChainNode(op.affectedHost) == Boolean.TRUE ){
            if (membership.isFrontChainNode(self) == Boolean.FALSE){//是后链
                Host  backTail=membership.getBackChainHead();
                if (backTail.equals(self)){//当前节点是链首
                    
                }else {// 当前节点是后链非链首
                    return;
                }
            }else{//当前节点是前链
                
            }
        }else {// 当删除节点是后链节点
            Host  backTail=membership.getBackChainHead();
            if (backTail.equals(op.affectedHost)){//当前删除的是后链链首
                if (membership.isFrontChainNode(self) == Boolean.TRUE){// 当前节点是前链
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
                            forward(instances.get(host).get(i));
                        }
                    }
                }else {//当前节点是后链其他节点
                    return;
                }
            }else {//删除的不是后链链首
                if (membership.isFrontChainNode(self) == Boolean.TRUE){
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
                                forward(instances.get(host).get(i));
                            }
                        }
                    }else {
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
                    forward(instances.get(host).get(i));
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
                    forward(instances.get(host).get(i));
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
                    forward(instances.get(host).get(i));
                }
            }
        }else{
            
        }
    }
    
    
    /**
     * 转发accept信息给下一个节点
     * */
    private void forwardCL(InstanceStateCL inst) {
        //TODO some cases no longer happen (like leader being removed)
        if (!membership.contains(inst.highestAccept.getNode())) {
            logger.error("Received accept from removed node (?)");
            throw new AssertionError("Received accept from removed node (?)");
        }
        
        if (nextOkFront==null && nextOkBack==null) { //am last 只有最理想的情况下：有后链链尾尾节点会这样
            //If last in chain than we must have decided (unless F+1 dead/inRemoval)
            if (inst.counter < QUORUM_SIZE) {
                logger.error("Last living in chain cannot decide. Are f+1 nodes dead/inRemoval? "
                        + inst.counter);
                throw new AssertionError("Last living in chain cannot decide. " +
                        "Are f+1 nodes dead/inRemoval? " + inst.counter);
            }
            sendMessage(new AcceptAckCLMsg(inst.iN), supportedLeader());
        } else { 
            if (inst.counter < QUORUM_SIZE){// 投票数不满F+1，发往nextOkFront
                //not last in chain...
                AcceptCLMsg msg = new AcceptCLMsg(inst.iN, inst.highestAccept, inst.counter, inst.acceptedValue,
                        highestAcknowledgedInstanceCl);
                sendMessage(msg, nextOkFront);
            }
            else {// 投票数大于等于F+1，发往nextOkBack
                //若后链节点为空，则说明到链尾，发送ack信息
                if (nextOkBack==null){
                    sendMessage(new AcceptAckCLMsg(inst.iN), supportedLeader());
                }else {//有后链节点
                    //not last in chain...
                    AcceptCLMsg msg = new AcceptCLMsg(inst.iN, inst.highestAccept, inst.counter, inst.acceptedValue,
                            highestAcknowledgedInstanceCl);
                    sendMessage(msg, nextOkBack);
                }
            }
        }
    }
    
    /**
     * decide并执行Execute实例
     * */
    private void decideAndExecuteCL(InstanceStateCL instance) {
        assert highestDecidedInstanceCl == instance.iN - 1;
        assert !instance.isDecided();

        instance.markDecided();
        highestDecidedInstanceCl++;
        logger.debug("Decided: " + instance.iN + " - " + instance.acceptedValue);

        //Actually execute message
        if (instance.acceptedValue.type == PaxosValue.Type.SORT) {
            if (state == TPOChainProto.State.ACTIVE){
                for(int i=highestExecuteInstanceCl+1;i<=instance.iN;i++){
                    boolean temp=execute(instance);
                    if (temp== false){// 返回结果说明 有的命令分发还没结束，停止执行
                        return;
                    }
                }
                execute(instance);
                triggerNotification(new ExecuteBatchNotification(((AppOpBatch) instance.acceptedValue).getBatch()));
            } else  //在节点处于加入join之后，暂存存放的批处理命令
                bufferedOps.add((SortValue) instance.acceptedValue);
        } else if (instance.acceptedValue.type == PaxosValue.Type.MEMBERSHIP) {
            executeMembershipOp(instance);
        } else if (instance.acceptedValue.type == PaxosValue.Type.NO_OP) {
            return ;
        }
    }
    //Notice 读是ack时执行而不是decide时执行
    /**
     * 对于ack包括以前的消息执行
     * */
    private void ackInstanceCL(int instanceN) {
        if (instanceN<0){
            return ;
        }
        //For nodes in the first half of the chain only
        for (int i = highestDecidedInstanceCl + 1; i <= instanceN; i++) {
            InstanceStateCL ins = globalinstances.get(i);
            assert !ins.isDecided();
            decideAndExecuteCL(ins);
            assert highestDecidedInstanceCl == i;
        }
        
        //先执行上面的代码，先更新状态，
        // Fixme  因为前链节点节点
        //For everyone
        for (int i = highestAcknowledgedInstanceCl + 1; i <= instanceN; i++) {
            InstanceStateCL ins = globalinstances.remove(i);
            ins.getAttachedReads().forEach((k, v) -> sendReply(new ExecuteReadReply(v, ins.iN), k));

            assert ins.isDecided();
            //更新ack信息
            highestAcknowledgedInstanceCl++;
            assert highestAcknowledgedInstanceCl == i;
        }
    }

    /**
     * leader接收ack信息，对实例进行ack
     * */
    private void uponAcceptAckCLMsg(AcceptAckMsg msg, Host from, short sourceProto, int channel) {
        //logger.debug(msg + " - " + from);
        if (msg.instanceNumber <= highestAcknowledgedInstanceCl) {
            logger.warn("Ignoring acceptAck for old instance: " + msg);
            return;
        }
        //TODO never happens?
        InstanceStateCL inst = globalinstances.get(msg.instanceNumber);
        if (!amQuorumLeader || !inst.highestAccept.getNode().equals(self)) {
            logger.error("Received Ack without being leader...");
            throw new AssertionError("Received Ack without being leader...");
        }
        if (inst.acceptedValue.type != PaxosValue.Type.NO_OP)
            lastAcceptTimeCl = 0; //Force sending a NO-OP (with the ack)
        ackInstanceCL(msg.instanceNumber);
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
            // 这里虽然指定了添加位置，但其实不用
            membership.addMember(target, o.position);
            triggerMembershipChangeNotification();
            // TODO 对next  重新赋值
            nextOkFront=membership.nextLivingInFrontedChain(self);
            nextOkBack=membership.nextLivingInBackChain(self);
            openConnection(target);

            if (state == TPOChainProto.State.ACTIVE) {
                //运行到这个方法说明已经满足大多数了
                sendMessage(new JoinSuccessMsg(instance.iN, instance.highestAccept, membership.deepCopy()), target);
                assert highestDecidedInstanceCl == instance.iN;
                //TODO need mechanism for joining node to inform nodes they can forget stored state
                pendingSnapshots.put(target, MutablePair.of(instance.iN, instance.counter == QUORUM_SIZE));
                //TODO 这里需要复制一些局部日志
                // 哪怕多复制一些局部日志都可以
                sendRequest(new GetSnapshotRequest(target, instance.iN), TPOChainFront.PROTOCOL_ID_BASE);
            }
        }
    }


    
    
    
    
    /**-----------------------------处理节点的分发信息-------------------------------**/
    
    
    /**
     *在当前节点是leader时处理，发送 或成员管理 或Noop 或App_Batch信息
     * */
    private void sendNextAccept(PaxosValue val) {
        RuntimeConfigure  hostSendConfigure= hostConfigureMap.get(self);
        // 当sent标记和ack相等，说明开启新的一轮分发，要开启闹钟，同时将标记时间为当前节点
        if (hostSendConfigure.lastAcceptSent == hostSendConfigure.highestAcknowledgedInstance){
            frontflushMsgTimer = setupPeriodicTimer(FlushMsgTimer.instance, NOOP_SEND_INTERVAL, Math.max(NOOP_SEND_INTERVAL / 3,1));
            lastSendTime = System.currentTimeMillis();
        }
        
        
        InstanceState instance = instances.get(self).computeIfAbsent(hostSendConfigure.lastAcceptSent+1, InstanceState::new);
        assert instance.acceptedValue == null && instance.highestAccept == null;
        
        
        PaxosValue nextValue;
        if (val.type == PaxosValue.Type.APP_BATCH) {
            nextValue = val;
            //nBatches++;
            //nOpsBatched += ops.size();
        } else
            nextValue = new NoOpValue();
        
        
        //对当前的生成自己的seqn,以term为期，self为标记
        // 应该不需要term，不需要term标记
        SeqN newterm=new SeqN(currentSN.getValue().getCounter(),self);
        // 先发给自己:这里直接使用对应方法
        this.uponAcceptMsg(new AcceptMsg(instance.iN, newterm,
                (short) 0, nextValue,hostSendConfigure.highestAcknowledgedInstance), self, this.getProtoId(), peerChannel);

        // 
        hostSendConfigure.lastAcceptSent = instance.iN;
        // 上次的刷新时间
        lastSendTime = System.currentTimeMillis();
        
        
        //同时向leader发送排序请求
        OrderMSg orderMSg=new OrderMSg(self,instance.iN);
        sendOrEnqueue(orderMSg,supportedLeader());
    }
    
    // 上面这个是前链节点转发消息需要执行的操作
    
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

        
        Host sender=msg.sN.getNode();
        InstanceState instance = instances.get(sender).computeIfAbsent(msg.iN, InstanceState::new);
        
        //"Discarding decided msg" 当instance
        if (instance.isDecided()) {
            logger.warn("Discarding decided msg");
            return;
        }
        
        // 废弃 对于重发的消息丢弃
        //if (msg.nodeCounter <= instance.counter) {
        //    logger.warn("Discarding since same sN & leader, while counter <=");
        //    return;
        //}
        
        //  对消息重复的还需要处理
        
        hostConfigureMap.get(sender).lastAcceptTime = System.currentTimeMillis();//进行更新领导操作时间
        
        instance.accept(msg.sN, msg.value, (short) (msg.nodeCounter + 1));//进行对实例的确定

        //更新highestAcceptedInstance信息
        if ( hostConfigureMap.get(sender).highestAcceptedInstance < instance.iN) {
            hostConfigureMap.get(sender).highestAcceptedInstance++;
            assert hostConfigureMap.get(sender).highestAcceptedInstance == instance.iN;
        }
        
        
        forward(instance);//转发下一个节点

        // 前段节点没有decide ，后端节点已经decide，
        if (!instance.isDecided() && instance.counter >= QUORUM_SIZE) //We have quorum!
            decideAndExecute(instance);//决定并执行实例

        ackInstance(sender,msg.ack);//对于之前的实例进行ack并进行垃圾收集
    }


    /**
     * 转发accept信息给下一个节点
     * */
    private void forward(InstanceState inst) {
        // some cases no longer happen (like leader being removed)
        //if (!membership.contains(inst.highestAccept.getNode())) {
        //    logger.error("Received accept from removed node (?)");
        //    throw new AssertionError("Received accept from removed node (?)");
        //}
        if (nextOkFront==null && nextOkBack==null) { //am last 只有最理想的情况下：有后链链尾尾节点会这样
            //If last in chain than we must have decided (unless F+1 dead/inRemoval)
            if (inst.counter < QUORUM_SIZE) {
                logger.error("Last living in chain cannot decide. Are f+1 nodes dead/inRemoval? "
                        + inst.counter);
                throw new AssertionError("Last living in chain cannot decide. " +
                        "Are f+1 nodes dead/inRemoval? " + inst.counter);
            }
            sendMessage(new AcceptAckMsg(inst.iN), inst.highestAccept.getNode());
        } else {
            if (inst.counter < QUORUM_SIZE){// 投票数不满F+1，发往nextOkFront
                //not last in chain...
                AcceptMsg msg = new AcceptMsg(inst.iN, inst.highestAccept, inst.counter, inst.acceptedValue,
                        hostConfigureMap.get(inst.highestAccept.getNode()).highestAcknowledgedInstance);
                sendMessage(msg, nextOkFront);
            } else {// 投票数大于等于F+1，发往nextOkBack
                //若后链节点为空，则说明到链尾，发送ack信息
                if (nextOkBack==null){
                    sendMessage(new AcceptAckMsg(inst.iN), inst.highestAccept.getNode());
                }else {//有后链节点
                    //not last in chain...
                    AcceptMsg msg = new AcceptMsg(inst.iN, inst.highestAccept, inst.counter, inst.acceptedValue,
                            hostConfigureMap.get(inst.highestAccept.getNode()).highestAcknowledgedInstance);
                    sendMessage(msg, nextOkBack);
                }
            }
        }
    }
    
    
    /**
     * decide并执行Execute实例
     * */
    private void decideAndExecute(InstanceState instance) {
        //assert hostConfigureMap.get(self).highestDecidedInstance == instance.iN - 1;
        //assert !instance.isDecided();
        
        instance.markDecided();
        hostConfigureMap.get(instance.highestAccept.getNode()).highestDecidedInstance++;
        // 上面是decide的，只对其进行了标记
        //logger.debug("Decided: " + instance.iN + " - " + instance.acceptedValue);

        
        //Actually execute message
        // 接下来是执行
        // TODO: 2023/5/18   需要等待排序命令一块到达才可以执行，触发一次排序命令的执行，因为
        //  排序命令缺少分发命令停止了一些执行
        execute();
        //if (instance.acceptedValue.type == PaxosValue.Type.APP_BATCH) {
        //    if (state == TPOChainProto.State.ACTIVE)
        //        triggerNotification(new ExecuteBatchNotification(((AppOpBatch) instance.acceptedValue).getBatch()));
        //    else
        //        bufferedOps.add((AppOpBatch) instance.acceptedValue);
        //} else if (instance.acceptedValue.type != PaxosValue.Type.NO_OP) {
        //    logger.error("Trying to execute unknown paxos value: " + instance.acceptedValue);
        //    throw new AssertionError("Trying to execute unknown paxos value: " + instance.acceptedValue);
        //}
    }
    
    /**
     * 对于ack包括以前的消息执行
     * */
    private void ackInstance(Host sendHost,int instanceN) {
        // 对于前链节点先进行deccide
        //For nodes in the first half of the chain only
        for (int i = hostConfigureMap.get(sendHost).highestDecidedInstance + 1; i <= instanceN; i++) {
            InstanceState ins = instances.get(sendHost).get(i);
            //assert !ins.isDecided();
            decideAndExecute(ins);
            //assert highestDecidedInstanceCl == i;
        }

        //For everyone
        for (int i = hostConfigureMap.get(sendHost).highestAcknowledgedInstance+ 1; i <= instanceN; i++) {
            hostConfigureMap.get(sendHost).highestAcknowledgedInstance++;
        }

        // TODO: 2023/5/18 其他节点在得到noop信息之后开启一段时间，自动对noop信息进行ack 
        //  因为noop的时钟已经关闭，
        //   这里是接收到noop的ack信息。
        //   在发送完一次noop消息得到他的ack消息
        if (self.equals(sendHost) && instances.get(sendHost).get(instanceN).acceptedValue.type== PaxosValue.Type.NO_OP){
            // 但ack赶上sent时结束闹钟 
            if(hostConfigureMap.get(self).highestAcknowledgedInstance == hostConfigureMap.get(self).lastAcceptSent) {
                cancelTimer(frontflushMsgTimer);
            }
        }
    }
    
    
    /**
     * leader接收ack信息，对实例进行ack
     * */
    private void uponAcceptAckMsg(AcceptAckMsg msg, Host from, short sourceProto, int channel) {

        // 这个方法只有命令的分发者才能调用
        if (msg.instanceNumber <= hostConfigureMap.get(self).highestAcknowledgedInstance) {
            logger.warn("Ignoring acceptAck for old instance: " + msg);
            return;
        }
        
        ackInstance(self,msg.instanceNumber);
        
        InstanceState inst = instances.get(self).get(msg.instanceNumber);
        // 立即发送no_op信息对消息进行确认
        if (inst.acceptedValue.type != PaxosValue.Type.NO_OP)
            lastSendTime= 0; //Force sending a NO-OP (with the ack)
    }



    
    
    
    //TODO 一个命令可以回复客户端，只有在它以及它之前所有实例被ack时，才能回复客户端
    // 这是分布式系统的要求，因为原程序已经隐含了这个条件，通过判断出队列结构，FIFO，保证一个batch执行了，那么
    // 它之前的batch也执行了
    
    //TODO 只有一个实例的排序消息和分发消息都到齐了，才能进行执行
    // 所以不是同步的，所以有时候可能稍后延迟，需要两者齐全
    // 所以每次分发消息或 排序消息达到执行要求后，都要对从
    // 全局日志的已经ack到当前decided之间的消息进行扫描，达到命令执行的要求
    
    // TODO 这里也包括了GC(垃圾收集)模块
    //   垃圾收集 既对局部日志GC也对全局日志进行GC，即是使用对应数据结构的remove方法
    
    
    
    //todo 怎么使用 ，由排序和分发执行
    // 
    //排序  
    //todo  在外面包含这个逻辑
    private boolean execute(){
            InstanceStateCL instance;
        
        //在全局日志中既包含了成员管理  no_op  也包含了 排序信息
        // 这里只筛选出 包含
        //判断类型
        int decide=instance.iN;
        SortValue sortTarget= (SortValue)globalinstances.get(decide).acceptedValue;
        Host target=sortTarget.getNode();
        int  iNtarget=sortTarget.getiN();
        InstanceState ins= instances.get(target).get(iNtarget);
        if (ins==null){
            return false;
        }// 接下来是存在
        if (state == TPOChainProto.State.ACTIVE)
            triggerNotification(new ExecuteBatchNotification(((AppOpBatch) ins.acceptedValue).getBatch()));
        else
            bufferedOps.add((AppOpBatch) ins.acceptedValue);
        
            if (instances.get(self).get(decide).acceptedValue.type== PaxosValue.Type.APP_BATCH){
                for (int i= highestExecuteInstanceCl+1;i<= decide;i++){
                    InstanceStateCL execute=globalinstances.get(i);
                    SortValue temp=(SortValue)execute.acceptedValue;
                    if(instances.get(temp.getNode()).get(temp.getiN())!=null){
                    }else{return ;}
                }
            }else if (instances.get(self).get(decide).acceptedValue.type != PaxosValue.Type.NO_OP){
                logger.error("Trying to execute unknown paxos value: " + instance.acceptedValue);
                throw new AssertionError("Trying to execute unknown paxos value: " + instance.acceptedValue);
            }
        //Map<Integer, InstanceStateCL> globalinstances
        // 接下来是执行
        //TODO  需要等待排序命令一块到达才可以执行
        return ;
    }

    
    
    
    
    
    
    //Notice
    //GC回收

    //TODO  新加入的currentSN怎么解决，会不会触发leader的选举
    //  应该不会，不是前链节点 
    // 那原协议，怎么解决：
    //

    
    
    
    
    
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
            
            if (membership.isFrontChainNode(self)){
                cancelfrontChainNodeAction();
            }else {//后链
                
            }
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
            // Notice 这里用到了seeds，是系统初始输入
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

    /**
     * 收到添加操作被执行的节点的反馈：先是后链节点 ，后是前链节点发送
     * */
    private void uponJoinSuccessMsg(JoinSuccessMsg msg, Host from, short sourceProto, int channel) {
        if (state == TPOChainProto.State.JOINING) {
            hostsWithSnapshot.add(from);
            cancelTimer(joinTimer);
            logger.info("Join successful");
            //TODO 这里也需要membership的状态信息
            //构造函数也得变
            
            //setupInitialState(msg.membership, msg.iN);
            setupJoinInitialState(msg.membership,msg.iN);
            setNewInstanceLeader(msg.iN, msg.sN);
            if (receivedState != null) {//第F+1节点传递过来StateTransferMsg消息
                assert receivedState.getKey().equals(msg.iN);
                triggerNotification(new InstallSnapshotNotification(receivedState.getValue()));
                state = TPOChainProto.State.ACTIVE;
                //bufferedOps.forEach(o -> triggerNotification(new ExecuteBatchNotification(o.getBatch())));
            } else {//在没有接收到F+1节点传递过来的状态
                state = TPOChainProto.State.WAITING_STATE_TRANSFER;
                stateTransferTimer = setupTimer(StateTransferTimer.instance, STATE_TRANSFER_TIMEOUT);
            }
        } else if (state == TPOChainProto.State.WAITING_STATE_TRANSFER) {
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
            bufferedOps.forEach(o -> triggerNotification(new ExecuteBatchNotification(o.getBatch())));
            // TODO: 2023/5/17 bufferedOPs的清空  
            /*
            highestExecuteInstanceCl=bufferedOps.size();
            bufferedOps.clear();
            */
        }
    }

    //TODO  新加入节点也要对局部日志和节点配置表进行更新
    
    
    
    

    //加入节点的接收服务端处理方法

    /**
     * 所有节点都可能收到新节点的请求加入信息，收到之后将添加节点信息发给supportedLeader()
     * */
    private void uponJoinRequestMsg(JoinRequestMsg msg, Host from, short sourceProto, int channel) {
        if (state == TPOChainProto.State.ACTIVE)
            if (supportedLeader() != null)
                sendOrEnqueue(new MembershipOpRequestMsg(MembershipOp.AddOp(from)), supportedLeader());
            else //有leader
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
            sendMessage(new StateTransferMsg(storedState.getKey(), storedState.getValue()), from);
        } else if (pendingSnapshots.containsKey(from)) {
            pendingSnapshots.get(from).setRight(true);
        } else {
            logger.error("Received stateRequest without having a pending snapshot... " + msg);
            throw new AssertionError("Received stateRequest without having a pending snapshot... " + msg);
        }
    }

    //Notice  所有执行添加节点的节点都会存储一个状态

    /**
     *接收到Frontend发来的Snapshot，转发到target.
     * */
    public void onDeliverSnapshot(DeliverSnapshotReply not, short from) {
        //  移除等待状态队列中  一个是GC  
        MutablePair<Integer, Boolean> pending = pendingSnapshots.remove(not.getSnapshotTarget());
        assert not.getSnapshotInstance() == pending.getLeft();
        
        storedSnapshots.put(not.getSnapshotTarget(), Pair.of(not.getSnapshotInstance(), not.getState()));
        //当此节点正好是F+1，正好是第qurom个节点，其余的不满足条件
        if (pending.getRight()) {
            sendMessage(new StateTransferMsg(not.getSnapshotInstance(), not.getState()), not.getSnapshotTarget());
        }
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
            if (canHandleQequest==true){//这个标志意味着leader存在
                //先将以往的消息转发
                PaxosValue nextOp;
                while ((nextOp = waitingAppOps.poll()) != null) {
                    sendNextAccept(nextOp);
                }
                // 还有其他候选者节点存储的消息
                sendNextAccept(new AppOpBatch(not.getBatch()));
            }else {
                waitingAppOps.add(new AppOpBatch(not.getBatch()));
            }
        }    
        ////打算废弃
        //else if (supportedLeader().equals(self))
        //    waitingAppOps.add(new AppOpBatch(not.getBatch()));
        else //忽视接受的消息
            logger.warn("Received " + not + " without being FrontedNode, ignoring.");
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
        //打算废弃
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
        logger.debug(event);
        logger.info("初始化时有没有这条消息，有的话说明向外输出通道已经建立，是openconnection()调用建立的");
        if(state == TPOChainProto.State.JOINING)
            return;
        //todo 新加入节点会用到了这个吗，所有节点向新加入节点发送以往的实例
        // 本意是在节点重连下一个节点时，发送数据
        // 会不会造成消息的投票信息不正确
        
        // todo 新加入节点满足这个条件吗
        if (membership.contains(event.getNode())) {// 若集群节点含有这个
            establishedConnections.add(event.getNode());

            for (int i = highestAcknowledgedInstanceCl + 1; i <= highestAcceptedInstanceCl; i++) {
                forwardCL(globalinstances.get(i));
            }
            
            Iterator<Map.Entry<Host, Map<Integer, InstanceState>>> outerIterator = instances.entrySet().iterator();
            while (outerIterator.hasNext()) {
                // 获取外层 Map 的键值对
                Map.Entry<Host, Map<Integer, InstanceState>> outerEntry = outerIterator.next();
                Host host = outerEntry.getKey();
                for (int i = hostConfigureMap.get(host).highestAcknowledgedInstance + 1; i <=  hostConfigureMap.get(host).highestAcceptedInstance; i++) {
                    forward(instances.get(host).get(i));
                }
            }
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
        logger.debug(event);
    }
    //无实际动作
    private void uponInConnectionDown(InConnectionDown event, int channel) {
        logger.info(event);
    }




    
    
    
    
    
    
    
    
    
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
        logger.debug("Destination: " + destination);
        if (msg == null || destination == null) {
            logger.error("null: " + msg + " " + destination);
        } else {
            if (destination.equals(self)) deliverMessageIn(new MessageInEvent(new BabelMessage(msg, (short)-1, (short)-1), self, peerChannel));
            else sendMessage(msg, destination);
        }
    }
}
