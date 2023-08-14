package TPOChain;

import TPOChain.ipc.SubmitOrderMsg;
import TPOChain.ipc.SubmitReadRequest;
import TPOChain.notifications.*;
import TPOChain.utils.*;
import common.values.*;
import org.apache.commons.lang3.tuple.Triple;
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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
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
    
    
    
    
    //注意对各项超时之间的依赖关系：noOp_sended_timeout  leader_timeout  reconnect_timeout 关系到时钟线程几组的设置，添加顺序有关
    /**
     * 下面是具体取值
     * leader_timeout=5000
     * noop_interval=100
     * join_timeout=3000
     * state_transfer_timeout=5000
     * reconnect_time=1000
     * */

    
    

    /**
     * 节点的状态：在加入整个系统中的状态变迁
     * */
    public enum State {JOINING, WAITING_STATE_TRANSFER, ACTIVE}
    private TPOChainProto.State state;

    /**
     * 这是对系统全体成员的映射包含状态的成员列表(即物理链)，随时可以更新
     * */
    private Membership membership;

    /**
     * 已经建立连接的主机,连接是指向外的连接
     * */
    private final Set<Host> establishedConnections = new HashSet<>();
    
    /**
     * 代表着leader，SeqN是一个term,由<int,host>组成，而currentSN前面的Int，是term刚开始的实例号
     * */
    private Map.Entry<Integer, SeqN> currentSN;
    
    /**
     * 代表自身，左右相邻主机
     * */
    private final Host self;
    
    //主要是重新配前链，根据选定的Leader，从Membership生成以Leader为首节点的前链节点之后，之后依次接入后链节点
    private List<Host> members=new ArrayList<>();
    // nextOKFront和nextOkBack是静态的，，而nextOk是Leader选定之后，nextOK从nextOKFront和NextOkBack中选定一个
    private Host Head=null;//使用
    private Host nextOK;
    //标记nextOk节点是否连接
    private boolean  nextOkConnnected=false;

    // 打算废弃物理链，改用逻辑链
    /**
     * 消息的下一个节点
     * */
    //private Host nextOkFront;
    //private Host nextOkBack;

    

    /**
     * Leader节点使用 是否为leader,职能排序
     * */
    private boolean amQuorumLeader;
    // TODO: 2023/8/1  amQuorumLeader 会和support Leader不一致吗？
    //  可能会，因为接收到更高序好的prepare之后， 
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

    

    
    



    // TODO: 2023/7/31   在成为候选者之后有新Leader比自身大那么暂存的排序命令需要转发至新Leader,而且只能附加在Prepare
    //  新Leader从prepareOk消息其中拿出  暂存的排序 和  已经排完序的accept消息
    //打算废弃？不能废弃，因为 在系统处于状态不稳定(即无leader时)时，暂存一些重要命令，如其他节点发过来的排序命令
    /**
     * 在竞选leader时即候选者时暂存的命令
     * */
    private final Queue<SortValue> waitingAppOps = new LinkedList<>();
    
    
    //成员添加命令呢？ 删除命令，这个旧领导可以不传给新leader，因为新Leader在当选成功后，会重新删掉已经断开连接的节点
    // TODO: 2023/8/2 关于添加成员的命令也需要转发，删除成员的命令不需要转发了，也附带在prepare之中
    private final Queue<MembershipOp> waitingMembershipOps = new LinkedList<>();
    
    
    
    

    /**
     * 加入节点时需要的一些配置
     * */

    // TODO: 2023/8/2   新加入节点也要 申请一份局部日志 和他的 局部日志表，先判断是否已经存在：出现这种情况是 被删除节点重新加入集群
    //   在leader宕机时，只是前链节点转发新的客户端消息不能进行，老消息可以继续进行
    
    /**
     * 在节点加入系统中的那个实例
     */
    private int joiningInstanceCl;
    private long joinTimer = -1;
    private long stateTransferTimer = -1;

    // TODO: 2023/7/21   新加入节点除了获取系统的状态，还要获取系统的membership，以及前链节点，以及哪些节点正在被删除状态
    //  在joinsuccessMsg添加这些信息：添加状态


    // TODO: 2023/7/21 除了状态还有日志从execute——>ack的实例(局部日志和全局日志)
    //  以及ack——>accept 
    //  将执行的实例和状态对应的
    //   

    // TODO: 2023/7/21 考虑命令执行 和 命令的分发和存储分离带来的，那么每个节点所发的状态都不一样 
    //  那么除了状态和要分发的日志是配套的，   


    // TODO: 2023/7/21 新加入节点的，消息队列的设置因为新加入节点要接受两部分消息：一个是和状态配套的日志消息 一个是加入之后新局部日志和全局日志信息来的  
    
    
    /**
     * 暂存一些系统的节点状态，好让新加入节点装载
     * */
     //Dynamic membership
    //TODO eventually forget stored states... (irrelevant for experiments)
    //Waiting for application to generate snapshot (boolean represents if we should send as soon as ready)
    private final Map<Host, MutablePair<Integer, Boolean>> pendingSnapshots = new HashMap<>();

    
    //---------------------新加入节点方的数据结构------------------------------
    //Application-generated snapshots
    // java中的三元组 Triple<Integer, String, Boolean> triplet;
    private final Map<Host, Pair<Integer, byte[]>> storedSnapshots = new HashMap<>();
    
    //给我这个节点发送 joinSuccess消息的节点  List of JoinSuccess (nodes who generated snapshots for me)
    private final Queue<Host> hostsWithSnapshot = new LinkedList<>();
    
    //这个需要 这里存储的是全局要处理的信息
    /**
     *暂存节点处于joining，暂存从前驱节点过来的批处理信息，等待状态变为active，再进行处理
     */
    private final Queue<SortValue> bufferedOps = new LinkedList<>();

    // TODO: 2023/7/21 加入的状态应该包含它这个状态执行到全局序列哪的序号，后续根据这个指标进行执行
    // TODO: 2023/7/21 加入之后关于执行线程和回收线程所需要的几个值的通道也需要初始化  
    private Map.Entry<Integer, byte[]> receivedState = null;
    // TODO: 2023/7/31 状态和后续的execute------>joininstacneIN 之间的消息(存储在)是配套的，
    //  可以添加一个map集合，或Pair 显示状态和消息的组合，当两者齐全之后进行状态的装载和消息的填充
    //   也添加消息的的缓存
    
    
    // TODO: 2023/7/21 关于日志状态的遗忘，因为时间，先不做，后续在优化，因为节点故障也不多浪费的存储也不多
    private  List<Host>  canForgetSoredStateTarget;
    


    
    
    
    
    
    //关于创建TCP通道需要使用的几个参数

    /**
     *关于节点的tcp通道信息
     */
    private int peerChannel;

    /**
     * java中net框架所需要的数据结构
     * */
    private final EventLoopGroup workerGroup;

    /**
     *根据初始的节点参数生成的固定的主机链表
     * */
    private final LinkedList<Host> seeds;

    
    
    
    //--------------物理链；传递给下层Data层的成员指标，代表Data端的主机映射，
    
    private  List<InetAddress>  frontChain=new ArrayList<>();
    private  List<InetAddress>  backChain =new ArrayList<>();




    
    
    /**
     * 在各个节点暂存的信息和全局存储的信息
     */
    private static final int INITIAL_MAP_SIZE = 100000;
    
    /**
     * 全局日志
     */
    private final Map<Integer, InstanceStateCL> globalinstances=new HashMap<>(INITIAL_MAP_SIZE);

    
    /**
     * leader这是主要排序阶段使用
     * */
    private int highestGarbageCollectionCl=-1;
    private int highestAcknowledgedInstanceCl=-1;
    private int highestExecuteInstanceCl= -1 ;
    private int highestDecidedInstanceCl = -1;
    private int highestAcceptedInstanceCl = -1;

    //leader使用，即使发生leader交换时，由新leader接棒：由新leader根据所有节点的accept信息来推断出lastAcceptSent
    private int lastAcceptSentCl = -1;

    

    // 加入一个执行锁:为什么加入这个，因为添加状态需要读取状态，而执行线程需要更新状态，两者冲突
    // 做法：将锁放入最小的执行单元，因为执行线程可能等待消息队列而阻塞整个线程，锁一直释放不了，将锁放入最小的单元，避免
    private final Object  executeLock=new Object();
    
    
    
    //执行实例守护线程
    private Thread executionSortThread;
    // decide的值的变化，避开对一个值的加锁
    private BlockingQueue<Integer> olddecidequeue= new LinkedBlockingQueue<>();
    //全局实例的队列
    private BlockingQueue<InstanceStateCL> olderInstanceClQueue= new LinkedBlockingQueue<>();
    
    
    
    
    //GC 
    private Thread executionGabageCollectionThread;
    private BlockingQueue<Integer> olddackqueue= new LinkedBlockingQueue<>();
    private BlockingQueue<Integer> olddexecutequeue= new LinkedBlockingQueue<>();
    
    //Leader宕机也需要向Data层发送通知， 或者不需要，因为Data层连接Leader的通道已经断开，不需要外部响应(除非Leader没宕机，只是被删除) 
    
    /**
     *构造函数
     */
    public TPOChainProto(Properties props, EventLoopGroup workerGroup) throws UnknownHostException {
        super(PROTOCOL_NAME, PROTOCOL_ID /*, new BetterEventPriorityQueue()*/);
        
        this.workerGroup = workerGroup;
        
        // Map.Entry<Integer, SeqN> currentSN   是一个单键值对
        currentSN = new AbstractMap.SimpleEntry<>(-1, new SeqN(-1, null));
        
        
        amQuorumLeader = false;//默认不是leader
        
        
        self = new Host(InetAddress.getByName(props.getProperty(ADDRESS_KEY)),
                Integer.parseInt(props.getProperty(PORT_KEY)));
        nextOK=null;
        nextOkConnnected=false;
        

        //不管是排序还是分发消息：标记下一个节点
        //nextOkFront =null;
        //nextOkBack=null;
        
        
        
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
        // 申请一个TCP通道
        Properties peerProps = new Properties();
        peerProps.put(TCPChannel.ADDRESS_KEY, props.getProperty(ADDRESS_KEY));
        peerProps.setProperty(TCPChannel.PORT_KEY, props.getProperty(PORT_KEY));
        peerProps.put(TCPChannel.WORKER_GROUP_KEY, workerGroup);
        peerChannel = createChannel(TCPChannel.NAME, peerProps);
        setDefaultChannel(peerChannel);

        

        registerMessageSerializer(peerChannel, AcceptAckCLMsg.MSG_CODE, AcceptAckCLMsg.serializer);
        registerMessageSerializer(peerChannel, AcceptCLMsg.MSG_CODE, AcceptCLMsg.serializer);
        registerMessageSerializer(peerChannel, DecidedMsg.MSG_CODE, DecidedMsg.serializer);
        registerMessageSerializer(peerChannel, DecidedCLMsg.MSG_CODE, DecidedCLMsg.serializer);
        registerMessageSerializer(peerChannel, JoinRequestMsg.MSG_CODE, JoinRequestMsg.serializer);
        registerMessageSerializer(peerChannel, JoinSuccessMsg.MSG_CODE, JoinSuccessMsg.serializer);
        registerMessageSerializer(peerChannel, MembershipOpRequestMsg.MSG_CODE, MembershipOpRequestMsg.serializer);
        registerMessageSerializer(peerChannel, PrepareMsg.MSG_CODE, PrepareMsg.serializer);
        registerMessageSerializer(peerChannel, PrepareOkMsg.MSG_CODE, PrepareOkMsg.serializer);
        registerMessageSerializer(peerChannel, StateRequestMsg.MSG_CODE, StateRequestMsg.serializer);
        registerMessageSerializer(peerChannel, StateTransferMsg.MSG_CODE, StateTransferMsg.serializer);
        registerMessageSerializer(peerChannel, UnaffiliatedMsg.MSG_CODE, UnaffiliatedMsg.serializer);
        //Data层的排序消息
        registerMessageSerializer(peerChannel, OrderMSg.MSG_CODE, OrderMSg.serializer);
        //新leader成功选举
        registerMessageSerializer(peerChannel, ElectionSuccessMsg.MSG_CODE, ElectionSuccessMsg.serializer);
        
        
        
        registerMessageHandler(peerChannel, UnaffiliatedMsg.MSG_CODE, this::uponUnaffiliatedMsg, this::uponMessageFailed);
        //新加
        registerMessageHandler(peerChannel, AcceptAckCLMsg.MSG_CODE, this::uponAcceptAckCLMsg, this::uponMessageFailed);
        //新加
        registerMessageHandler(peerChannel, AcceptCLMsg.MSG_CODE, this::uponAcceptCLMsg, this::uponMessageFailed);
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
        //新加
        registerMessageHandler(peerChannel, ElectionSuccessMsg.MSG_CODE,
                this::uponElectionSuccessMsg, this::uponMessageFailed);
        
        
        
        
        registerChannelEventHandler(peerChannel, InConnectionDown.EVENT_ID, this::uponInConnectionDown);
        registerChannelEventHandler(peerChannel, InConnectionUp.EVENT_ID, this::uponInConnectionUp);
        registerChannelEventHandler(peerChannel, OutConnectionDown.EVENT_ID, this::uponOutConnectionDown);
        registerChannelEventHandler(peerChannel, OutConnectionUp.EVENT_ID, this::uponOutConnectionUp);
        registerChannelEventHandler(peerChannel, OutConnectionFailed.EVENT_ID, this::uponOutConnectionFailed);

        
        

        registerTimerHandler(LeaderTimer.TIMER_ID, this::onLeaderTimer);
        registerTimerHandler(NoOpTimer.TIMER_ID, this::onNoOpTimer);
        registerTimerHandler(ReconnectTimer.TIMER_ID, this::onReconnectTimer);
        registerTimerHandler(JoinTimer.TIMER_ID, this::onJoinTimer);
        registerTimerHandler(StateTransferTimer.TIMER_ID, this::onStateTransferTimer);
        // 新加  ForgetStateTimer  删除节点存储的state
        registerTimerHandler(ForgetStateTimer.TIMER_ID, this:: onForgetStateTimer);
       
        

        //接收从front的   读请求
        registerRequestHandler(SubmitReadRequest.REQUEST_ID, this::onSubmitRead);
        // 接收从data层的排序request
        registerRequestHandler(SubmitOrderMsg.REQUEST_ID, this::onSubmitOrderMsg);
        
        //接收来自front的 状态 答复
        registerReplyHandler(DeliverSnapshotReply.REPLY_ID, this::onDeliverSnapshot);

        
        // 向Data层通知当前节点的状态
        triggerStateChange();


        //在这里只是声明初始化执行和回收线程
        this.executionSortThread = new Thread(this::execLoop,  "---execute" );
        this.executionGabageCollectionThread=new Thread(this::gcLoop,"---gc");
        
        
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
            // TODO: 2023/8/3 记得在加入成功之后进行开启执行线程和回收线程 
        }
        logger.info("TPOChainProto: " + membership + " qs " + QUORUM_SIZE);
    }
    
    
    
    
    
    
    // --------------------------触发Leader选举
    
    /**
     * 初始状态为Active开始启动节点
     * */
    private void setupInitialState(List<Host> members, int instanceNumber) {
        //传进来的参数setupInitialState(seeds, -1);
        //这里根据传进来的顺序， 已经将前链节点和后链节点分清楚出了
        membership = new Membership(members, QUORUM_SIZE);

        
        //next:因为需要全连接作为成员管控，发送prepare  prepareok   electionok消息
        members.stream().filter(h -> !h.equals(self)).forEach(this::openConnection);
        joiningInstanceCl = highestGarbageCollectionCl=highestAcceptedInstanceCl = highestExecuteInstanceCl =highestAcknowledgedInstanceCl = highestDecidedInstanceCl =
                instanceNumber;// instanceNumber初始为-1,但对于
        
        //对命令分发进行初始化配置
        for (Host tempHost:members) {
            //如果原配置表中不存在，再添加新配置表和日志表，因为有节点被删除后重新加入节点 
            InetAddress temp=tempHost.getAddress();
            
            if (!hostConfigureMap.containsKey(temp)){
                //对分发的配置进行初始化  hostConfigureMap Map<Host,RuntimeConfigure>
                RuntimeConfigure runtimeConfigure=new RuntimeConfigure();
                hostConfigureMap.put(temp,runtimeConfigure);
            }
            
            if (!instances.containsKey(temp)){
                Map<Integer, InstanceState>  ins=new HashMap<>(50000);
                instances.put(temp,ins);
            }
            
            if (!hostMessageQueue.containsKey(temp)){
                BlockingQueue<InstanceState>  que=new LinkedBlockingQueue<>();
                hostMessageQueue.put(temp,que);
            }
        }
        //主要是分发的相关配置完成，通知Data层初始化完成
        triggerInitializeCompletedNotification();

        // 将更新物理前链和后链节点，并将链通知传递到Data层
        changeDataFrontAndBackChain();
       
        //当判断当前节点是否为前链节点，是的话，执行
        if(membership.frontChainContain(self)){
            if (logger.isDebugEnabled()){
                logger.debug("我是前链节点，开始做前链初始操作frontChainNodeAction()");
            }
            frontChainNodeAction();
        }
        
        //开启执行和回收线程
        this.executionSortThread.start();
        this.executionGabageCollectionThread.start();
    }

    
    /**
     * 改变物理链,将前链和后链节点传递到Data层,生成通道链
    */
    private void  changeDataFrontAndBackChain(){
        // 清空前链原来的内容
        frontChain.clear();
        // 复制前链
        List<Host> tmpfrontChain= membership.getFrontChain();
        for (int i=0;i<tmpfrontChain.size();i++){
            frontChain.add(tmpfrontChain.get(i).getAddress());
        }
        // 清空后链原来的内容
        backChain.clear();
        //复制后链
        List<Host> tmpbackChain= membership.getBackChain();
        for (int i=0;i<tmpbackChain.size();i++){
            backChain.add(tmpbackChain.get(i).getAddress());
        }
        //触发Data层的列表改变，传递的是物理链，Data转换之后成为通道链
        triggerFrontChainChange();
    }
    
    
    /**
     *前链节点执行的pre操作，开启时钟超时时钟
    */
    private  void  frontChainNodeAction(){
        //拥有了设置选举leader的资格，设置领导超时处理
        leaderTimeoutTimer = setupPeriodicTimer(LeaderTimer.instance, LEADER_TIMEOUT, LEADER_TIMEOUT / 3);
        lastLeaderOp = System.currentTimeMillis();
        
    }
    
    
    /**  抑制一些候选举leader的情况：只有在与leader相距(F+1)/2 +1
    * 有问题：在leader故障时，leader与后链首节点交换了位置
    * 除了初始情况下supportedLeader()=null;正常超时怎么解决
    * 对上面情况做出解答： 所有节点都可以竞选新leader
    */ 
    /**
     * 在leadertimer超时争取当leader
     */
    private void onLeaderTimer(LeaderTimer timer, long timerId) {
        // 在进入超时,说明失去leader之间的联系
        if (!amQuorumLeader && (System.currentTimeMillis() - lastLeaderOp > LEADER_TIMEOUT) &&
                (supportedLeader() == null //初始为空 ，或新加入节点
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
        

        //为什么是ack信息，保证日志的连续
        //InstanceStateCL instance = globalinstances.computeIfAbsent(highestAcknowledgedInstanceCl + 1, InstanceStateCL::new);
        InstanceStateCL instance;
        if (!globalinstances.containsKey(highestAcknowledgedInstanceCl + 1)) {
            instance=new InstanceStateCL(highestAcknowledgedInstanceCl + 1);
            globalinstances.put(highestAcknowledgedInstanceCl + 1, instance);
        }else {
            instance=globalinstances.get(highestAcknowledgedInstanceCl + 1);
        }
        
        //private Map.Entry<Integer, SeqN> currentSN是
        //currentSN初始值是new AbstractMap.SimpleEntry<>(-1, new SeqN(-1, null));
        SeqN newSeqN = new SeqN(currentSN.getValue().getCounter() + 1, self);
        //生成对应序号的prepareok的Map
        instance.prepareResponses.put(newSeqN, new HashSet<>());
        
        
        PrepareMsg pMsg = new PrepareMsg(instance.iN, newSeqN);
        //这是对所有节点包括自己发送prepare消息
        // TODO: 2023/8/2 因为只有前链节点能选举，所以Maybe只需要向前链节点发送prepare 
        membership.getMembers().forEach(h -> sendOrEnqueue(pMsg, h));
    }
    
    
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
                lastLeaderOp = System.currentTimeMillis();// 这个是必须的，防止再竞争Leader
                //
            } else//当msg的SN小于等于当前leader标记，丢弃
                logger.warn("Discarding prepare since sN <= hP");
        } else { //Respond with decided message  主要对于系统中信息滞后的节点进行更新
            //若发送prepare消息的节点比接收prepare消息的节点信息落后，可以发送已经DecidedMsg信息
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

    // 这个调用处有发放大的prepare消息处
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
            // 清空队列
            waitingAppOps.clear();
            waitingMembershipOps.clear();
        }
        //不为leader的就更新个leader的标志SN
        changeLogicChain();
    }
    
    
    /** 一种情况是：当一个处于中间节点状态的节点向比它先进的节点  或  落后节点发送prepare消息，先进节点回复
     *  decideCLMsg，落后节点回复  prepareoK消息，怎么处理
     */
    //此节点落后于其他节点，对排序信息进行处理
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
                    //highestAcceptedInstanceCl++;// 原来是这个
                    highestAcceptedInstanceCl=acceptedInstance.iN;
                    assert acceptedInstance.iN == highestAcceptedInstanceCl;
                }
            }else {//不需要管
                //到达这里 表明这个节点已经有了对应的实例，且那个实例的leader term大于
                // 消息中的term，那么不对Msg中的消息进行更新
            }
        }
        
        /**Notice 最坏的情况下：若每次都有新的节点打破原来的prepareok的消息，那最后Host的IP地址最大将是leader
         */

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
    
    
    //Notice 在leader选举成功，前链节点才能开始工作
    /**
     * I am leader now! @ instance
     * */
    private void becomeLeader(int instanceNumber) {
        //发送竞选成功消息
        ElectionSuccessMsg pMsg=new ElectionSuccessMsg(instanceNumber,currentSN.getValue());
        membership.getMembers().forEach(h -> sendOrEnqueue(pMsg, h));
        
        // 设定是领导标记
        amQuorumLeader = true;
        noOpTimerCL = setupPeriodicTimer(NoOpTimer.instance, NOOP_SEND_INTERVAL, Math.max(NOOP_SEND_INTERVAL / 3,1));
        //新leader会发送noop消息，激活前链节点的转发消息
        logger.info("I am leader now! @ instance " + instanceNumber);
        
        // Leader需要这个调整逻辑控制链
        changeLogicChain();
        
        /**
         * 传递已经接收到accpted消息    Propagate received accepted ops
         * */
        for (int i = instanceNumber; i <= highestAcceptedInstanceCl; i++) {
            if (logger.isDebugEnabled()) 
                logger.debug("Propagating received operations: " + i);
            
            InstanceStateCL aI = globalinstances.get(i);
            
            assert aI.acceptedValue != null;
            assert aI.highestAccept != null;
            //goes to the end of the queue
            this.deliverMessageIn(new MessageInEvent(new BabelMessage(new AcceptCLMsg(i, currentSN.getValue(), (short) 0,
                    aI.acceptedValue, highestAcknowledgedInstanceCl), (short)-1, (short)-1), self, peerChannel));
        }
        //标记leader发送到下一个节点到哪了
        lastAcceptSentCl = highestAcceptedInstanceCl;
        
        
        //若在之前的命令中就有要删除节点现在再对其删除，是不是应该进行判定，当leader删除节点不存在系统中，则跳过此条消息
        // 已经解决了，在leader的sendnextaccept中，如果删除不存在节点或添加已有节点，则转换为Noop消息
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
        
        
        //设置为0立马给其他节点发送消息,因为上面的条件不一定全部满足,需要立马发送noop消息
        lastAcceptTimeCl = 0;
        
        
        //上面的都从系统中收集是旧消息，下面是新加入的消息需要传播
        
        //这里的暂存的成员消息都是添加节点,只有在系统中处于选举状态，这个才有意义，
        /**
         * 对咱暂存在成员操作和App操作进行发送
         * */
        PaxosValue nextOp;
        if (!waitingMembershipOps.isEmpty()){
            while ((nextOp = waitingMembershipOps.poll()) != null) {
                sendNextAcceptCL(nextOp);
            } 
        }
        
        //处理在候选阶段的信息
        /**
        * 发送暂存的的排序消息
        * */
        if (! waitingAppOps.isEmpty()){
            while ((nextOp = waitingAppOps.poll()) != null) {
                sendNextAcceptCL(nextOp);
            }
        }
    }

    //TODO 应该判别发送的Term是否大于当前的Term，大于再接受，小于不接受
    /**
     * 在成功选举后，发送选举成功消息
     */
    private  void uponElectionSuccessMsg(ElectionSuccessMsg msg, Host from, short sourceProto, int channel) {
        if (logger.isDebugEnabled()){
            logger.debug("收到选举成功消息"+msg + " from:" + from);
        }
        setNewInstanceLeader(msg.iN, msg.sN);
        // 在候选阶段，不会通知Data新Leader，只有成功当选之后再通知Data层，所以不会再候选者阶段收到排序请求 

        // 向Data层传输新Leader
        triggerLeaderChange();
        //因为Leader已经选举出来，从物理链中可以生成逻辑控制链， 
        changeLogicChain();
        // TODO: 2023/8/2 应该这里改变，应该在Leader选举成功消息调用，当然这里也可以调用 
        // 改变节点的挂载，将后链节点挂载在前链除了leader节点的前链节点
        triggerMembershipChangeNotification();
    }
    
    
    /**
     * 处理leader和其他节点呼吸事件
     * */
    private  void onNoOpTimer(NoOpTimer timer, long timerId) {
        //设置时钟的触发时间，但在内部使用if判断才执行对应指令
        if (amQuorumLeader) {
            assert waitingAppOps.isEmpty() && waitingMembershipOps.isEmpty();
            // TODO: 2023/8/14  
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

    // TODO: 2023/8/3 什么时候会出现候选者：因为prepare发过来就相等于候选者，达到多数才属于Leader,
    //  那接收prepare的节点只是以为是Leader，其实只是候选者，只有在拿到选举成功消息，才能说明绑定的是Leader
    
    // TODO: 2023/8/2 在Leader更新成功调用，缺少调用： 在节点被删除  被添加处使用
    /**
     * 每当Leader发生变化或删除或添加节点时调用这个
     */
    private  void changeLogicChain(){
        Host leader=supportedLeader();
        int indexleader=membership.frontIndexOf(leader);

        List<Host> fronttemp=membership.getFrontChain();
        for (int i=0;i<frontChain.size();i++){
            int index=(indexleader+i)%frontChain.size();
            members.add(fronttemp.get(index));
        }
        Head=members.get(0);
        // 这个是不包含标记删除节点的后链
        List<Host> backtemp=membership.getBackChain();
        members.addAll(backtemp);


        //--------------重新生成逻辑链之后要改变nextok的连接
        int indexself=members.indexOf(self);
        Host newnextok=members.get((indexself+1)%members.size());
        if (nextOK == null || !nextOK.equals(newnextok)) {
            // 因为control层本身是全连接，不能关闭也不能开启(因为本身是开启)，nextok节点
            //当满足这个条件，
            if (nextOK != null && !nextOK.getAddress().equals(self.getAddress())) {
                nextOkConnnected = false;
            }
            //Update and open to new writesTo
            nextOK =newnextok;
            logger.info("New nextok connected: " + nextOK.getAddress());

            // 因为是全连接，当nextok更换时不会调用uponoutConnnectionup()即自动转发ack->accept的消息，需要手动进行转发ack
            if (state==State.JOINING){
                return;
            }// 转发所有ack->accept的全局排序日志到nextok节点 
            for (int i = highestAcknowledgedInstanceCl + 1; i <= highestAcceptedInstanceCl; i++) {
                forwardCL(globalinstances.get(i));
            }
        }
    }
    
    
    
    
    
    

    //--------------------Notification ----通知Front和Data层

    /**
     * 生成局部配置信息和局部日志表和日志队列 
     * */
    private void   triggerInitializeCompletedNotification(){
        triggerNotification(new InitializeCompletedNotification());
    }

    // TODO: 2023/8/4 以下这条通知，应该在哪里调用，是Prepare setNewLeader  还是  ElectionSuccess 
    /**
     * 向front层发送成员改变通知
     * */
    private void triggerMembershipChangeNotification() {
        //可以设置固定的后链与对应的前链节点挂载，还是在Leader选举之后新的后链重新挂载前链的非Leader节点 
        // 理论上还是动态挂载更好
        
        //这个方法的调用处，已经说明supportleader肯定不为null
        Host  host=membership.appendFrontChainNode(self,supportedLeader());
        //触发前链时附加一个多少前链标记 
        if(host!=null){// 说明是后链节点，有附加的前链节点
            triggerNotification(new MembershipChange(
                    membership.getMembers().stream().map(Host::getAddress).collect(Collectors.toList()),
                    null, host.getAddress(), null,-1));
            if (logger.isDebugEnabled())
                logger.debug("当前节点是后链"+self.toString()+"挂载在"+host.toString());
        }else{// 自己就是前链节点，同时通知Front层使用哪个通道
            triggerNotification(new MembershipChange(
                    membership.getMembers().stream().map(Host::getAddress).collect(Collectors.toList()),
                    null, self.getAddress(), null,membership.frontIndexOf(self)));
            if (logger.isDebugEnabled()){
                logger.info("当前节点是前链"+self.toString());
            }
        }
    }
    
    
    /**
     * 向Data层通知物理链，在初始化中调用和前后链发生改变时调用
     */
    private  void  triggerFrontChainChange(){
        //logger.info("前链"+frontChain+"后链是"+backChain);
        // 物理链发生变化
        triggerNotification(new FrontChainNotification(frontChain,backChain));
    }

    
    /**
     * 在选举成功后向data层leader改变通知，重新指向新leader
     * */
    private  void  triggerLeaderChange(){
        triggerNotification(new LeaderNotification(supportedLeader()));
    }


    // FIXME: 2023/8/3 除了初始配置，应该在状态更新之后调用调用处写清，为什么需要这个，因为再状态是Join,是不能
    /**
     *  data层和control层状态一致，特别在outConnectionUp()中根据这个状态，判断是否转发消息 
     */
    private  void  triggerStateChange(){
        // 这个不需要序列和反序列化，因为是进程间通信，通知所有的data层的状态
        triggerNotification(new StateNotification(state));
    }
    
    
    
    
    
    
    
    
    
/**------leader的重新发送排序命令，也可以包括成员添加，删除操作--**/

 
    /**
     *  其他节点的data端发过来OrderMsg信息，在这里接收
     */
    private void uponOrderMSg(OrderMSg msg, Host from, short sourceProto, int channel) {
        // 还有一种情况，当是候选者 ？已解决：排序请求不需要有候选者，因为Data层在旧Leader死机时，停止发送排序请求
        if (amQuorumLeader){//只有leader才能处理这个排序请求
            //if (logger.isDebugEnabled()){
            //    logger.debug("我是leader,收到"+from+"的"+msg+"开始sendNextAcceptCL()");
            //}
            sendNextAcceptCL(new SortValue(msg.node,msg.iN));
        }else if (supportedLeader().equals(self)){// 当为候选者
            // 将排序请求缓存
            waitingAppOps.add(new SortValue(msg.node,msg.iN));
        } else {//对消息进行转发,转发到leader
            sendOrEnqueue(msg,supportedLeader());
            logger.warn("not leader but receive OrderMSg msg");
        }
    }


    /**
     * 只有自己为leader时,自己的data端调用这个
     */
    public void onSubmitOrderMsg(SubmitOrderMsg not, short from){
        OrderMSg msg=not.getOrdermsg();
        if (amQuorumLeader){//只有leader才能处理这个排序请求
            sendNextAcceptCL(new SortValue(msg.node,msg.iN));
        } else if (supportedLeader().equals(self)){// 当为候选者
            // 将排序请求缓存 
            if (logger.isDebugEnabled()){
                logger.debug("暂存从自己Data端过来的消息");
            }
            waitingAppOps.add(new SortValue(msg.node,msg.iN));
        } else {//对消息进行转发,转发到leader
            sendOrEnqueue(msg,supportedLeader());
            logger.warn("not leader but receive SubmitOrderMsg ipc");
        }
    }

    
    // TODO: 2023/7/19  noop太多，也影响性能，因为无效信息太多了，挤压正常信息的处理 ;但是太少也不行(读依附在一定数量的noop消息)同时noop附带的前链节点上次的ack请求，然后执行，是否可以将noop与其他消息并行处理
    
    /**
     * 生成排序消息发给
     * **/
    private void sendNextAcceptCL(PaxosValue val) {
        // TODO: 2023/7/27 在control端可以判定这个之前的实例是不是发送排序了，没有则生成之前和这次的实例 
        //  若这个实例长时间不存在则空过这个的执行
        
        
        InstanceStateCL instance;
        if (!globalinstances.containsKey(lastAcceptSentCl + 1)) {
            instance=new InstanceStateCL(lastAcceptSentCl + 1);
            globalinstances.put(lastAcceptSentCl + 1, instance);
        }else {
            instance=globalinstances.get(lastAcceptSentCl + 1);
        }
        
        //InstanceStateCL instance = globalinstances.computeIfAbsent(lastAcceptSentCl + 1, InstanceStateCL::new);
        
        PaxosValue nextValue = null;
        if (val.type== PaxosValue.Type.SORT){
            nextValue =val;
        }else if ( val.type== PaxosValue.Type.NO_OP){
            //nextValue = new NoOpValue();
            nextValue=val;
        } else if (val.type == PaxosValue.Type.MEMBERSHIP) {
            MembershipOp next = (MembershipOp) val;
            if ((next.opType == MembershipOp.OpType.REMOVE && !membership.contains(next.affectedHost)) ||
                    (next.opType == MembershipOp.OpType.ADD && membership.contains(next.affectedHost))) {
                logger.warn("Duplicate or invalid mOp: " + next);
                nextValue = new NoOpValue();
            } else {//若是正常的添加，删除
                // fixme 这里需要修改，直接添加吧，不需要指定添加的位置，由集群节点映射表自己处理
                if (next.opType == MembershipOp.OpType.ADD) {
                    // TODO: 2023/8/4 应该加入节点，未被标记的链尾
                    //这里指定了添加位置: 后链链尾位置，其实应该是后链链尾的后面，
                    next = MembershipOp.AddOp(next.affectedHost,membership.indexOf(membership.getBackChainTail()));
                }
                nextValue = next;
            }
        }

        //this.uponAcceptCLMsg(new AcceptCLMsg(instance.iN, currentSN.getValue(),
        //        (short) 0, nextValue, highestAcknowledgedInstanceCl), self, this.getProtoId(), peerChannel);

        //直接传递处理通道中
        deliverMessageIn(new MessageInEvent(new BabelMessage(new AcceptCLMsg(instance.iN, currentSN.getValue(),
                (short) 0, nextValue, highestAcknowledgedInstanceCl), (short)-1, (short)-1), self, peerChannel));
        
        //更新发送标记
        lastAcceptSentCl = instance.iN;
        //更新上次操作时间
        lastAcceptTimeCl = System.currentTimeMillis();
    }
    
    
    
    // 上面那三个方法是Leader节点使用，下面的是
    private void uponAcceptCLMsg(AcceptCLMsg msg, Host from, short sourceProto, int channel) {
        //-------无效信息：对不在系统中的节点发送未定义消息让其重新加入系统
        if(!membership.contains(from)){
            logger.warn("Received msg from unaffiliated host " + from);
            sendMessage(new UnaffiliatedMsg(), from, TCPChannel.CONNECTION_IN);
            return;
        }
        
        // 这里访问的同时，不能对globalinstances修改，不然会触发并发修改异常
        InstanceStateCL instance;
        if (!globalinstances.containsKey(msg.iN)) {
            instance=new InstanceStateCL(msg.iN);
            globalinstances.put(msg.iN, instance);
        }else {
            instance=globalinstances.get(msg.iN);
        }
        
        
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

        
        // TODO: 2023/6/26 因为leader选举成功会向所有节点发送选举成功消息，所以这个不需要了 
        //设置msg.sN.getNode()为新支持的leader
        //if (msg.sN.greaterThan(currentSN.getValue())){
        //    setNewInstanceLeader(msg.iN, msg.sN);
        //}
        

        //在这里可能取消了删除节点，后续真的删除节点时也在后面的mark中重新加入节点
        maybeCancelPendingRemoval(instance.acceptedValue);
        instance.accept(msg.sN, msg.value, (short) (msg.nodeCounter + 1));//进行对实例的投票确定
        
        
        //更新highestAcceptedInstance信息
        if (highestAcceptedInstanceCl < instance.iN) {
            highestAcceptedInstanceCl=instance.iN;
        }

        
        if ((instance.acceptedValue.type == PaxosValue.Type.MEMBERSHIP) &&
                (((MembershipOp) instance.acceptedValue).opType == MembershipOp.OpType.REMOVE))
            markForRemoval(instance);//对节点进行标记
        
        forwardCL(instance);//转发下一个节点
        
        
        // 为什么需要前者，因为新leader选举出来会重发一些消息，所以需要判断instance.isDecided()
        if (!instance.isDecided() && instance.counter >= QUORUM_SIZE) //We have quorum!
            decideAndExecuteCL(instance);//决定并执行实例

        // 将实例放入待执行队列中
        olderInstanceClQueue.add(instance);
        
        if (msg.ack>highestAcknowledgedInstanceCl)
             ackInstanceCL(msg.ack);//对于之前的实例进行ack并进行垃圾收集
        
        //接收了此次消息 进行更新领导操作时间
        lastLeaderOp = System.currentTimeMillis();
    }

    
    /**
     *标记要删除的节点
     * */
    private void markForRemoval(InstanceStateCL inst) {
        // TODO: 2023/8/3  应该修改删除物理链 从而触发逻辑链的修改,之后需要将转发
        // 分情况讨论： 后链节点   前链节点非Leader    Leader节点(不会，Leader不会发出自己删除自己的命令)    
        MembershipOp op = (MembershipOp) inst.acceptedValue;
        //这里封装了实现细节：实际改变了物理链：那么需要重新调整 逻辑控制链和数据通道链；Leader宕机是不会走这个流程，那么只有两种情况
        membership.addToPendingRemoval(op.affectedHost);
          
        //通知应该改变Front层，
        triggerMembershipChangeNotification();
        // TODO: 2023/8/3 因为后链节点的Front挂载在对应前链节点，当 
        
        //通知改变Data层，这个是更新FrontChain和BackChain，之后通知Data层
        changeDataFrontAndBackChain();
    }
    
    
    /**
     * 转发accept信息给下一个节点或ack到Leader
     * */
    // 链只是传导排序信息，最后ack发给Leader，是新Leader还是
    private void forwardCL(InstanceStateCL inst) {
        // 转发消息应该循环完整条链  
        if (nextOK.equals(Head)){//使用逻辑链，表明已经传输到链尾
            // 发送ack前判断是否满足大多数的含义
            if (inst.counter < QUORUM_SIZE) {
                logger.error("Last living in chain cannot decide. Are f+1 nodes dead/inRemoval? "
                        + inst.counter);
                throw new AssertionError("Last living in chain cannot decide. " +
                        "Are f+1 nodes dead/inRemoval? " + inst.counter);
            }
            sendMessage(new AcceptAckCLMsg(inst.iN), supportedLeader());
        }else {//发送accept信息
            AcceptCLMsg msg = new AcceptCLMsg(inst.iN, inst.highestAccept, inst.counter, inst.acceptedValue,
                    highestAcknowledgedInstanceCl);
            sendMessage(msg, nextOK);
        }
    }
    
    
    /**
     * decide并执行Execute实例
     * */
    private void decideAndExecuteCL(InstanceStateCL instance) {
        //if (logger.isDebugEnabled())
        //    logger.debug("Decided: " + instance.iN + " - " + instance.acceptedValue);
        if (instance.acceptedValue.type == PaxosValue.Type.SORT) {
            if (state == TPOChainProto.State.ACTIVE){
                // 不做执行
            } else  //在节点处于加入join之后，暂存存放的批处理命令
                // TODO: 2023/6/1 这里不对，在加入节点时，不能执行这个 
                bufferedOps.add((SortValue) instance.acceptedValue);
        } else if (instance.acceptedValue.type == PaxosValue.Type.MEMBERSHIP) {
            executeMembershipOp(instance);
        } else if (instance.acceptedValue.type == PaxosValue.Type.NO_OP) {
            //nothing  
        }
        instance.markDecided();
        highestDecidedInstanceCl++;
        //放入旧的decide值
        olddecidequeue.add(highestDecidedInstanceCl);
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
            triggerFrontChainChange();
            closeConnection(target);
        } else if (o.opType == MembershipOp.OpType.ADD) {
            logger.info("Added to membership: " + target + " in inst " + instance.iN);
            membership.addMember(target);
            triggerMembershipChangeNotification();
            triggerFrontChainChange();
            //对next  重新赋值
            //nextOkFront=membership.nextLivingInFrontedChain(self);
            //nextOkBack=membership.nextLivingInBackChain(self);
            // TODO: 2023/8/3 对nextOk重新赋值
            
            
            openConnection(target);//

            if (!hostConfigureMap.containsKey(target)){
                //添加成功后，需要添加一新加节点的局部日志表 和  局部配置表
                RuntimeConfigure runtimeConfigure=new RuntimeConfigure();
                hostConfigureMap.put(target.getAddress(),runtimeConfigure);
            }

            if (!instances.containsKey(target)){
                //局部日志进行初始化 
                //Map<Host,Map<Integer, InstanceState>> instances 
                ConcurrentMap<Integer, InstanceState>  ins=new ConcurrentHashMap<>(50000);
                instances.put(target.getAddress(),ins);
            }
            
            
            if (state == TPOChainProto.State.ACTIVE) {
                //运行到这个方法说明已经满足大多数了
                sendMessage(new JoinSuccessMsg(instance.iN, instance.highestAccept, membership.shallowCopy()), target);
                //assert highestDecidedInstanceCl == instance.iN;
                //TODO need mechanism for joining node to inform nodes they can forget stored state
                pendingSnapshots.put(target, MutablePair.of(instance.iN, instance.counter == QUORUM_SIZE));
                sendRequest(new GetSnapshotRequest(target, instance.iN), TPOChainFront.PROTOCOL_ID_BASE);
            }
        }
    }

    
    //Notice  读应该是在附加的日志项被执行前执行，而不是之后或ack时执行
    /**
     * 对于ack包括以前的消息执行
     * */
    private void ackInstanceCL(int instanceN) {
        //处理重复消息或过时
        //if (instanceN<=highestAcknowledgedInstanceCl){
        //    logger.info("Discarding 重复 acceptackclmsg"+instanceN+"当前highestAcknowledgedInstanceCl是"+highestAcknowledgedInstanceCl);
        //    return;
        //}

        //For nodes in the first half of the chain only
        for (int i = highestDecidedInstanceCl + 1; i <= instanceN; i++) {
            InstanceStateCL ins = globalinstances.get(i);
            //assert !ins.isDecided();
            decideAndExecuteCL(ins);
            //assert highestDecidedInstanceCl == i;
        }
        
        // 进行ack信息的更新，使用++ 还是直接赋值好 
        highestAcknowledgedInstanceCl++;
        if (highestAcknowledgedInstanceCl % 200 == 0) {
            olddackqueue.add(highestAcknowledgedInstanceCl);
        }
    }
    
    
    /**
     * leader接收ack信息，对实例进行ack
     * */
    private void uponAcceptAckCLMsg(AcceptAckCLMsg msg, Host from, short sourceProto, int channel) {
        //if (logger.isDebugEnabled()){
        //    logger.debug("接收"+from+"的"+msg);
        //}
        
        if (msg.instanceNumber <= highestAcknowledgedInstanceCl) {
            logger.warn("Ignoring  acceptAckCL of Control  layer for old instance: " + msg);
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

    // TODO: 2023/8/4 如果消息队列中没有，再从日志中获取日志
    //TODO 一个命令可以回复客户端，只有在它以及它之前所有实例被ack时，才能回复客户端
    // 这是分布式系统的要求，因为原程序已经隐含了这个条件，通过判断出队列结构，FIFO，保证一个batch执行了，那么
    // 它之前的batch也执行了
    //TODO 只有一个实例的排序消息和分发消息都到齐了，才能进行执行
    // 所以不是同步的，所以有时候可能稍后延迟，需要两者齐全
    // 所以每次分发消息或 排序消息达到执行要求后，都要对从
    // 全局日志的已经ack到当前decided之间的消息进行扫描，达到命令执行的要求
    // 从execute到ack的开始执行
    private void execLoop(){
        while(true){
            //先得到旧值
            int olddecide;
            try {
                olddecide=this.olddecidequeue.take();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            //logger.info("旧decide"+olddecide);
            for (int i=highestExecuteInstanceCl+1;i<=olddecide;i++){
                //logger.info("执行值"+highestExecuteInstanceCl);
                //logger.info("i是"+i);
                InstanceStateCL instanceCLtemp;
                try {
                    instanceCLtemp=olderInstanceClQueue.take();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                //logger.info("从全局队列中的值"+instanceCLtemp.iN);
                //InstanceStateCL instanceCLtemp=globalinstances.get(i);
                // 要进行复制机状态的改变，必须获得锁，因为改变状态操作和加入节点申请状态冲突，所以加锁
                if (instanceCLtemp.acceptedValue.type!= PaxosValue.Type.SORT){
                    // 那消息可能是成员管理消息  也可能是noop消息
                    synchronized (executeLock){
                        //
                    }
                    highestExecuteInstanceCl++;
                    if (highestExecuteInstanceCl % 200 == 0) {
                        olddexecutequeue.add(highestExecuteInstanceCl);
                    }
                    //触发读
                    InstanceStateCL finalGlobalInstanceTemp = instanceCLtemp;
                    if(!instanceCLtemp.getAttachedReads().isEmpty()){
                        instanceCLtemp.getAttachedReads().forEach((k, v) -> sendReply(new ExecuteReadReply(v, finalGlobalInstanceTemp.iN), k));
                    }
                    if (logger.isDebugEnabled()){
                        logger.debug("当前执行的序号为"+i+"; 当前全局实例为"+instanceCLtemp.acceptedValue);
                    }
                }else {// 是排序消息
                    SortValue sortTarget= (SortValue)instanceCLtemp.acceptedValue;
                    InetAddress tempInetAddress=sortTarget.getNode().getAddress();
                    int  iNtarget=sortTarget.getiN();
                    InstanceState  intancetmp= null;
                    try {
                        intancetmp = hostMessageQueue.get(tempInetAddress).take();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    if (intancetmp.iN==iNtarget){
                        synchronized (executeLock){
                            triggerNotification(new ExecuteBatchNotification(((AppOpBatch) intancetmp.acceptedValue).getBatch()));
                        }
                        highestExecuteInstanceCl++;
                        if(iNtarget%200==0){
                            hostConfigureMap.get(tempInetAddress).executeFlagQueue.add(iNtarget);
                        }
                        if (highestExecuteInstanceCl % 200 == 0) {
                            olddexecutequeue.add(highestExecuteInstanceCl);
                        }
                        if (logger.isDebugEnabled()){
                            logger.debug("当前执行的序号为"+i+"; 当前全局实例为"+instanceCLtemp.acceptedValue);
                        }
                        //触发读
                        InstanceStateCL finalGlobalInstanceTemp = instanceCLtemp;
                        if(!instanceCLtemp.getAttachedReads().isEmpty()){
                            instanceCLtemp.getAttachedReads().forEach((k, v) -> sendReply(new ExecuteReadReply(v, finalGlobalInstanceTemp.iN), k));
                        }
                    } else {
                        throw new AssertionError("执行的不是所期望的 ");
                    }
                }
            }
        }
    }


    /**
     * 回收线程,回收全局日志的
     */
    private void gcLoop(){
        int olderexecute;
        int olderack;
        while(true){
            while(true){
                try {
                    olderexecute=olddexecutequeue.take();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                if (highestGarbageCollectionCl<olderexecute){
                    break;
                }
            }
            while(true){
                try {
                    olderack=olddackqueue.take();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                if (highestGarbageCollectionCl<olderack){
                    break;
                }
            }
            int min=Math.min(olderexecute,olderack);
            for (int i=highestGarbageCollectionCl+1;i<min;i++){
                globalinstances.remove(i);//删除全局实例
                highestGarbageCollectionCl++;
            }
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
            //通知Data的状态变化，
            triggerStateChange();
        }
        
    }
    
    
    // 失去前链节点
    private  void  cancelfrontChainNodeAction(){
        //取消拥有了设置选举leader的资格，取消设置领导超时处理
        cancelTimer(leaderTimeoutTimer)   ;
        lastLeaderOp = System.currentTimeMillis();
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

    

    // TODO: 2023/8/3  如果新加入节点之前存在在系统中，应该根据joinSuccessMsg拿到之前的消息
    // TODO: 2023/5/29 因为joinsuccessage和store  state是配套的，
    //  用哪个节点的store state就用那个 joinMessage
    //TODO  新加入节点也要对局部日志和节点配置表进行更新
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
                triggerStateChange();
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

    
    //TODO  新加入节点对系统中各个分发节点也做备份，
    // 新加入节点执行这个操作
    /**
     * 新节点加入成功后 执行的方法
     * */
    private void setupJoinInitialState(List<Host> members, int instanceNumber) {
        //传进来的参数setupInitialState(seeds, -1);
        //这里根据传进来的顺序， 已经将前链节点和后链节点分清楚出了
        membership = new Membership(members, QUORUM_SIZE);

        // TODO: 2023/8/3 应该在这里对配置信息进行更新比如accept  ack  等等序号 
        
        
        //nextOkCl = membership.nextLivingInChain(self);
        // 对排序的下一个节点准备，打算在这里
        //nextOkFront =membership.nextLivingInFrontedChain(self);
        //nextOkBack=membership.nextLivingInBackChain(self);
        //next
        members.stream().filter(h -> !h.equals(self)).forEach(this::openConnection);
        //对全局的排序消息进行配置  -1
        //joiningInstanceCl = highestAcceptedInstanceCl = highestAcknowledgedInstanceCl = highestDecidedInstanceCl =
        //        instanceNumber;// instanceNumber初始为-1
        //对命令分发进行初始化配置
        for (Host temp:members) {
            //对分发的配置进行初始化  hostConfigureMap
            //  Map<Host,RuntimeConfigure>
            RuntimeConfigure runtimeConfigure=new RuntimeConfigure();
            hostConfigureMap.put(temp.getAddress(),runtimeConfigure);

            //局部日志进行初始化 
            //Map<Host,Map<Integer, InstanceState>> instances 
            ConcurrentMap<Integer, InstanceState>  ins=new ConcurrentHashMap<>();
            instances.put(temp.getAddress(),ins);
        }
        //当判断当前节点是否为前链节点
        if(membership.frontChainContain(self)){
            frontChainNodeAction();
        } else {//成为后链节点
        }
    }

    

    // TODO: 2023/8/3  可以设置从原链尾节点发送状态:包括已执行状态和未执行消息的列表List集  
    // TODO: 2023/8/3  新加入节点应该接收的状态是到accept信息的状态：包括执行状态， 
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
                //execute();
                // 处理队头元素
                // ...
                // 在此处可以对队头元素进行处理，可以根据具体需求编写处理逻辑
                //bufferedOps.forEach(o -> triggerNotification(new ExecuteBatchNotification(o.getBatch())));
            }
            // TODO: 2023/8/3 在状态填充完毕，在开启执行线程的回收线程的开启 
            // TODO: 2023/5/17 bufferedOPs的清空  
            /*
            highestExecuteInstanceCl +=bufferedOps.size();
            bufferedOps.clear();
            */
        }
    }



    
    
    //-------------------------------加入节点的接收服务端处理方法

    
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

    // TODO: 2023/7/31 新加入节点接收暂存消息，生成数据结构
    
    /**
     * 设置一个时钟对超时的状态进行删除操作 ：这个时间和状态转换的时间有关系，比他大一点 
     * */
    private void onForgetStateTimer(ForgetStateTimer timer, long timerId) {
        // 如果超时，进行删除对应
        // TODO: 2023/5/29 设置节点状态删除条件，如果ack发送的是节点存储的的那个实例号，
        //  进行删除存储的状态
        //storedSnapshots.remove();
    }

    // TODO: 2023/7/25  新加入的节点如果是之前的节点还需要拿到之前的配置，根据传入


    
    
    
    
    
    
    
    

    /**----------涉及读 写  成员更改;接收上层中间件来的消息---------------------*/    

    
    // 在节点处于候选者时，waitingAppOps  waitingMembershipOps接收正常操作，不应该接收，因为当前的候选节点不是总能成为leader，那么暂存的这些消息将一直保留在这里，但直接丢弃也不行，直接丢弃会损失数据
    
    /**
     *处理frontend发来的批处理读请求
     * */
    public void onSubmitRead(SubmitReadRequest not, short from) {
        int readInstance = highestAcceptedInstanceCl + 1;
        //globalinstances.computeIfAbsent(readInstance, InstanceStateCL::new).attachRead(not);
        InstanceStateCL instance;
        if (!globalinstances.containsKey(readInstance)) {
            instance=new InstanceStateCL(readInstance);
            globalinstances.put(readInstance, instance);
            instance.attachRead(not);
        }else {
            globalinstances.get(readInstance).attachRead(not);
        }
    }

    
    // 两个地方使用,一个是和别的节点断开连接 一个是刚当选新Leader，对集群中不能连接的节点删除
    /**
     *接收处理成员改变Msg：在Leader断开某个节点启动
     */
    private void uponMembershipOpRequestMsg(MembershipOpRequestMsg msg, Host from, short sourceProto, int channel) {
        if (amQuorumLeader)//不改，成员管理还是由leader负责
            sendNextAcceptCL(msg.op);
        else if (supportedLeader().equals(self))//是候选者，其实很少用，因为调用处都是Leader调用
            waitingMembershipOps.add(msg.op);
        else
            logger.warn("Received " + msg + " without being leader, ignoring.");
    }


    // TODO: 2023/8/4 发送前，判断连接是否存在，可以避免，消息发送失败的，只会遗漏在断开连接传递过来消息之前的消息 

    /**
     * 消息Failed，发出logger.warn("Failed:)
     */
    private void uponMessageFailed(ProtoMessage msg, Host host, short i, Throwable throwable, int i1) {
        logger.warn("Failed: " + msg + ", to: " + host + ", reason: " + throwable.getMessage());
    }

    
    /**
     * 当向外的连接 建立时的操作：重新向nexotok发断点因为宕机漏发的信息这是系统的自动修正
     * */
    //TODO 将ack到accept的消息全部转发到下一个节点
    // 系统本来就会发重复消息: 在接收端对消息进行筛选处理，对重复消息丢弃
    //Notice  新加入节点会触发这个，
    // 新加入节点的前继将所有东西进行转发
    // todo 会 or不会 ？因为初始时，这是outconnetion？
    //   除非是先变更列表，将新加入节点纳入自己的nextok节点，之后再开启连接，两者顺序不可颠倒
    private void uponOutConnectionUp(OutConnectionUp event, int channel) {
        if (logger.isDebugEnabled()){
            logger.debug(event);
        }
        // 是新加入节点
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
            // TODO: 2023/7/19  将nextFront和nextokBack分别传输不同的消息
            //  nextOk
            for (int i = highestAcknowledgedInstanceCl + 1; i <= highestAcceptedInstanceCl; i++) {
                forwardCL(globalinstances.get(i));
            }
        }
    }


    /**
     * 当向外连接断掉时的操作，发出删除节点的消息
     * */
    private void uponOutConnectionDown(OutConnectionDown event, int channel) {
        logger.warn(event);
        //建立连接列表中去除指定元素
        establishedConnections.remove(event.getNode());
        // TODO: 2023/7/23  当Leader宕机立马进行Leader选举，先看看是否可行，如果恢复时间长，先设置Leader超时2s
        if (membership.contains(event.getNode())) {
            // 设置重连时钟
            setupTimer(new ReconnectTimer(event.getNode()), RECONNECT_TIME);
            if (amQuorumLeader)//若是leader,触发删除节点操作
                uponMembershipOpRequestMsg(new MembershipOpRequestMsg(MembershipOp.RemoveOp(event.getNode())),
                        self, getProtoId(), peerChannel);
            else if (supportedLeader()!=null && supportedLeader().equals(event.getNode())){
                // TODO: 2023/8/2 是前链节点拉取后链首节点 
                // TODO: 2023/8/2 所有节点执行将后链首节点代替Leader位置之后再进行选举
                lastLeaderOp = 0;// 强制进行leader选举
                // TODO: 2023/8/3 更好的设置一个一次性定时时钟类似LeaderTimout，还是直接触发Leader时钟超时的处理函数
                //  那么只有进入选举状态
                
                // TODO: 2023/7/31 Leader超时设置的是3秒，是不是还能改进
                //  在设置第二次重连，前链节点直接进入选举状态即直接进入Leader时钟超时的处理函数
            }
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
        if (logger.isDebugEnabled()){
            logger.debug(event);
        }
    }






    


    
    //  ------------Utils-------工具方法
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
    
    
    //  因为 maybeAddToPendingRemoval()方法对于前链节点实际改变了节点的排布情况
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
    
    
    //第一种情况：在对于新加入节点时，可能需要这些操作，新节点得到的成员列表和系统中的不一致的情况
    //第二种情况： 新旧Leader更换之后，重新执行以往的删除操作，
    //在接收一些删除节点操作的实例时，先进行可能移除
    //在删除操作时，先进行标记，后进行删除：若删除的是前链节点，需要将后链首节点与要删除的节点互换位置


    /**
     * 返回当前节点所支持的leader
     * */
    private Host supportedLeader() {
        return currentSN.getValue().getNode();
    }
    
    
    // 用到这里的应该是prepare  prepareOk  accept ack  electionok 几种消息,都是发送对应节点的Control层，所以使用sendMessage()的包含两个参数的用法
    /**
     * 发送消息给自己和其他主机
     */
    void sendOrEnqueue(ProtoMessage msg, Host destination) {
        if (msg == null || destination == null) {
            logger.error("null: " + msg + " " + destination);
        } else {
            if (destination.equals(self)) {
                deliverMessageIn(new MessageInEvent(new BabelMessage(msg, (short)-1, (short)-1), self, peerChannel));     
            }
            else{
                sendMessage(msg, destination);// 发送至Control层  
            }
        }
    }
}
