package TPOChain;

import TPOChain.ipc.SubmitReadRequest;
import TPOChain.utils.*;
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
import common.values.AppOpBatch;
import common.values.MembershipOp;
import common.values.NoOpValue;
import common.values.PaxosValue;
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




    /**
     * 在竞选leader时即候选者时暂存的命令
     * */
    private final Queue<AppOpBatch> waitingAppOps = new LinkedList<>();
    private final Queue<MembershipOp> waitingMembershipOps = new LinkedList<>();


    /**
     * 代表自身，左右相邻主机
     * */
    private final Host self;
    /**
     * 消息的下一个节点
     * */
    private Host nextOk;

    /**
     * 节点的状态：在加入整个系统中的状态变迁
     * */
    enum State {JOINING, WAITING_STATE_TRANSFER, ACTIVE}
    private TPOChainProto.State state;



    /**
     * 包含状态的成员列表，随时可以更新
     * */
    private Membership membership;

    /**
     * 已经建立连接的主机
     * */
    private final Set<Host> establishedConnections = new HashSet<>();



    /**
     * 在各个节点暂存的信息和全局存储的信息
     */
    private static final int INITIAL_MAP_SIZE = 4000;

    /**
     * 局部日志
     */
    private final Map<Integer, InstanceState> instances = new HashMap<>(INITIAL_MAP_SIZE);
//    private final Map<Integer,Map<Integer, InstanceState>> instances = new HashMap<>(INITIAL_MAP_SIZE);

    /**
     * 全局日志
     */
    private final Map<Integer, InstanceStateCL> globalinstances=new HashMap<>(INITIAL_MAP_SIZE);

    //各副本的的一些数据状态：
    private  Map<Host,HostConfigure>  hostConfigureMap=new HashMap<>();



    /**
     * 标记是否为前段节点，代表者可以发送command，并向leader发送排序
     * */
    private boolean  isFrontedChain;


    //标志着leader当前是谁，<Integer>是当选为leader的instance的实例号,SeqN是二元组，标志着term和Host
    private Map.Entry<Integer, SeqN> currentSN;
    //是否为leader,职能排序
    private boolean amQuorumLeader;
    //保持leader的心跳信息，用系统当前时间减去上次时间
    private long lastAcceptTime;



    /**
     * 各个command leader进行命令的分发
     * */
    private int highestAcknowledgedInstance = -1;
    private int highestAcceptedInstance = -1;
    private int highestDecidedInstance = -1;
    private int lastAcceptSent = -1;

    //在节点加入系统中的那个实例
    private int joiningInstance;


    /**
     *leader的排序字段
     * */
    private int highestAcknowledgedInstanceCL = -1;
    private int highestAcceptedInstanceCL = -1;
    private int highestDecidedInstanceCl = -1;
    private int lastAcceptSentCL = -1;


    //Timers
    private long joinTimer = -1;
    private long stateTransferTimer = -1;
    private long noOpTimer = -1;

    //多长时间没接收到leader消息
    private long leaderTimeoutTimer;

    //在每个节点标记上次leader命令的时间
    private long lastLeaderOp;



    /**
     * 暂存一些系统的节点状态，好让新节点装载
     * */
    //Dynamic membership
    //TODO eventually forget stored states... (irrelevant for experiments)
    //Waiting for application to generate snapshot (boolean represents if we should send as soon as ready)
    private final Map<Host, MutablePair<Integer, Boolean>> pendingSnapshots = new HashMap<>();
    //Application-generated snapshots
    private final Map<Host, Pair<Integer, byte[]>> storedSnapshots = new HashMap<>();
    //List of JoinSuccess (nodes who generated snapshots for me)
    private final Queue<Host> hostsWithSnapshot = new LinkedList<>();



    /**
     *暂存节点处于joining，暂存从前驱节点过来的批处理信息，等待状态变为active，再进行处理
     */
    private final Queue<AppOpBatch> bufferedOps = new LinkedList<>();
    private Map.Entry<Integer, byte[]> receivedState = null;





    //关于节点的tcp通道信息
    private int peerChannel;

    //java中net框架所需要的数据结构
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

        //viewNumber=0;//视图序号附初值

        this.workerGroup = workerGroup;
        // Map.Entry<Integer, SeqN> currentSN   是一个单键值对
        currentSN = new AbstractMap.SimpleEntry<>(-1, new SeqN(-1, null));
        amQuorumLeader = false;

        self = new Host(InetAddress.getByName(props.getProperty(ADDRESS_KEY)),
                Integer.parseInt(props.getProperty(PORT_KEY)));
        nextOk = null;//向左


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
        registerMessageSerializer(peerChannel, AcceptAckCLMsg.MSG_CODE, AcceptAckCLMsg.serializer);
        registerMessageSerializer(peerChannel, AcceptMsg.MSG_CODE, AcceptMsg.serializer);
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

        registerMessageHandler(peerChannel, UnaffiliatedMsg.MSG_CODE, this::uponUnaffiliatedMsg, this::uponMessageFailed);
        registerMessageHandler(peerChannel, AcceptAckMsg.MSG_CODE, this::uponAcceptAckMsg, this::uponMessageFailed);
        registerMessageHandler(peerChannel, AcceptAckCLMsg.MSG_CODE, this::uponAcceptAckCLMsg, this::uponMessageFailed);
        registerMessageHandler(peerChannel, AcceptMsg.MSG_CODE, this::uponAcceptMsg, this::uponMessageFailed);
        registerMessageHandler(peerChannel, AcceptCLMsg.MSG_CODE, this::uponAcceptCLMsg, this::uponMessageFailed);
        registerMessageHandler(peerChannel, DecidedMsg.MSG_CODE, this::uponDecidedMsg, this::uponMessageFailed);
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

        registerReplyHandler(DeliverSnapshotReply.REPLY_ID, this::onDeliverSnapshot);

        registerRequestHandler(SubmitBatchRequest.REQUEST_ID, this::onSubmitBatch);
        registerRequestHandler(SubmitReadRequest.REQUEST_ID, this::onSubmitRead);

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

    private void uponDecidedCLMsg(DecidedCLMsg protoMessage, Host from, short sourceProto, int channel) {
    }

    private void uponAcceptCLMsg(AcceptCLMsg protoMessage, Host from, short sourceProto, int channel) {
    }

    private void uponAcceptAckCLMsg(AcceptAckCLMsg protoMessage, Host from, short sourceProto, int channel) {
    }


    //接下来几个方法涉及leader选举
    /**
     * 初始状态开始启动
     * */
    private void setupInitialState(List<Host> members, int instanceNumber) {
        //传进来的参数setupInitialState(seeds, -1);
        membership = new Membership(members, QUORUM_SIZE);
        nextOk = membership.nextLivingInChain(self);
        //next
        members.stream().filter(h -> !h.equals(self)).forEach(this::openConnection);
        joiningInstance = highestAcceptedInstance = highestAcknowledgedInstance = highestDecidedInstance =
                instanceNumber;// instanceNumber初始为-1
        //设置领导超时处理
        leaderTimeoutTimer = setupPeriodicTimer(LeaderTimer.instance, LEADER_TIMEOUT, LEADER_TIMEOUT / 3);
        lastLeaderOp = System.currentTimeMillis();
    }

    /**
     * 在leadertimer超时争取当leader
     */
    private void onLeaderTimer(LeaderTimer timer, long timerId) {
        if (!amQuorumLeader && (System.currentTimeMillis() - lastLeaderOp > LEADER_TIMEOUT) &&
                (supportedLeader() == null
                        /**
                         * 不需要，避免加入节点在链的前半部分结束并减慢一切。只有在节点加入时领导者选举才会发生。
                         * */
                        //Not required, avoids joining nodes from ending in the first half of
                        // the chain and slowing everything down. Only would happen with leader
                        // election simultaneous with nodes joining.
                        // 下面这是注释的那段文字
                        // membership.distanceFrom(self, supportedLeader()) <= membership.size() - QUORUM_SIZE
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
        //为什么是ack信息
        InstanceState instance = instances.computeIfAbsent(highestAcknowledgedInstance + 1, InstanceState::new);
        //private Map.Entry<Integer, SeqN> currentSN是
        //new AbstractMap.SimpleEntry<>(-1, new SeqN(-1, null));
        SeqN newSeqN = new SeqN(currentSN.getValue().getCounter() + 1, self);
        instance.prepareResponses.put(newSeqN, new HashSet<>());
        PrepareMsg pMsg = new PrepareMsg(instance.iN, newSeqN);
        membership.getMembers().forEach(h -> sendOrEnqueue(pMsg, h));
    }

    /**
     * 处理PrepareMsg消息
     * */
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

        if (msg.iN > highestAcknowledgedInstance) {
            // currentSN消息是private Map.Entry<Integer, SeqN> currentSN;
            assert msg.iN >= currentSN.getKey();
            //在msg中大于此节点的选举领导信息
            if (!msg.sN.lesserOrEqualsThan(currentSN.getValue())) {
                //Accept - Change leader
                setNewInstanceLeader(msg.iN, msg.sN);//此函数已经选择 此msg为支持的领导

                //Gather list of accepts (if they exist)
                List<AcceptedValue> values = new ArrayList<>(Math.max(highestAcceptedInstance - msg.iN + 1, 0));
                for (int i = msg.iN; i <= highestAcceptedInstance; i++) {
                    InstanceState acceptedInstance = instances.get(i);
                    assert acceptedInstance.acceptedValue != null && acceptedInstance.highestAccept != null;
                    values.add(new AcceptedValue(i, acceptedInstance.highestAccept, acceptedInstance.acceptedValue));
                }
                sendOrEnqueue(new PrepareOkMsg(msg.iN, msg.sN, values), from);
                lastLeaderOp = System.currentTimeMillis();
            } else//否则丢弃
                logger.warn("Discarding prepare since sN <= hP");
        } else { //Respond with decided message  主要对于系统中信息滞后的节点进行更新
            /*
             若发送prepare消息的节点比接收prepare消息的节点信息落后，可以发送已经DecidedMsg信息
             */
            logger.info("Responding with decided");
            List<AcceptedValue> values = new ArrayList<>(highestDecidedInstance - msg.iN + 1);
            for (int i = msg.iN; i <= highestDecidedInstance; i++) {
                InstanceState decidedInstance = instances.get(i);
                assert decidedInstance.isDecided();
                values.add(new AcceptedValue(i, decidedInstance.highestAccept, decidedInstance.acceptedValue));
            }
            sendOrEnqueue(new DecidedMsg(msg.iN, msg.sN, values), from);
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
            cancelTimer(noOpTimer);

            waitingAppOps.clear();
            waitingMembershipOps.clear();
        }
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

    /**
     * 收到Decided信息
     */
    private void uponDecidedMsg(DecidedMsg msg, Host from, short sourceProto, int channel) {
        logger.debug(msg + " from:" + from);
        InstanceState instance = instances.get(msg.iN);
        //在实例被垃圾收集  或    msg的实例小于此节点的Decide   或    现在的leader大于msg的leader
        if (instance == null || msg.iN <= highestDecidedInstance || currentSN.getValue().greaterThan(msg.sN)) {
            logger.warn("Late decided... ignoring");
            return;
        }
        //若存在的话，
        instance.prepareResponses.remove(msg.sN);
        //Update decided values
        for (AcceptedValue decidedValue : msg.decidedValues) {
            InstanceState decidedInst = instances.computeIfAbsent(decidedValue.instance, InstanceState::new);
            logger.debug("Deciding:" + decidedValue + ", have: " + instance);
            decidedInst.forceAccept(decidedValue.sN, decidedValue.value);
            //Make sure there are no gaps between instances
            assert instance.iN <= highestAcceptedInstance + 1;
            if (instance.iN > highestAcceptedInstance) {
                highestAcceptedInstance++;
                assert instance.iN == highestAcceptedInstance;
            }
            if (!decidedInst.isDecided())
                decideAndExecute(decidedInst);
        }
        //No-one tried to be leader after me, trying again
        //若leader还是没有变化，还是重试，此处在更新节点的信息之后，结果可能不一样
        if (currentSN.getValue().equals(msg.sN))
            tryTakeLeadership();
    }

    /**
     *接收PrepareOkMsg消息
     * */
    private void uponPrepareOkMsg(PrepareOkMsg msg, Host from, short sourceProto, int channel) {
        InstanceState instance = instances.get(msg.iN);
        logger.debug(msg + " from:" + from);
        //在实例被垃圾收集  或   已经进行更高的ack  或   现在的leader比msg中的leader更大时
        if (instance == null || msg.iN <= highestAcknowledgedInstance || currentSN.getValue().greaterThan(msg.sN)) {
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
        for (AcceptedValue acceptedValue : msg.acceptedValues) {
            InstanceState acceptedInstance = instances.computeIfAbsent(acceptedValue.instance, InstanceState::new);
            //acceptedInstance的SeqN highestAccept属性;

            //  候选者的别的实例为空   或   新实例的term大于存在的实例的term
            if (acceptedInstance.highestAccept == null || acceptedValue.sN.greaterThan(
                    acceptedInstance.highestAccept)) {
                maybeCancelPendingRemoval(instance.acceptedValue);
                acceptedInstance.forceAccept(acceptedValue.sN, acceptedValue.value);
                maybeAddToPendingRemoval(instance.acceptedValue);

                //Make sure there are no gaps between instances
                assert acceptedInstance.iN <= highestAcceptedInstance + 1;
                if (acceptedInstance.iN > highestAcceptedInstance) {
                    highestAcceptedInstance++;
                    assert acceptedInstance.iN == highestAcceptedInstance;
                }
            }else {//不需要管
            }
        }


        //TODO maybe only need the second argument of max?
        if (okHosts.size() == Math.max(QUORUM_SIZE, membership.size() - QUORUM_SIZE + 1)) {
            //清空答复
            instance.prepareResponses.remove(msg.sN);
            assert currentSN.getValue().equals(msg.sN);
            assert supportedLeader().equals(self);
            //开始成为leader
            becomeLeader(msg.iN);
        }
    }

    /**
     * I am leader now! @ instance
     * */
    private void becomeLeader(int instanceNumber) {
        amQuorumLeader = true;
        noOpTimer = setupPeriodicTimer(NoOpTimer.instance, NOOP_SEND_INTERVAL, Math.max(NOOP_SEND_INTERVAL / 3,1));
        logger.info("I am leader now! @ instance " + instanceNumber);

        /**
         * 传递已经接收到accpted消息    Propagate received accepted ops
         * */
        for (int i = instanceNumber; i <= highestAcceptedInstance; i++) {
            if (logger.isDebugEnabled()) logger.debug("Propagating received operations: " + i);
            InstanceState aI = instances.get(i);
            assert aI.acceptedValue != null;
            assert aI.highestAccept != null;
            //goes to the end of the queue
            this.deliverMessageIn(new MessageInEvent(new BabelMessage(new AcceptMsg(i, currentSN.getValue(), (short) 0,
                    aI.acceptedValue, highestAcknowledgedInstance), (short)-1, (short)-1), self, peerChannel));
        }

        //标记leader发送到下一个节点到哪了
        lastAcceptSent = highestAcceptedInstance;

        /**
         * 对集群中不能连接的节点进行删除
         * */
        membership.getMembers().forEach(h -> {
            if (!h.equals(self) && !establishedConnections.contains(h)) {
                logger.info("Will propose remove " + h);
                uponMembershipOpRequestMsg(new MembershipOpRequestMsg(MembershipOp.RemoveOp(h)),
                        self, getProtoId(), peerChannel);
            }
        });

        lastAcceptTime = 0;




        /**
         * 对于暂存一些消息可以不做
         * */

        /**
         * 对咱暂存在成员操作和App操作进行发送
         * */
        PaxosValue nextOp;
        while ((nextOp = waitingMembershipOps.poll()) != null) {
            sendNextAccept(nextOp);
        }
        while ((nextOp = waitingAppOps.poll()) != null) {
            sendNextAccept(nextOp);
        }

    }


    //接下一个方法涉及消息的正常处理accpt
    /**
     *在当前节点是leader时处理，发送 或成员管理 或Noop 或App_Batch信息
     * */
    private void sendNextAccept(PaxosValue val) {
        assert supportedLeader().equals(self) && amQuorumLeader;

        InstanceState instance = instances.computeIfAbsent(lastAcceptSent + 1, InstanceState::new);
        assert instance.acceptedValue == null && instance.highestAccept == null;

        PaxosValue nextValue;
        if (val.type == PaxosValue.Type.MEMBERSHIP) {
            MembershipOp next = (MembershipOp) val;
            if ((next.opType == MembershipOp.OpType.REMOVE && !membership.contains(next.affectedHost)) ||
                    (next.opType == MembershipOp.OpType.ADD && membership.contains(next.affectedHost))) {
                logger.warn("Duplicate or invalid mOp: " + next);
                nextValue = new NoOpValue();
            } else {//若是正常的添加，删除
                if (next.opType == MembershipOp.OpType.ADD) //TODO remove this hack
                    next = MembershipOp.AddOp(next.affectedHost, membership.indexOf(self));
                nextValue = next;
            }

        } else if (val.type == PaxosValue.Type.APP_BATCH) {
            nextValue = val;
            //nBatches++;
            //nOpsBatched += ops.size();
        } else
            nextValue = new NoOpValue();

        this.uponAcceptMsg(new AcceptMsg(instance.iN, currentSN.getValue(),
                (short) 0, nextValue, highestAcknowledgedInstance), self, this.getProtoId(), peerChannel);
        //参数列表AcceptMsg(int iN, SeqN sN, short nodeCounter, PaxosValue value, int ack)
        // 参数列表uponAcceptMsg(AcceptMsg msg, Host from, short sourceProto, int channel)
        lastAcceptSent = instance.iN;
        lastAcceptTime = System.currentTimeMillis();
    }


    //发送排序消息到下一个节点
    private void sendNextAcceptCL(PaxosValue val) {
        assert supportedLeader().equals(self) && amQuorumLeader;

        InstanceState instance = instances.computeIfAbsent(lastAcceptSent + 1, InstanceState::new);
        assert instance.acceptedValue == null && instance.highestAccept == null;

        PaxosValue nextValue;
        if (val.type == PaxosValue.Type.MEMBERSHIP) {
            MembershipOp next = (MembershipOp) val;
            if ((next.opType == MembershipOp.OpType.REMOVE && !membership.contains(next.affectedHost)) ||
                    (next.opType == MembershipOp.OpType.ADD && membership.contains(next.affectedHost))) {
                logger.warn("Duplicate or invalid mOp: " + next);
                nextValue = new NoOpValue();
            } else {//若是正常的添加，删除
                if (next.opType == MembershipOp.OpType.ADD) //TODO remove this hack
                    next = MembershipOp.AddOp(next.affectedHost, membership.indexOf(self));
                nextValue = next;
            }

        } else if (val.type == PaxosValue.Type.APP_BATCH) {
            nextValue = val;
            //nBatches++;
            //nOpsBatched += ops.size();
        } else
            nextValue = new NoOpValue();

        this.uponAcceptMsg(new AcceptMsg(instance.iN, currentSN.getValue(),
                (short) 0, nextValue, highestAcknowledgedInstance), self, this.getProtoId(), peerChannel);
        //参数列表AcceptMsg(int iN, SeqN sN, short nodeCounter, PaxosValue value, int ack)
        // 参数列表uponAcceptMsg(AcceptMsg msg, Host from, short sourceProto, int channel)
        lastAcceptSent = instance.iN;
        lastAcceptTime = System.currentTimeMillis();
    }




    /**
     * 处理leader和其他节点呼吸事件
     * */
    private void onNoOpTimer(NoOpTimer timer, long timerId) {
        if (amQuorumLeader) {
            assert waitingAppOps.isEmpty() && waitingMembershipOps.isEmpty();
            if (System.currentTimeMillis() - lastAcceptTime > NOOP_SEND_INTERVAL)
                sendNextAccept(new NoOpValue());
        } else {
            logger.warn(timer + " while not quorumLeader");
            cancelTimer(noOpTimer);
        }
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

        InstanceState instance = instances.computeIfAbsent(msg.iN, InstanceState::new);

        /* 参数列表
        * this.uponAcceptMsg(new AcceptMsg(instance.iN, currentSN.getValue(),
                (short) 0, nextValue, highestAcknowledgedInstance), self, this.getProtoId(), peerChannel);
        *
        * */
        //"Discarding decided msg" 当instance
        if (instance.isDecided() && msg.sN.equals(instance.highestAccept)) {
            logger.warn("Discarding decided msg");
            return;
        }

        if (msg.sN.lesserThan(currentSN.getValue())) {
            //logger.warn("Discarding accept since sN < hP: " + msg);
            return;
        }//之后新消息msg.SN大于等于现在的SN

        //设置msg.sN.getNode()为新支持的leader
        if (msg.sN.greaterThan(currentSN.getValue()))
            setNewInstanceLeader(msg.iN, msg.sN);
        //
        if (msg.sN.equals(instance.highestAccept) && (msg.nodeCounter <= instance.counter)) {
            logger.warn("Discarding since same sN & leader, while counter <=");
            return;
        }

        lastLeaderOp = System.currentTimeMillis();//进行更新领导操作时间

        assert msg.sN.equals(currentSN.getValue());

        maybeCancelPendingRemoval(instance.acceptedValue);
        instance.accept(msg.sN, msg.value, (short) (msg.nodeCounter + 1));//进行对实例的确定

        //更新highestAcceptedInstance信息
        if (highestAcceptedInstance < instance.iN) {
            highestAcceptedInstance++;
            assert highestAcceptedInstance == instance.iN;
        }

        if ((instance.acceptedValue.type == PaxosValue.Type.MEMBERSHIP) &&
                (((MembershipOp) instance.acceptedValue).opType == MembershipOp.OpType.REMOVE))
            markForRemoval(instance);//删除节点

        forward(instance);//转发下一个节点

        if (!instance.isDecided() && instance.counter >= QUORUM_SIZE) //We have quorum!
            decideAndExecute(instance);//决定并执行实例

        ackInstance(msg.ack);//对于之前的实例进行ack并进行垃圾收集
    }

    /**
     * 处理acceptCl信息
     */
    private void uponAcceptMsgCL(AcceptMsg msg, Host from, short sourceProto, int channel) {
        //对不在系统中的节点发送未定义消息让其重新加入系统
        if(!membership.contains(from)){
            logger.warn("Received msg from unaffiliated host " + from);
            sendMessage(new UnaffiliatedMsg(), from, TCPChannel.CONNECTION_IN);
            return;
        }

        InstanceState instance = instances.computeIfAbsent(msg.iN, InstanceState::new);

        /* 参数列表
        * this.uponAcceptMsg(new AcceptMsg(instance.iN, currentSN.getValue(),
                (short) 0, nextValue, highestAcknowledgedInstance), self, this.getProtoId(), peerChannel);
        *
        * */
        //"Discarding decided msg" 当instance
        if (instance.isDecided() && msg.sN.equals(instance.highestAccept)) {
            logger.warn("Discarding decided msg");
            return;
        }

        if (msg.sN.lesserThan(currentSN.getValue())) {
            //logger.warn("Discarding accept since sN < hP: " + msg);
            return;
        }//之后新消息msg.SN大于等于现在的SN

        //设置msg.sN.getNode()为新支持的leader
        if (msg.sN.greaterThan(currentSN.getValue()))
            setNewInstanceLeader(msg.iN, msg.sN);
        //
        if (msg.sN.equals(instance.highestAccept) && (msg.nodeCounter <= instance.counter)) {
            logger.warn("Discarding since same sN & leader, while counter <=");
            return;
        }

        lastLeaderOp = System.currentTimeMillis();//进行更新领导操作时间

        assert msg.sN.equals(currentSN.getValue());

        maybeCancelPendingRemoval(instance.acceptedValue);
        instance.accept(msg.sN, msg.value, (short) (msg.nodeCounter + 1));//进行对实例的确定

        //更新highestAcceptedInstance信息
        if (highestAcceptedInstance < instance.iN) {
            highestAcceptedInstance++;
            assert highestAcceptedInstance == instance.iN;
        }

        if ((instance.acceptedValue.type == PaxosValue.Type.MEMBERSHIP) &&
                (((MembershipOp) instance.acceptedValue).opType == MembershipOp.OpType.REMOVE))
            markForRemoval(instance);//删除节点

        forward(instance);//转发下一个节点

        if (!instance.isDecided() && instance.counter >= QUORUM_SIZE) //We have quorum!
            decideAndExecute(instance);//决定并执行实例

        ackInstance(msg.ack);//对于之前的实例进行ack并进行垃圾收集
    }






    /**
     *标记要删除的节点
     * */
    private void markForRemoval(InstanceState inst) {
        MembershipOp op = (MembershipOp) inst.acceptedValue;
        membership.addToPendingRemoval(op.affectedHost);
        if (nextOk.equals(op.affectedHost)) {
            nextOk = membership.nextLivingInChain(self);
            //对ack序号信息进行到accpt序号信息进行重新转发
            for (int i = highestAcknowledgedInstance + 1; i < inst.iN; i++) {
                forward(instances.get(i));
            }
        }
    }

    /**
     * 转发accept信息给下一个节点
     * */
    private void forward(InstanceState inst) {
        //TODO some cases no longer happen (like leader being removed)
        if (!membership.contains(inst.highestAccept.getNode())) {
            logger.error("Received accept from removed node (?)");
            throw new AssertionError("Received accept from removed node (?)");
        }

        Iterator<Host> targets = membership.nextNodesUntil(self, nextOk);
        while (targets.hasNext()) {
            Host target = targets.next();
            if (target.equals(self)) {
                logger.error("Sending accept to myself: " + membership);
                throw new AssertionError("Sending accept to myself: " + membership);
            }

            if (membership.isAfterLeader(self, inst.highestAccept.getNode(), target)) { //am last
                assert target.equals(inst.highestAccept.getNode());
                //If last in chain than we must have decided (unless F+1 dead/inRemoval)
                if (inst.counter < QUORUM_SIZE) {
                    logger.error("Last living in chain cannot decide. Are f+1 nodes dead/inRemoval? "
                            + inst.counter);
                    throw new AssertionError("Last living in chain cannot decide. " +
                            "Are f+1 nodes dead/inRemoval? " + inst.counter);
                }
                sendMessage(new AcceptAckMsg(inst.iN), target);
            } else { //not last in chain...
                AcceptMsg msg = new AcceptMsg(inst.iN, inst.highestAccept, inst.counter, inst.acceptedValue,
                        highestAcknowledgedInstance);
                sendMessage(msg, target);
            }
        }
    }


    /**
     * decide并执行Execute实例
     * */
    private void decideAndExecute(InstanceState instance) {
        assert highestDecidedInstance == instance.iN - 1;
        assert !instance.isDecided();
        instance.markDecided();
        highestDecidedInstance++;

        //Actually execute message
        logger.debug("Decided: " + instance.iN + " - " + instance.acceptedValue);
        if (instance.acceptedValue.type == PaxosValue.Type.APP_BATCH) {
            if (state == TPOChainProto.State.ACTIVE)
                triggerNotification(new ExecuteBatchNotification(((AppOpBatch) instance.acceptedValue).getBatch()));
            else
                bufferedOps.add((AppOpBatch) instance.acceptedValue);
        } else if (instance.acceptedValue.type == PaxosValue.Type.MEMBERSHIP) {
            executeMembershipOp(instance);
        } else if (instance.acceptedValue.type != PaxosValue.Type.NO_OP) {
            logger.error("Trying to execute unknown paxos value: " + instance.acceptedValue);
            throw new AssertionError("Trying to execute unknown paxos value: " + instance.acceptedValue);
        }

    }


    /**
     * 对于ack包括以前的消息执行
     * */
    private void ackInstance(int instanceN) {
        //For nodes in the first half of the chain only
        for (int i = highestDecidedInstance + 1; i <= instanceN; i++) {
            InstanceState ins = instances.get(i);
            assert !ins.isDecided();
            decideAndExecute(ins);
            assert highestDecidedInstance == i;
        }

        //For everyone
        for (int i = highestAcknowledgedInstance + 1; i <= instanceN; i++) {
            InstanceState ins = instances.remove(i);
            ins.getAttachedReads().forEach((k, v) -> sendReply(new ExecuteReadReply(v, ins.iN), k));

            assert ins.isDecided();
            //更新ack信息
            highestAcknowledgedInstance++;
            assert highestAcknowledgedInstance == i;
        }
    }

    /**
     * 对于ack包括以前的消息执行
     * */
    private void ackInstanceCL(int instanceN) {
        //For nodes in the first half of the chain only
        for (int i = highestDecidedInstance + 1; i <= instanceN; i++) {
            InstanceState ins = instances.get(i);
            assert !ins.isDecided();
            decideAndExecute(ins);
            assert highestDecidedInstance == i;
        }

        //For everyone
        for (int i = highestAcknowledgedInstance + 1; i <= instanceN; i++) {
            InstanceState ins = instances.remove(i);
            ins.getAttachedReads().forEach((k, v) -> sendReply(new ExecuteReadReply(v, ins.iN), k));

            assert ins.isDecided();
            //更新ack信息
            highestAcknowledgedInstance++;
            assert highestAcknowledgedInstance == i;
        }
    }


    /**
     * leader接收ack信息，对实例进行ack
     * */
    private void uponAcceptAckMsg(AcceptAckMsg msg, Host from, short sourceProto, int channel) {
        //logger.debug(msg + " - " + from);
        if (msg.instanceNumber <= highestAcknowledgedInstance) {
            logger.warn("Ignoring acceptAck for old instance: " + msg);
            return;
        }

        //TODO never happens?
        InstanceState inst = instances.get(msg.instanceNumber);
        if (!amQuorumLeader || !inst.highestAccept.getNode().equals(self)) {
            logger.error("Received Ack without being leader...");
            throw new AssertionError("Received Ack without being leader...");
        }

        if (inst.acceptedValue.type != PaxosValue.Type.NO_OP)
            lastAcceptTime = 0; //Force sending a NO-OP (with the ack)

        ackInstance(msg.instanceNumber);
    }







    //请求加入集群的客户端方法

    /**
     * 当处理到UnaffiliatedMsg，将节点重新加入集群
     * */
    private void uponUnaffiliatedMsg(UnaffiliatedMsg msg, Host from, short sourceProto, int channel) {
        if (state == TPOChainProto.State.ACTIVE && membership.contains(from)){
            logger.error("Looks like I have unknowingly been removed from the membership, rejoining");
            instances.clear();
            cancelTimer(leaderTimeoutTimer);
            membership.getMembers().stream().filter(h -> !h.equals(self)).forEach(this::closeConnection);
            membership = null;
            state = TPOChainProto.State.JOINING;
            joinTimer = setupTimer(JoinTimer.instance, 1000);
        }
    }

    /**
     * 处理新节点的join
     * */
    private void onJoinTimer(JoinTimer timer, long timerId) {
        if (state == TPOChainProto.State.JOINING) {
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
     * 先收到 join成功
     * */
    private void uponJoinSuccessMsg(JoinSuccessMsg msg, Host from, short sourceProto, int channel) {
        if (state == TPOChainProto.State.JOINING) {
            hostsWithSnapshot.add(from);
            cancelTimer(joinTimer);
            logger.info("Join successful");
            setupInitialState(msg.membership, msg.iN);
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
     *重新请求快照
     * */
    private void onStateTransferTimer(StateTransferTimer timer, long timerId) {
        if (state == TPOChainProto.State.WAITING_STATE_TRANSFER) {
            Host poll = hostsWithSnapshot.poll();
            if (poll == null) {
                logger.error("StateTransferTimeout and have nobody to ask a snapshot for...");
                throw new AssertionError();
            } else {
                sendMessage(new StateRequestMsg(joiningInstance), poll);
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
            receivedState = new AbstractMap.SimpleEntry<>(msg.instanceNumber, msg.state);
        } else if (state == TPOChainProto.State.WAITING_STATE_TRANSFER) {
            assert msg.instanceNumber == joiningInstance;
            triggerNotification(new InstallSnapshotNotification(msg.state));
            state = TPOChainProto.State.ACTIVE;
            bufferedOps.forEach(o -> triggerNotification(new ExecuteBatchNotification(o.getBatch())));
        }
    }



    //加入节点的服务端处理方法

    /**
     * 所有节点都可能收到新节点的请求加入信息，收到之后将添加节点信息发给supportedLeader()
     * */
    private void uponJoinRequestMsg(JoinRequestMsg msg, Host from, short sourceProto, int channel) {
        if (state == TPOChainProto.State.ACTIVE)
            if (supportedLeader() != null)
                sendOrEnqueue(new MembershipOpRequestMsg(MembershipOp.AddOp(from)), supportedLeader());
            else
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


    /**
     *接收到Frontend发来的Snapshot，转发到target.
     * */
    public void onDeliverSnapshot(DeliverSnapshotReply not, short from) {
        MutablePair<Integer, Boolean> pending = pendingSnapshots.remove(not.getSnapshotTarget());
        assert not.getSnapshotInstance() == pending.getLeft();
        storedSnapshots.put(not.getSnapshotTarget(), Pair.of(not.getSnapshotInstance(), not.getState()));
        if (pending.getRight()) {//当此节点正好是F+1，正好是大多数节点qurom
            sendMessage(new StateTransferMsg(not.getSnapshotInstance(), not.getState()), not.getSnapshotTarget());
        }
    }



    /**
     * 执行成员Removed OR  addMember操作
     * */
    private void executeMembershipOp(InstanceState instance) {
        MembershipOp o = (MembershipOp) instance.acceptedValue;
        Host target = o.affectedHost;
        if (o.opType == MembershipOp.OpType.REMOVE) {
            logger.info("Removed from membership: " + target + " in inst " + instance.iN);
            membership.removeMember(target);
            triggerMembershipChangeNotification();
            closeConnection(target);
        } else if (o.opType == MembershipOp.OpType.ADD) {
            logger.info("Added to membership: " + target + " in inst " + instance.iN);
            membership.addMember(target, o.position);
            triggerMembershipChangeNotification();
            nextOk = membership.nextLivingInChain(self);
            openConnection(target);

            if (state == TPOChainProto.State.ACTIVE) {
                sendMessage(new JoinSuccessMsg(instance.iN, instance.highestAccept, membership.shallowCopy()), target);
                assert highestDecidedInstance == instance.iN;
                //TODO need mechanism for joining node to inform nodes they can forget stored state
                pendingSnapshots.put(target, MutablePair.of(instance.iN, instance.counter == QUORUM_SIZE));
                sendRequest(new GetSnapshotRequest(target, instance.iN), TPOChainFront.PROTOCOL_ID_BASE);
            }
        }
    }














    /**
     * 当前时leader或正在竞选leader的情况下处理frontend的提交batch
     */
    public void onSubmitBatch(SubmitBatchRequest not, short from) {
        if (amQuorumLeader)
            sendNextAccept(new AppOpBatch(not.getBatch()));
        else if (supportedLeader().equals(self))
            waitingAppOps.add(new AppOpBatch(not.getBatch()));
        else //忽视接受的消息
            logger.warn("Received " + not + " without being leader, ignoring.");
    }


    /**
     *处理frontend发来的批处理读请求
     * */
    public void onSubmitRead(SubmitReadRequest not, short from) {
        int readInstance = highestAcceptedInstance + 1;
        instances.computeIfAbsent(readInstance, InstanceState::new).attachRead(not);
    }




    /**
     * 消息Failed，发出logger.warn("Failed:)
     */
    private void uponMessageFailed(ProtoMessage msg, Host host, short i, Throwable throwable, int i1) {
        logger.warn("Failed: " + msg + ", to: " + host + ", reason: " + throwable.getMessage());
    }

    /**
     * 当向外的连接 建立时的操作  重新向nexotok发断点因为宕机漏发的信息
     * */
    private void uponOutConnectionUp(OutConnectionUp event, int channel) {
        logger.debug(event);
        if(state == TPOChainProto.State.JOINING)
            return;
        if (membership.contains(event.getNode())) {
            establishedConnections.add(event.getNode());
            if (event.getNode().equals(nextOk))//进行重发断点的消息
                for (int i = highestAcceptedInstance; i <= highestAcknowledgedInstance && i >= 0; i++)
                    forward(instances.get(i));
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
            else if (supportedLeader().equals(event.getNode()))//若leader断掉，触发计时
                lastLeaderOp = 0;
        }
    }

    /**
     *发送成员改变的消息
     */
    private void uponMembershipOpRequestMsg(MembershipOpRequestMsg msg, Host from, short sourceProto, int channel) {
        if (amQuorumLeader)
            sendNextAccept(msg.op);
        else if (supportedLeader().equals(self))
            waitingMembershipOps.add(msg.op);
        else
            logger.warn("Received " + msg + " without being leader, ignoring.");
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
