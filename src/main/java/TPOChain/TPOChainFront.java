package TPOChain;

import TPOChain.messages.JoinRequestMsg;
import TPOChain.notifications.FrontIndexNotification;
import frontend.FrontendProto;
import app.Application;
import TPOChain.ipc.ExecuteReadReply;
import TPOChain.timers.ReconnectTimer;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.babel.internal.BabelMessage;
import pt.unl.fct.di.novasys.babel.internal.MessageInEvent;
import pt.unl.fct.di.novasys.channel.tcp.events.OutConnectionDown;
import pt.unl.fct.di.novasys.channel.tcp.events.OutConnectionFailed;
import pt.unl.fct.di.novasys.channel.tcp.events.OutConnectionUp;
import frontend.ipc.SubmitBatchRequest;
import TPOChain.ipc.SubmitReadRequest;
import frontend.network.*;
import frontend.notifications.*;
import frontend.ops.OpBatch;
import frontend.timers.BatchTimer;
import pt.unl.fct.di.novasys.network.data.Host;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;
public class TPOChainFront extends FrontendProto {

    public final static short PROTOCOL_ID_BASE = 100;
    public final static String PROTOCOL_NAME_BASE = "TPOChainFronted";


    public static final String BATCH_SIZE_KEY = "batch_size"; 
    public static final String BATCH_INTERVAL_KEY = "batch_interval";
    
    public static final String LOCAL_BATCH_SIZE_KEY = "local_batch_size";
    public static final String LOCAL_BATCH_INTERVAL_KEY = "local_batch_interval";
 
    
    private static final Logger logger = LogManager.getLogger(TPOChainFront.class);
    private final int BATCH_INTERVAL;
    private final int LOCAL_BATCH_INTERVAL;
    private final int BATCH_SIZE;
    private final int LOCAL_BATCH_SIZE;
    
    
    
    //Forwarded 对于已经发送下层协议的批处理，备份保存，之后调用处，使用这个队列出队执行
    private final Queue<Pair<Long, OpBatch>> pendingWrites;
    // 读缓存，是不发往Control层和其他节点的，只发一个标识
    private final Queue<Pair<Long, List<byte[]>>> pendingReads;

    

    // 一个向WrieTo发送失败队列：元素应该是发送的peerBatchMessage
    private Queue<PeerBatchMessage>  failWrites= new LinkedList<>();
    // 一个缓存，暂存在writTo不可连接的暂存队列
    private Queue<PeerBatchMessage>  waitWrites= new LinkedList<>();

    // 一个缓存，在上一个通道的消息批没有处理完毕的暂存队列
    private Queue<PeerBatchMessage>  waitBatchWrites= new LinkedList<>();
    
    
    
    // 读和写的锁
    private final Object readLock = new Object();
    private final Object writeLock = new Object();
    
    
    //消息的发往地，是一个Host，后链的挂载节点，前链是自身
    private Host writesTo;
    private boolean writesToConnected;
    
    
    //上次读写操作的计时
    private long lastWriteBatchTime;
    private long lastReadBatchTime;
    
    
    
    //缓存中心：ToForward writes将用户请求打成批处理
    private List<byte[]> writeDataBuffer;
    //ToSubmit reads
    private List<byte[]> readDataBuffer;

    
    // Front使用哪个编号的Data通道
    private  int index;
    

    // 上次发送的Batch id  
    public long  lastSentBatchid;
    // 上次执行的batch id
    public long  lastExecuteBatchid;
    // TODO: 2023/8/17 初始状态是true,在发生挂载WriteTo变迁之后，变为false，在执行上一个通道的批之后，再变为false， 
    public boolean canSendToWriteTo;
    
    
    
    public TPOChainFront(Properties props, short protoIndex, Application app) throws IOException {
        super(PROTOCOL_NAME_BASE + protoIndex, (short) (PROTOCOL_ID_BASE + protoIndex),
                props, protoIndex, app);

        this.BATCH_INTERVAL = Integer.parseInt(props.getProperty(BATCH_INTERVAL_KEY));
        this.LOCAL_BATCH_INTERVAL = Integer.parseInt(props.getProperty(LOCAL_BATCH_INTERVAL_KEY));
        this.BATCH_SIZE = Integer.parseInt(props.getProperty(BATCH_SIZE_KEY));
        this.LOCAL_BATCH_SIZE = Integer.parseInt(props.getProperty(LOCAL_BATCH_SIZE_KEY));
        
        //象征着leader,这里是挂载的前链节点
        writesTo = null;
        writesToConnected = false;


        // 初始状态为-1
        lastSentBatchid=-1;
        lastExecuteBatchid=-1;

        canSendToWriteTo=true;
        
        //缓存用户端来的请求达成批处理
        writeDataBuffer = new ArrayList<>(BATCH_SIZE);
        readDataBuffer = new ArrayList<>(LOCAL_BATCH_SIZE);

        //记录发到下层协议层的批处理，准备回复客户端
        pendingWrites = new ConcurrentLinkedQueue<>();
        pendingReads = new ConcurrentLinkedQueue<>();
    }
    
    ///**
    // * 构造函数
    // *
    // * @param protocolName
    // * @param protocolId
    // * @param props
    // * @param protoIndex
    // * @param app
    // * @param batchInterval
    // * @param localBatchInterval
    // * @param batchSize
    // * @param localBatchSize
    // * @param pendingWrites
    // * @param pendingReads
    // */
    //public TPOChainFront(String protocolName, short protocolId, Properties props, short protoIndex, Application app, int batchInterval, int localBatchInterval, int batchSize, int localBatchSize, Queue<Pair<Long, OpBatch>> pendingWrites, Queue<Pair<Long, List<byte[]>>> pendingReads) throws IOException {
    //    super(protocolName, protocolId, props, protoIndex, app);
    //    BATCH_INTERVAL = batchInterval;
    //    LOCAL_BATCH_INTERVAL = localBatchInterval;
    //    BATCH_SIZE = batchSize;
    //    LOCAL_BATCH_SIZE = localBatchSize;
    //    this.pendingWrites = pendingWrites;
    //    this.pendingReads = pendingReads;
    //}

    @Override
    protected void _init(Properties props) throws HandlerRegistrationException {
        registerTimerHandler(BatchTimer.TIMER_ID, this::handleBatchTimer);
        
        final int minTimer = Math.min(LOCAL_BATCH_INTERVAL, BATCH_INTERVAL);
        setupPeriodicTimer(new BatchTimer(), minTimer, minTimer);
        
        
        registerTimerHandler(ReconnectTimer.TIMER_ID, this::onReconnectTimer);
        
        
        //接收来自 proto的请求
        registerReplyHandler(ExecuteReadReply.REPLY_ID, this::onExecuteRead);

        // 接收Front层使用Data通道的id 改变
        subscribeNotification(FrontIndexNotification.NOTIFICATION_ID, this::onFrontIndexNotificationChange);

        
        lastWriteBatchTime = System.currentTimeMillis();
        lastReadBatchTime = System.currentTimeMillis();
    }

    
    
    
    
    
    //---------CLIENT OPS-----关于从HashMap App是直接调用同步关系，不是异步完成;
    
    /**
     * 被HashMapApp直接调用；接收从客户端接收来的信息，先缓存成批，最后成批处理
     * */
    @Override
    public void submitOperation(byte[] op, OpType type) {
        switch (type) {
            case STRONG_READ:
                synchronized (readLock) {
                    readDataBuffer.add(op);
                    if (readDataBuffer.size() == LOCAL_BATCH_SIZE)
                        sendNewReadBatch();
                }
                break;
            case WRITE:
                synchronized (writeLock) {
                    writeDataBuffer.add(op);
                    if (writeDataBuffer.size() == BATCH_SIZE)
                        sendNewWriteBatch();
                }
                break;
        }
    }

    
    
    /**-----------------------写-------------------------*/
    
    //将客户端来的写信息处理：若满足指定size的batch，则进行此项处理
    private void sendNewWriteBatch() {
        long internalId = nextId();//nextId()生成了关于本地ip的有序列数
        OpBatch batch = new OpBatch(internalId, self, getProtoId(), writeDataBuffer);
        
        pendingWrites.add(Pair.of(internalId, batch));
        writeDataBuffer = new ArrayList<>(BATCH_SIZE);//重新生成一个新缓存空间
        sendBatchToWritesTo(new PeerBatchMessage(batch));
        // 设置上次的发送时间
        lastWriteBatchTime = System.currentTimeMillis();
    }
    
    
    /**
     * 将写信息转发至writeTo即leader处理，直接调用到这里截止，后面都是异步完成
     * */
    private void sendBatchToWritesTo(PeerBatchMessage msg) {
        if (canSendToWriteTo){//因为更换通道需要上次通道的消息批执行完毕，才可以发送下一个节点
            //logger.info("发送"+msg+"到"+writesTo);
            if (writesTo.getAddress().equals(self)) {
                // 原来直接使用，现在解耦，以消息传输，异步完成
                //onPeerBatchMessage(msg, writesTo, getProtoId(), peerChannel);
                deliverMessageIn(new MessageInEvent(new BabelMessage(msg,getProtoId(),getProtoId()) ,new Host(self,PEER_PORT),peerChannel));
                // 记录上次发送的batchid
                lastSentBatchid=msg.getBatch().getBatchId();
                return ;
            }
            if (writesToConnected){//如果连接到挂载点，进行发送
                sendMessage(peerChannel, msg, writesTo);
            }else { //挂载点还没有建立连接，暂存消息
                waitWrites.add(msg);
            }
        }else{//上一个通道的消息还没有处理完毕,下面是新一轮通道的
            waitBatchWrites.add(msg);
        }
    }
    
    
    
    // 这个只能是前链节点调用,接收自身或挂载在自身节点的后链节点的Msg
    /**
     * 接收PeerBatchMsg的处理函数：连接下层的通道 将消息转发给下层的协议层
     * 只传递写的batch信息到protocol层 发送 TPOChainProto的submitBatchRequest
     * **/
    protected void onPeerBatchMessage(PeerBatchMessage msg, Host from, short sProto, int channel) {
        //sendRequest(new SubmitBatchRequest(msg.getBatch()), TPOChainProto.PROTOCOL_ID);
        // 这里改为了向data层发送请求
        sendRequest(new SubmitBatchRequest(msg.getBatch()),(short)(TPOChainData.PROTOCOL_ID+index));
        // 能到这里肯定是前链节点，不需要
    }
    
    
    /**
     * 在成功发送完peerBatchMessage之后
     * */
    protected void uponPeerBatchMessageOut(PeerBatchMessage msg, Host from, short sProto, int channel) {
        lastSentBatchid=msg.getBatch().getBatchId();
    }
    
    
    //---------------读-----------------
    
    /**
     * 将客户端来的读信息处理：若满足指定size的batch，则进行此项处理
     */
    private void sendNewReadBatch() {
        long internalId = nextId();
        //
        pendingReads.add(Pair.of(internalId, readDataBuffer));
        //清空读缓存
        readDataBuffer = new ArrayList<>(LOCAL_BATCH_SIZE);
        sendRequest(new SubmitReadRequest(internalId, getProtoId()),TPOChainProto.PROTOCOL_ID);
        lastReadBatchTime = System.currentTimeMillis();
    }
    
    
    
    
    
    //-------------------TIMERS-----------------------
    
    /**
     * 在batch时间间隔内开启处理sendNewReadBatch()或sendNewWriteBatch()
     * */
    private void handleBatchTimer(BatchTimer timer, long l) {
        long currentTime = System.currentTimeMillis();
        //Send read buffer
        if ((lastReadBatchTime + LOCAL_BATCH_INTERVAL) < currentTime)
            synchronized (readLock) {
                if (!readDataBuffer.isEmpty()) {
                    logger.warn("Sending read batch by timeout, size " + readDataBuffer.size());
                    sendNewReadBatch();
                }
            }
        //Check if write buffer timed out
        if ((lastWriteBatchTime + BATCH_INTERVAL) < currentTime)
            synchronized (writeLock) {
                if (!writeDataBuffer.isEmpty()) {
                    logger.warn("Sending write batch by timeout, size " + writeDataBuffer.size());
                    sendNewWriteBatch();
                }
            }
    }





    
    

    
    /**
     * 在TCP连接writesTO节点时，将pendingWrites逐条发送到下一个节点
     * */
    protected void onOutConnectionUp(OutConnectionUp event, int channel) {
        Host peer = event.getNode();
        if (peer.equals(writesTo)) {
            writesToConnected = true;
            logger.debug("Connected to writesTo " + event);
            // 当通道没有转换时处理这个
            if (canSendToWriteTo){
                while (!failWrites.isEmpty()) {
                    // 从队列头部取出元素并进行处理
                    PeerBatchMessage element = failWrites.poll();
                    sendBatchToWritesTo(element);
                }
                //转发暂存的队列
                while (!waitWrites.isEmpty()) {
                    // 从队列头部取出元素并进行处理
                    PeerBatchMessage element = waitWrites.poll();
                    sendBatchToWritesTo(element);
                }
            }
            // 注释掉下面这个 //不会重复吗 ？
            //pendingWrites.forEach(b -> sendBatchToWritesTo(new PeerBatchMessage(b.getRight())));
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
            logger.warn("Lost connection to writesTo, re-connecting in 1: " + event);
            writesToConnected = false;
            setupTimer(new ReconnectTimer(writesTo), 1000);
        }
    }

    protected void onOutConnectionFailed(OutConnectionFailed<Void> event, int channel) {
        logger.info(event);
        Host peer = event.getNode();
        if (peer.equals(writesTo)) {
            writesToConnected = false;
            logger.warn("Connection failed to writesTo, re-trying in 1: " + event);
            setupTimer(new ReconnectTimer(writesTo), 1000);
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

    
    /**
     * 在消息发送至writeTo失败的消息暂存
     * */
    public void uponMessageFailed(ProtoMessage msg, Host host, short i, Throwable throwable, int i1){
        logger.warn("Failed: " + msg + ", to: " + host + ", reason: " + throwable.getMessage());
        failWrites.add((PeerBatchMessage)msg);
    }






    // -------------------- CONSENSUS OPS ----------------------------------------------- 

    /**
     * Fronted具体负责执行write的batch
     * */
    protected void onExecuteBatch(ExecuteBatchNotification not, short from) {
        if ((not.getBatch().getIssuer().equals(self)) && (not.getBatch().getFrontendId() == getProtoId())) {
            Pair<Long, OpBatch> ops = pendingWrites.poll();
            // TODO: 2023/8/13 这里可以去掉，不保持顺序 
            if (ops == null || ops.getLeft() != not.getBatch().getBatchId()) {
                logger.error("Expected " + not.getBatch().getBatchId() + ". Got " + ops + "\n" +
                        pendingWrites.stream().map(Pair::getKey).collect(Collectors.toList()));
                throw new AssertionError("Expected " + not.getBatch().getBatchId() + ". Got " + ops);
            }
            not.getBatch().getOps().forEach(op -> app.executeOperation(op, true, not.getInstId()));
            lastExecuteBatchid=not.getBatch().getBatchId();
            if (!canSendToWriteTo  && lastExecuteBatchid == lastSentBatchid){
                canSendToWriteTo=true;
                while (!failWrites.isEmpty()) {
                    // 从队列头部取出元素并进行处理
                    PeerBatchMessage element = failWrites.poll();
                    sendBatchToWritesTo(element);
                }
                //转发暂存的队列
                while (!waitWrites.isEmpty()) {
                    // 从队列头部取出元素并进行处理
                    PeerBatchMessage element = waitWrites.poll();
                    sendBatchToWritesTo(element);
                }
                while (!waitBatchWrites.isEmpty()){
                    PeerBatchMessage msg = waitBatchWrites.poll();
                    sendBatchToWritesTo(msg);
                }
            }
        } else {
            not.getBatch().getOps().forEach(op -> app.executeOperation(op, false, not.getInstId()));
        }
    }

    /**
     *对Proto发过来的Read请求处理
     * */
    protected void onExecuteRead(ExecuteReadReply not, short from) {
        /*
         * ExecuteReadReply是这个new ExecuteReadReply(v, ins.iN)
         * 读是能挂载多个读请求的，
         * 读是不传播在其他节点的
         */
        not.getBatchIds().forEach(bId -> {
            Pair<Long, List<byte[]>> ops = pendingReads.poll();
            if (ops == null || !ops.getKey().equals(bId)) {
                logger.error("Expected " + bId + ". Got " + ops + "\n" +
                        pendingReads.stream().map(Pair::getKey).collect(Collectors.toList()));
                throw new AssertionError("Expected " + bId + ". Got " + ops);
            }
            ops.getRight().forEach(op -> app.executeOperation(op, true, not.getInstId()));
        });
    }

    
    
    
    
    
    //主要是改变成员列表参数，改变节点所指向的WritesTo参数  ; Front只连接到 writeto即 leader节点，对于其他节点不连接
    /**
     * logger.info("New writesTo: " + writesTo.getAddress());
     * */
    protected void onMembershipChange(MembershipChange notification, short emitterId) {
        //  这里改变数据通道的指向 
        this.index=notification.getIndex();//改变分发消息的通道指向 
     
        //update membership and responder
        membership = notification.getOrderedMembers();
        
        // 初始化使用这个
        if (writesTo == null){
            //Update and open to new writesTo
            writesTo = new Host(notification.getWritesTo(), PEER_PORT);
            logger.info("New mount: " + writesTo.getAddress());
            // 更换挂载点
            if (!writesTo.getAddress().equals(self)){//如果不等于自身节点
                openConnection(writesTo, peerChannel);
            }
            canSendToWriteTo=true;
            return;
        }
        
        //Writes to changed
        if (!notification.getWritesTo().equals(writesTo.getAddress())) {
            //这个意思是换通道了，不能进行响应了
            canSendToWriteTo=false;
            
            //Close old writesTo
            if (!writesTo.getAddress().equals(self)) {
                writesToConnected = false;
                closeConnection(writesTo, peerChannel);
            }
            
            //Update and open to new writesTo
            writesTo = new Host(notification.getWritesTo(), PEER_PORT);
            logger.info("New mount: " + writesTo.getAddress());
            // 更换挂载点
            if (!writesTo.getAddress().equals(self)){//如果不等于自身节点
                openConnection(writesTo, peerChannel);
            }
        }
    }
    
    // 已经废弃,在Control层不会有调用，那这个也不会响应
    /**
     * 发送请求改变选择哪个Data通道
     * */
    protected void onFrontIndexNotificationChange(FrontIndexNotification notification,short emitterID){
        this.index=notification.getIndex();
    }
}