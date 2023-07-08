package frontend;

import app.Application;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel;
import pt.unl.fct.di.novasys.channel.tcp.events.*;
import frontend.ipc.DeliverSnapshotReply;
import frontend.ipc.GetSnapshotRequest;
import frontend.network.*;
import frontend.notifications.*;
import pt.unl.fct.di.novasys.network.data.Host;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Properties;

public abstract class FrontendProto extends GenericProtocol {

    public static final String ADDRESS_KEY = "frontend_address";
    public static final String PEER_PORT_KEY = "frontend_peer_port";
    
    private static final Logger logger = LogManager.getLogger(FrontendProto.class);

    // 操作的两种枚举
    public enum OpType {STRONG_READ, WRITE}
    
    // 使用的端口号
    protected final int PEER_PORT;
    // 标识自身
    protected final InetAddress self;
    // 对状态层的标识
    protected final Application app;
    
    
    
    //下面两个参数拼接生成对操作的唯一标识
    private final int opPrefix;
    private int opCounter;
    
    
    
    //第几个Front索引
    private final short protoIndex;
    // TCP通道
    protected int peerChannel;
    
    
    //系统中节点列表动态的更新，从下层的protocol层接收，是protocol层的membership的备份
    protected List<InetAddress> membership;

    
    //构造函数
    public FrontendProto(String protocolName, short protocolId, Properties props,
                         short protoIndex, Application app) throws IOException {
        super(protocolName, protocolId);

        self = InetAddress.getByName(props.getProperty(ADDRESS_KEY));
        this.PEER_PORT = Integer.parseInt(props.getProperty(PEER_PORT_KEY)) + protoIndex;
        
        
        opPrefix = ByteBuffer.wrap(self.getAddress()).getInt();
        opCounter = 0;
        
        
        this.app = app;
        membership = null;
        this.protoIndex = protoIndex;
    }

    
    @SuppressWarnings("DuplicatedCode")
    @Override
    public void init(Properties props) throws HandlerRegistrationException, IOException {
        //Peer
        Properties peerProps = new Properties();
        peerProps.setProperty(TCPChannel.ADDRESS_KEY, props.getProperty(ADDRESS_KEY));
        peerProps.setProperty(TCPChannel.PORT_KEY, Integer.toString(PEER_PORT));
        //peerProps.put(TCPChannel.DEBUG_INTERVAL_KEY, 10000);
        peerChannel = createChannel(TCPChannel.NAME, peerProps);

        
        registerMessageSerializer(peerChannel, PeerBatchMessage.MSG_CODE, PeerBatchMessage.serializer);

        registerMessageHandler(peerChannel, PeerBatchMessage.MSG_CODE, this::onPeerBatchMessage, this::uponMessageFailed);

        

        registerChannelEventHandler(peerChannel, InConnectionDown.EVENT_ID, this::onInConnectionDown);
        registerChannelEventHandler(peerChannel, InConnectionUp.EVENT_ID, this::onInConnectionUp);
        registerChannelEventHandler(peerChannel, OutConnectionDown.EVENT_ID, this::onOutConnectionDown);
        registerChannelEventHandler(peerChannel, OutConnectionUp.EVENT_ID, this::onOutConnectionUp);
        registerChannelEventHandler(peerChannel, OutConnectionFailed.EVENT_ID, this::onOutConnectionFailed);

        //Consensus
        subscribeNotification(MembershipChange.NOTIFICATION_ID, this::onMembershipChange);
        subscribeNotification(ExecuteBatchNotification.NOTIFICATION_ID, this::onExecuteBatch);
        subscribeNotification(InstallSnapshotNotification.NOTIFICATION_ID, this::onInstallSnapshot);
        
        //接收来自proto的请求state的请求
        registerRequestHandler(GetSnapshotRequest.REQUEST_ID, this::onGetStateSnapshot);
       
       
        _init(props);
    }

    
    protected abstract void _init(Properties props) throws HandlerRegistrationException;

    
    /**
     * 生成独一无二的有关ip和本地数字的MessageID
     * 返回结果是两串字拼接的成果： (opcount，IP地址)
     * */
    protected long nextId() {
        //Message id is constructed using the server ip and a local counter (makes it unique and sequential)
        opCounter++;
        return ((long) opCounter << 32) | (opPrefix & 0xFFFFFFFFL);
    }

    
   
    /* ------------------------ APP INTERFACE ----------------------- */
   
    //TODO SYNCHRONIZE THIS FOR EVERY FRONTEND
    public abstract void submitOperation(byte[] op, OpType type);
    
    
    
    /* ----------------------- PEER EVENTS -------------- */

    /**
     * 处理PeerBatch消息事件
     * */
    protected abstract void onPeerBatchMessage(PeerBatchMessage msg, Host from, short sProto, int channel);

    
    
    
    protected abstract void onOutConnectionUp(OutConnectionUp event, int channel);

    protected abstract void onOutConnectionDown(OutConnectionDown event, int channel);

    protected abstract void onOutConnectionFailed(OutConnectionFailed<Void> event, int channel);

    private void onInConnectionDown(InConnectionDown event, int channel) {
        logger.debug(event);
    }

    private void onInConnectionUp(InConnectionUp event, int channel) {
        logger.debug(event);
    }

    
    public void uponMessageFailed(ProtoMessage msg, Host host, short i, Throwable throwable, int i1) {
        logger.warn("Failed: " + msg + ", to: " + host + ", reason: " + throwable.getMessage());
    }


  
    
    
    /* ------------------------------------------- CONSENSUS OPS ------------------------------------------- */
    
    /**
     * 由proto请求，fronted接收，然后答复发送到相应的协议Proto得到的快照
     * */
    public void onGetStateSnapshot(GetSnapshotRequest not, short from) {
        byte[] state = app.getSnapshot();
        sendReply(new DeliverSnapshotReply(not.getSnapshotTarget(),
                not.getSnapshotInstance(), state), from);
    }
    
    /**
     * 安装快照
     * */
    private void onInstallSnapshot(InstallSnapshotNotification not, short from) {
        app.installState(not.getState());
    }
    
    
    
    protected abstract void onExecuteBatch(ExecuteBatchNotification reply, short from);

    
    
    
    protected abstract void onMembershipChange(MembershipChange notification, short emitterId);
}
