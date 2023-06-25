package app;

import TPOChain.TPOChainData;
import TPOChain.TPOChainFront;
import TPOChain.TPOChainProto;
import app.networking.RequestDecoder;
import app.networking.RequestMessage;
import app.networking.ResponseEncoder;
import app.networking.ResponseMessage;
import chainpaxos.ChainPaxosDelayedFront;
import chainpaxos.ChainPaxosDelayedProto;
import chainpaxos.ChainPaxosMixedFront;
import chainpaxos.ChainPaxosMixedProto;
import chainreplication.ChainRepMixedFront;
import chainreplication.ChainRepMixedProto;
import distinguishedpaxos.DistPaxosFront;
import distinguishedpaxos.DistPaxosPiggyProto;
import distinguishedpaxos.DistPaxosProto;
import distinguishedpaxos.MultiPaxosProto;
import epaxos.EPaxosFront;
import epaxos.EPaxosProto;
import epaxos.EsolatedPaxosProto;
import frontend.FrontendProto;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.EventExecutorGroup;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pt.unl.fct.di.novasys.babel.core.Babel;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.exceptions.InvalidParameterException;
import pt.unl.fct.di.novasys.babel.exceptions.ProtocolAlreadyExistsException;
import pt.unl.fct.di.novasys.network.NetworkManager;
import ringpaxos.RingPaxosFront;
import ringpaxos.RingPaxosPiggyProto;
import ringpaxos.RingPaxosProto;
import uringpaxos.URingPaxosFront;
import uringpaxos.URingPaxosProto;

import java.io.*;
import java.lang.reflect.Field;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

public class HashMapApp implements Application {

    private static final Logger logger = LogManager.getLogger(HashMapApp.class);
    private final ConcurrentMap<Integer, Pair<Integer, Channel>> opMapper;
    private final AtomicInteger idCounter;
    private final List<FrontendProto> frontendProtos;
    private int nWrites,nReads;
    private ConcurrentMap<String, byte[]> store;

    public HashMapApp(Properties configProps) throws IOException, ProtocolAlreadyExistsException,
            HandlerRegistrationException, InterruptedException {

        store = new ConcurrentHashMap<>();
        idCounter = new AtomicInteger(0);
        opMapper = new ConcurrentHashMap<>();
        int port = Integer.parseInt(configProps.getProperty("app_port"));
        Babel babel = Babel.getInstance();
        EventLoopGroup consensusWorkerGroup     = NetworkManager.createNewWorkerGroup();
        EventLoopGroup consensusdataWorkerGroup = NetworkManager.createNewWorkerGroup();
        EventLoopGroup consensusdataWorkerGroup2 = NetworkManager.createNewWorkerGroup();
        String alg = configProps.getProperty("algorithm");
        int nFrontends = Short.parseShort(configProps.getProperty("n_frontends"));
        frontendProtos = new LinkedList<>();//frontendProtos是List<FrontendProto> frontendProtos;
        GenericProtocol consensusProto;
        GenericProtocol consensusdata = null;
        GenericProtocol consensusdata2=null;
        //int availableProcessors = Runtime.getRuntime().availableProcessors();
        //logger.info("Available Processors: " + availableProcessors);
        
        switch (alg) {
            case "chain_mixed":
                for (short i = 0; i < nFrontends; i++)
                    frontendProtos.add(new ChainPaxosMixedFront(configProps, i, this));
                consensusProto = new ChainPaxosMixedProto(configProps, consensusWorkerGroup);
                break;
            case "chain_mixed_2":
                for (short i = 0; i < nFrontends; i++)
                    frontendProtos.add(new ChainPaxosMixedFront(configProps, i, this));
                consensusProto = new ChainPaxosMixedProto(configProps, consensusWorkerGroup);
                break;
            case "chain_mixed_3":
                for (short i = 0; i < nFrontends; i++)
                    frontendProtos.add(new ChainPaxosMixedFront(configProps, i, this));
                consensusProto = new ChainPaxosMixedProto(configProps, consensusWorkerGroup);
                break;
            case "chain_delayed":
                for (short i = 0; i < nFrontends; i++)
                    frontendProtos.add(new ChainPaxosDelayedFront(configProps, i, this));
                consensusProto = new ChainPaxosDelayedProto(configProps, consensusWorkerGroup);
                break;
            case "chainrep":
                for (short i = 0; i < nFrontends; i++)
                    frontendProtos.add(new ChainRepMixedFront(configProps, i, this));
                consensusProto = new ChainRepMixedProto(configProps, consensusWorkerGroup);
                break;
            case "distinguished":
                for (short i = 0; i < nFrontends; i++)
                    frontendProtos.add(new DistPaxosFront(configProps, i, this));
                consensusProto = new DistPaxosProto(configProps, consensusWorkerGroup);
                break;
            case "distinguished_piggy":
                for (short i = 0; i < nFrontends; i++)
                    frontendProtos.add(new DistPaxosFront(configProps, i, this));
                consensusProto = new DistPaxosPiggyProto(configProps, consensusWorkerGroup);
                break;
            case "multi":
                for (short i = 0; i < nFrontends; i++)
                    frontendProtos.add(new DistPaxosFront(configProps, i, this));
                consensusProto = new MultiPaxosProto(configProps, consensusWorkerGroup);
                break;
            case "epaxos":
                for (short i = 0; i < nFrontends; i++)
                    frontendProtos.add(new EPaxosFront(configProps, i, this));
                consensusProto = new EPaxosProto(configProps, consensusWorkerGroup);
                break;
            case "esolatedpaxos":
                for (short i = 0; i < nFrontends; i++)
                    frontendProtos.add(new EPaxosFront(configProps, i, this));
                consensusProto = new EsolatedPaxosProto(configProps, consensusWorkerGroup);
                break;
            case "ring":
                for (short i = 0; i < nFrontends; i++)
                    frontendProtos.add(new RingPaxosFront(configProps, i, this));
                consensusProto = new RingPaxosProto(configProps, consensusWorkerGroup);
                break;
            case "uring":
                for (short i = 0; i < nFrontends; i++)
                    frontendProtos.add(new URingPaxosFront(configProps, i, this));
                consensusProto = new URingPaxosProto(configProps, consensusWorkerGroup);
                break;
            case "ringpiggy":
                for (short i = 0; i < nFrontends; i++)
                    frontendProtos.add(new RingPaxosFront(configProps, i, this));
                consensusProto = new RingPaxosPiggyProto(configProps, consensusWorkerGroup);
                break;
            //case "improchain_delayed":
            //    for (short i = 0; i < nFrontends; i++)
            //        frontendProtos.add(new ImproChainPaxosDelayedFront(configProps, i, this));
            //    consensusProto = new ImproChainPaxosDelayedProto(configProps, consensusWorkerGroup);
            //    break;
            case "TPOChain":
                for (short i = 0; i < nFrontends; i++)
                    frontendProtos.add(new TPOChainFront(configProps, i, this));
                consensusdata  =new  TPOChainData(configProps,(short)0,consensusdataWorkerGroup);
                consensusdata2  =new TPOChainData(configProps,(short)1,consensusdataWorkerGroup2);
                consensusProto = new TPOChainProto(configProps, consensusWorkerGroup);
                break;
            default:
                logger.error("Unknown algorithm: " + alg);
                System.exit(-1);
                return;
        }

        for (FrontendProto frontendProto : frontendProtos)
            babel.registerProtocol(frontendProto);
        if (consensusdata!=null){
            babel.registerProtocol(consensusdata);
            babel.registerProtocol(consensusdata2);
        }
        babel.registerProtocol(consensusProto);

        
        for (FrontendProto frontendProto : frontendProtos)
            frontendProto.init(configProps);
        if (consensusdata!=null){
            consensusdata.init(configProps);
            consensusdata2.init(configProps);
        }
        consensusProto.init(configProps);
        
        
        //线程开
        babel.start();

        Runtime.getRuntime().addShutdownHook( new Thread(new Runnable() {
            @Override
            public void run() {
                logger.info("Writes: " + nWrites + ", reads: " + nReads);
            }
        }));
        //开启app的监听服务
        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) {
                            //给pipeline管道设置处理器
                            ch.pipeline().addLast(new RequestDecoder(), new ResponseEncoder(), new ServerHandler());
                        }
                    }).option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.SO_KEEPALIVE, true);;//给workerGroup的EventLoop对应的管道设置处理器
            ChannelFuture f = b.bind(port).sync();//绑定端口号，启动服务端
            if(logger.isDebugEnabled())
                logger.debug("Listening: " + f.channel().localAddress());
            f.channel().closeFuture().sync(); //对关闭通道进行监听
            logger.info("Server channel closed");
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
            logger.info("main over");
        }
    }

    public static void main(String[] args) throws InvalidParameterException, IOException,
            HandlerRegistrationException, ProtocolAlreadyExistsException, InterruptedException {
        Properties configProps =
                Babel.loadConfig(Arrays.copyOfRange(args, 0, args.length), "config.properties");
        //logger.info(configProps);
        //传入的参数是interface=eth0 algorithm=chain_delayed initial_state=ACTIVE initial_membership=chain1,chain2,chain3 quorum_size=2
        if (configProps.containsKey("interface")) {
            String address = getAddress(configProps.getProperty("interface"));
            if (address == null) return;
            //设置   frontend_address : ____
            configProps.setProperty(FrontendProto.ADDRESS_KEY, address);
            //设置    consensus_address:____
            configProps.setProperty(ChainPaxosMixedProto.ADDRESS_KEY, address);
        }
        new HashMapApp(configProps);
    }


    /**
    * 以文本形式返回对应interface的 IP 地址字符串。
    **/
    private static String getAddress(String inter) throws SocketException {
        //NetworkInterface.getByName()搜索具有指定名称的网络接口。
        NetworkInterface byName = NetworkInterface.getByName(inter);
        if (byName == null) {
            logger.error("No interface named " + inter);
            return null;
        }
        //NetworkInterface类的getInetAddresses()是使用绑定到此网络接口的全部或部分InetAddresses返回Enumeration的便捷方法
        Enumeration<InetAddress> addresses = byName.getInetAddresses();
        InetAddress currentAddress;
        while (addresses.hasMoreElements()) {
            currentAddress = addresses.nextElement();
            if (currentAddress instanceof Inet4Address) {
                logger.info("此机器IP地址" +currentAddress.getHostAddress());
                return currentAddress.getHostAddress();//String getHostAddress()：以文本形式返回 IP 地址字符串。
            }
        }
        logger.error("No ipv4 found for interface " + inter);
        return null;
    }


    /**
     * 被单个frontend调用执行单个读操作 或 写操作
     * */
    //Called by **single** frontend thread
    @Override
    public void executeOperation(byte[] opData, boolean local, long instId) {
        HashMapOp op;
        try {
            op = HashMapOp.fromByteArray(opData);
        } catch (IOException e) {
            logger.error("Error decoding opData", e);
            throw new AssertionError("Error decoding opData");
        }
        //logger.info("Exec op: " + op + (local ? "local" : ""));

        //
        Pair<Integer, Channel> opInfo = local ? opMapper.remove(op.getId()) : null;
        if (op.getRequestType() == RequestMessage.WRITE) {
            store.put(op.getRequestKey(), op.getRequestValue());
            nWrites++;
            if (local) {
                opInfo.getRight().writeAndFlush(new ResponseMessage(opInfo.getLeft(), new byte[0]));
                if(logger.isDebugEnabled()) logger.debug("Responding");
            }
            
        } else { //READ
            if (local) {
                nReads++;
                opInfo.getRight().writeAndFlush(
                        new ResponseMessage(opInfo.getLeft(), store.getOrDefault(op.getRequestKey(), new byte[0])));
                if(logger.isDebugEnabled()) logger.debug("Responding");
            } //If remote read, nothing to do
        }
    }



    /**
     * 处理添加节点时
     * */
    @Override
    public byte[] getSnapshot() {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(baos);
            out.writeInt(nWrites);
            out.writeInt(store.size());
            for (Map.Entry<String, byte[]> e : store.entrySet()) {
                out.writeUTF(e.getKey());
                out.writeInt(e.getValue().length);
                out.write(e.getValue());
            }
            logger.info("State stored(" + nWrites + "), size: " + baos.size() );
            return baos.toByteArray();
        } catch (IOException e) {
            logger.error("Error generating state", e);
            throw new AssertionError();
        }
    }

    @Override
    public void installState(byte[] state) {
        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(state);
            DataInputStream in = new DataInputStream(bais);
            nWrites = in.readInt();
            int mapSize = in.readInt();
            store = new ConcurrentHashMap<>(mapSize);
            for (int i = 0; i < mapSize; i++) {
                String key = in.readUTF();
                byte[] value = new byte[in.readInt()];
                in.read(value);
                store.put(key, value);
            }
            logger.info("State installed(" + nWrites + ") ");
        } catch (IOException e) {
            logger.error("Error installing state", e);
            throw new AssertionError();
        }
    }



    /**
     * 自定义的Handler需要继承Netty规定好的HandlerAdapter
     * 才能被Netty框架所关联，有点类似SpringMVC的适配器模式
     **/
    class ServerHandler extends ChannelInboundHandlerAdapter {
        /**
         * 连接建立事件
         * */
        //Called by netty threads
        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            //logger.debug("Client connected: " + ctx.channel().remoteAddress());
            logger.info("Client connected: " + ctx.channel().remoteAddress());
            ctx.fireChannelActive();
        }

        /**
         * 连接关闭事件
         * */
        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            logger.info("Client connection lost: " + ctx.channel().remoteAddress());
            ctx.fireChannelInactive();
        }

        /**
         * 异常通知事件
         * */
        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            logger.error("Exception caught.", cause);
            //ctx.fireExceptionCaught(cause);
        }

        /**
        * 用户自定义事件 fireUserEventTriggered
        * */
        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
            logger.info("Unexpected event: " + evt);
            ctx.fireUserEventTriggered(evt);
        }

        //Called by netty threads
        /**
         * 获取客户端发送过来的消息
         * */
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            RequestMessage rMsg = (RequestMessage) msg;
            if(logger.isDebugEnabled())
                logger.debug("Client op: " + msg);
            if (rMsg.getRequestType() == RequestMessage.WEAK_READ) { //Exec immediately and respond
                byte[] bytes = store.get(rMsg.getRequestKey());//store类型ConcurrentMap<String, byte[]>
                ctx.channel().writeAndFlush(new ResponseMessage(rMsg.getcId(), bytes == null ? new byte[0] : bytes));
            } else { //Submit to consensus     //当rMsg.getRequestType()== WRITE   or  STRONG_READ
                int id = idCounter.incrementAndGet();
                //存档 
                opMapper.put(id, Pair.of(rMsg.getcId(), ctx.channel()));
                //发送Frontend处理
                byte[] opData = HashMapOp.toByteArray(id, rMsg.getRequestType(), rMsg.getRequestKey(), rMsg.getRequestValue());
                //List<FrontendProto> frontendProtos是List集合
                frontendProtos.get(0).submitOperation(opData, rMsg.getRequestType() == RequestMessage.WRITE ?
                        FrontendProto.OpType.WRITE : FrontendProto.OpType.STRONG_READ);
            }
        }

    }

}
