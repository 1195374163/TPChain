package TPOChain.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;

/**
 * 请求排序信息  reguest消息
 * */
public class OrderMSg extends ProtoMessage {

    public static final short MSG_CODE = 304;
    
    //二元组 标识哪个节点的第几个序号
    public final Host node;
    public final int iN;


    public OrderMSg(Host node,int iN) {
        super(MSG_CODE);
        this.node=node;
        this.iN=iN;
    }

    @Override
    public String toString() {
        return "OrderMSg{" +
                "node=" + node +
                "iN=" + iN +
                '}';
    }

    public static ISerializer<? extends ProtoMessage> serializer = new ISerializer<OrderMSg>() {
        public void serialize(OrderMSg msg, ByteBuf out) throws IOException {
            Host.serializer.serialize(msg.node,out);
            out.writeInt(msg.iN);
        }

        public OrderMSg deserialize(ByteBuf in) throws IOException {
            Host t=Host.serializer.deserialize(in);
            int iN = in.readInt();
            return new OrderMSg(t,iN);
        }
    };
}