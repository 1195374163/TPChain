package TPOChain.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

/**
 * 未定义消息让节点进行重新加入系统
 * */
public class UnaffiliatedMsg extends ProtoMessage {
    public static final short MSG_CODE = 211;

    public UnaffiliatedMsg() {
        super(MSG_CODE);
    }

    @Override
    public String toString() {
        return "UnaffiliatedMsg{}";
    }

    public static ISerializer<? extends ProtoMessage> serializer = new ISerializer<UnaffiliatedMsg>() {
        public void serialize(UnaffiliatedMsg msg, ByteBuf out) {
        }

        public UnaffiliatedMsg deserialize(ByteBuf in) {
            return new UnaffiliatedMsg();
        }
    };

}