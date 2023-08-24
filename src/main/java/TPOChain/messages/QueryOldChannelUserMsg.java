package TPOChain.messages;

import TPOChain.utils.SeqN;
import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class QueryOldChannelUserMsg extends ProtoMessage {
    public static final short MSG_CODE = 701;


    public final List<AbstractMap.SimpleEntry<Host, Integer>> request;
    public final SeqN sN;
    
    public QueryOldChannelUserMsg( SeqN sN,List<AbstractMap.SimpleEntry<Host, Integer>>_request) {
        super(MSG_CODE);
        this.sN = sN;
        request=_request;
    }

    @Override
    public String toString() {
        return "QueryOldChannelUserMsg{" +
                "request=" + request.toString() +
                ", sN=" + sN +
                '}';
    }

    public static ISerializer<? extends ProtoMessage> serializer = new ISerializer<QueryOldChannelUserMsg>() {
        public void serialize(QueryOldChannelUserMsg msg, ByteBuf out) throws IOException {
            //out.writeInt(msg.iN);
            msg.sN.serialize(out);
            
            // 先写入数量
            out.writeInt(msg.request.size());  // Serialize the size of the list
            for (AbstractMap.SimpleEntry<Host, Integer> pair : msg.request) {
                Host host = pair.getKey();
                Host.serializer.serialize(host, out);
                int value = pair.getValue();
                out.writeInt(value);
            }
        }

        public QueryOldChannelUserMsg deserialize(ByteBuf in) throws IOException {
            SeqN sN = SeqN.deserialize(in);
            
            int listSize = in.readInt();  // Deserialize the size of the list
            List<AbstractMap.SimpleEntry<Host, Integer>> request = new ArrayList<>(listSize);
            for (int i = 0; i < listSize; i++) {
                // Deserialize host and value
                // ...
                Host node = Host.serializer.deserialize(in);
                int iN=in.readInt();
                // You need to implement deserialization for Host and Integer types
                AbstractMap.SimpleEntry<Host, Integer> pair = new AbstractMap.SimpleEntry<>(node, iN);
                request.add(pair);
            }
            return new QueryOldChannelUserMsg(sN,request);
        }
    };
}




