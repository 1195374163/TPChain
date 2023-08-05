package TPOChain.messages;

import TPOChain.utils.InstanceState;
import TPOChain.utils.InstanceStateCL;
import TPOChain.utils.RuntimeConfigure;
import TPOChain.utils.SeqN;
import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.*;

public class JoinSuccessMsg extends ProtoMessage {

    public static final short MSG_CODE = 205;

    public final int iN;
    public final SeqN sN;
    public final List<Host> membership;
    public  HashSet<Host> pendingRemove;
    // TODO: 2023/5/23  对日志改成 acceptValue和acceptedValueCl来传输，传输日志消息 ，以一个列表传输，不以消息传输
    public  Map<Host, RuntimeConfigure>  hostConfigureMap;
    public  Map<Host,Map<Integer, InstanceState>> instances;
    public  Map<Integer, InstanceStateCL> globalinstances;
    
    public  int executeid;
    public  int ackid;
    public  int decideid;
    public  int acceptid;
    
    public JoinSuccessMsg(int iN, SeqN sN,List<Host> membership) {
        super(MSG_CODE);
        this.iN = iN;
        this.sN = sN;
        this.membership = membership;
    }
    public JoinSuccessMsg(int iN, SeqN sN,List<Host> membership,HashSet<Host> pendingRemove,
                          Map<Host, RuntimeConfigure>  hostConfigureMap,Map<Host,Map<Integer, InstanceState>> instances,
                          Map<Integer, InstanceStateCL> globalinstances,
                          int executeid,int ackid,int decideid,int acceptid) {
        super(MSG_CODE);
        this.iN = iN;
        this.sN = sN;
        this.membership = membership;
        this.pendingRemove=pendingRemove;
        this.instances=instances;
        this.hostConfigureMap=hostConfigureMap;
        this.globalinstances=globalinstances;
        this.executeid=executeid;
        this.ackid=ackid;
        this.decideid=decideid;
        this.acceptid=acceptid;

    }

    @Override
    public String toString() {
        return "JoinSuccessMsg{" +
                "iN=" + iN +
                ", sN=" + sN +
                ", membership=" + membership +
                '}';
    }

    public static ISerializer<? extends ProtoMessage> serializer = new ISerializer<JoinSuccessMsg>() {
        public void serialize(JoinSuccessMsg msg, ByteBuf out) throws IOException {
            out.writeInt(msg.iN);
            msg.sN.serialize(out);
            
            out.writeInt(msg.membership.size());
            for (Host h : msg.membership)
                Host.serializer.serialize(h, out);

            out.writeInt(msg.pendingRemove.size());
            for (Host host : msg.pendingRemove) {
                Host.serializer.serialize(host, out);
            }


            out.writeInt(msg.instances.size());
            for (Map.Entry<Host, Map<Integer, InstanceState>> entry : msg.instances.entrySet()) {
                Host key = entry.getKey();
                Host.serializer.serialize(key, out);
                Map<Integer, InstanceState> value = entry.getValue();
                out.writeInt(value.size());
                for (Map.Entry<Integer, InstanceState> entryValue : value.entrySet()) {
                    int keyValue = entryValue.getKey();
                    InstanceState valueValue = entryValue.getValue();
                    out.writeInt(keyValue);
                   // InstanceState.serializer.serialize(value, out);
                }
            }
            
            
            
            
            //out.writeInt(map.size());
            //for (Map.Entry<Host, RuntimeConfigure> entry : map.entrySet()) {
            //    Host key = entry.getKey();
            //    RuntimeConfigure value = entry.getValue();
            //
            //    Host.serializer.serialize(key, out);
            //    //RuntimeConfigure.serializer.serialize(value, out);
            //}
            //
            
        }

        public JoinSuccessMsg deserialize(ByteBuf in) throws IOException {
            int instanceNumber = in.readInt();
            SeqN seqN = SeqN.deserialize(in);
            
            int membershipSize = in.readInt();
            List<Host> membership = new ArrayList<>(membershipSize);
            for (int i = 0; i < membershipSize; i++)
                membership.add(Host.serializer.deserialize(in));

            int size = in.readInt();
            HashSet<Host> pendingRemove = new HashSet<>(size);
            for (int i = 0; i < size; i++) {
                Host host = Host.serializer.deserialize(in);
                pendingRemove.add(host);
            }
            
            
            
            
            return new JoinSuccessMsg(instanceNumber, seqN, membership);
        }
    };
}
