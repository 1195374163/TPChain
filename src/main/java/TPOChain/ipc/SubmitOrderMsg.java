package TPOChain.ipc;

import TPOChain.messages.OrderMSg;
import frontend.ops.OpBatch;
import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;

public class SubmitOrderMsg extends ProtoRequest {
    
    public static final short REQUEST_ID = 201;

    private final OrderMSg ordermsg;

    public SubmitOrderMsg(OrderMSg ordermsg) {
        super(REQUEST_ID);
        this.ordermsg = ordermsg;
    }
    
    public OrderMSg  getOrdermsg(){
        return  ordermsg;
    }
}