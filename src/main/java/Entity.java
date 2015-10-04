import java.nio.ByteBuffer;

/**
 * @author Dmitry Spikhalskiy <dspikhalskiy@pulsepoint.com>
 */
public class Entity {
    private Type etype;
    private ByteBuffer payload;

    public Type getEtype() {
        return etype;
    }

    public void setEtype(Type etype) {
        this.etype = etype;
    }

    public ByteBuffer getPayload() {
        return payload;
    }

    public void setPayload(ByteBuffer payload) {
        this.payload = payload;
    }
}
