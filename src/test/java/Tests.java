import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.apache.avro.io.*;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;


/**
 * This 2 test different in only type of payload field - HeapByteBuffer or DirectByteBuffer
 * @author Dmitry Spikhalskiy <dspikhalskiy@pulsepoint.com>
 */
public class Tests {
    @Test
    public void failingTest() throws IOException {
        //construct entity

        // BufferedBinaryEncoder#159 (!bytes.hasArray() && bytes.remaining() > bulkLimit) - here we make "!bytes.hasArray()"
        ByteBuffer payload = ByteBuffer.allocateDirect(8 * 1024);

        // BufferedBinaryEncoder#159 (!bytes.hasArray() && bytes.remaining() > bulkLimit) - here we make "bytes.remaining() > bulkLimit"
        for (int i=0; i<500; i++) {
            payload.putInt(1);
        }

        payload.flip();

        Entity entity = new Entity();
        entity.setEtype(Type.ONE);
        entity.setPayload(payload);

        //serialize
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ReflectDatumWriter<Entity> datumWriter = new ReflectDatumWriter<Entity>(Entity.class);
        BinaryEncoder avroEncoder = EncoderFactory.get().blockingBinaryEncoder(outputStream, null);
        datumWriter.write(entity, avroEncoder);
        avroEncoder.flush();

        byte[] bytes = outputStream.toByteArray();

        //deserialize
        ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
        ReflectDatumReader<Entity> datumReader = new ReflectDatumReader<Entity>(Entity.class);
        BinaryDecoder avroDecoder = DecoderFactory.get().binaryDecoder(inputStream, null);
        Entity deserailized = datumReader.read(null, avroDecoder);

        assertEquals(entity.getEtype(), deserailized.getEtype());
        assertEquals(entity.getPayload(), deserailized.getPayload());
    }

    //difference with previous one - only in "ByteBuffer payload = ByteBuffer.allocate(8 * 1024);"
    @Test
    public void goodTest() throws IOException {
        //construct entity

        //BufferedBinaryEncoder#159 (!bytes.hasArray() && bytes.remaining() > bulkLimit) - here we fail check "!bytes.hasArray()"
        ByteBuffer payload = ByteBuffer.allocate(8 * 1024);

        // BufferedBinaryEncoder#159 (!bytes.hasArray() && bytes.remaining() > bulkLimit) - here we make "bytes.remaining() > bulkLimit"
        for (int i=0; i<500; i++) {
            payload.putInt(1);
        }

        payload.flip();

        Entity entity = new Entity();
        entity.setEtype(Type.ONE);
        entity.setPayload(payload);

        //serialize
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ReflectDatumWriter<Entity> datumWriter = new ReflectDatumWriter<Entity>(Entity.class);
        BinaryEncoder avroEncoder = EncoderFactory.get().blockingBinaryEncoder(outputStream, null);
        datumWriter.write(entity, avroEncoder);
//        outputStream.close();
        avroEncoder.flush();
        byte[] bytes = outputStream.toByteArray();

        //deserialize
        ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
        ReflectDatumReader<Entity> datumReader = new ReflectDatumReader<Entity>(Entity.class);
        BinaryDecoder avroDecoder = DecoderFactory.get().binaryDecoder(inputStream, null);
        Entity deserailized = datumReader.read(null, avroDecoder);

        assertEquals(entity.getEtype(), deserailized.getEtype());
        assertEquals(entity.getPayload(), deserailized.getPayload());
    }
}
