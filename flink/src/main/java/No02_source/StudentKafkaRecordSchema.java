package No02_source;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class StudentKafkaRecordSchema implements DeserializationSchema<StudentKafkaRecord> {
    /**
     * 将kafka中二进制数据反序列化为java对象
     * @param message
     * @return
     * @throws IOException
     */
    @Override
    public StudentKafkaRecord deserialize(byte[] message) throws IOException {
        String str = new String(message);
        StudentKafkaRecord studentKafkaRecord = new StudentKafkaRecord(str);
        return studentKafkaRecord;
    }


    //无界流 不能结束
    @Override
    public boolean isEndOfStream(StudentKafkaRecord nextElement) {
        return false;
    }

    @Override
    public TypeInformation<StudentKafkaRecord> getProducedType() {
        return TypeInformation.of(StudentKafkaRecord.class);
    }
}
