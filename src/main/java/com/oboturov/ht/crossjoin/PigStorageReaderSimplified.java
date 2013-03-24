package com.oboturov.ht.crossjoin;

import org.apache.hadoop.io.Text;
import org.apache.pig.LoadCaster;
import org.apache.pig.ResourceSchema;
import org.apache.pig.builtin.Utf8StorageConverter;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.util.CastUtils;
import org.apache.pig.impl.util.Utils;

import java.io.IOException;
import java.util.ArrayList;

/**
 * This code borrowed from the {@link org.apache.pig.builtin.PigStorage} class.
 *
 * @author aoboturov
 */
public class PigStorageReaderSimplified {

    private byte fieldDel = '\t';
    private String serializedSchema;

    private TupleFactory mTupleFactory = TupleFactory.getInstance();

    public PigStorageReaderSimplified(final String schema) {
        serializedSchema = schema;
    }

    private void addTupleValue(ArrayList<Object> tuple, byte[] buf, int start, int end) {
        tuple.add(readField(buf, start, end));
    }

    /**
     * Read the bytes between start and end into a DataByteArray for inclusion in the return tuple.
     * @param bytes byte array to copy data from
     * @param start starting point to copy from
     * @param end ending point to copy to, exclusive.
     * @return
     */
    protected DataByteArray readField(byte[] bytes, int start, int end) {
        if (start == end) {
            return null;
        } else {
            return new DataByteArray(bytes, start, end);
        }
    }

    /**
     * This will be called on the front end during planning and not on the back
     * end during execution.
     * @return the {@link org.apache.pig.LoadCaster} associated with this loader. Returning null
     * indicates that casts from byte array are not supported for this loader.
     * construction
     * @throws java.io.IOException if there is an exception during LoadCaster
     */
    public LoadCaster getLoadCaster() throws IOException {
        return new Utf8StorageConverter();
    }

    public Tuple getNext(final String line) throws IOException {
        ArrayList<Object> mProtoTuple = new ArrayList<Object>();
        Text value = new Text(line);
        byte[] buf = value.getBytes();
        int len = value.getLength();
        int start = 0;
        int fieldID = 0;
        for (int i = 0; i < len; i++) {
            if (buf[i] == fieldDel) {
                addTupleValue(mProtoTuple, buf, start, i);
                start = i + 1;
                fieldID++;
            }
        }
        // pick up the last field
        if (start <= len) {
            addTupleValue(mProtoTuple, buf, start, len);
        }
        Tuple t =  mTupleFactory.newTupleNoCopy(mProtoTuple);

        return applySchema(t);
    }

    private Tuple applySchema(Tuple tup) throws IOException {
        final LoadCaster caster = getLoadCaster();
        final ResourceSchema schema;
        schema = new ResourceSchema(Utils.getSchemaFromString(serializedSchema));

        ResourceSchema.ResourceFieldSchema[] fieldSchemas = schema.getFields();
        int tupleIdx = 0;
        // If some fields have been projected out, the tuple
        // only contains required fields.
        // We walk the requiredColumns array to find required fields,
        // and cast those.
        for (int i = 0; i < Math.min(fieldSchemas.length, tup.size()); i++) {
            Object val = null;
            if(tup.get(tupleIdx) != null){
                byte[] bytes = ((DataByteArray) tup.get(tupleIdx)).get();
                val = CastUtils.convertToType(caster, bytes,
                        fieldSchemas[i], fieldSchemas[i].getType());
                tup.set(tupleIdx, val);
            }
            tupleIdx++;
        }
        for (int i = tup.size(); i < fieldSchemas.length; i++) {
            tup.append(null);
        }
        return tup;
    }

}
