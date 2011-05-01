/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package net.ooici.netcdf.iosp;

//import biz.source_code.base64Coder.Base64Coder;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Properties;
import net.ooici.cdm.syntactic.Cdmdatatype;
import ucar.ma2.DataType;

/**
 *
 * @author cmueller
 */
public class IospUtils {

    public static HashMap<String, String> props;

    static {
        props = new HashMap<String, String>();
    }

    @Deprecated
    public static String serialize(Object obj) throws IOException {
        String ret = null;
        ObjectOutputStream out = null;
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            out = new ObjectOutputStream(baos);
            out.writeObject(obj);

//            ret = new String(Base64Coder.encode(baos.toByteArray()));
        } finally {
            if (out != null) {
                out.close();
            }
        }
        return ret;
    }

    @Deprecated
    public static Object deserialize(String s) throws IOException, ClassNotFoundException {
        Object ret = null;
        ObjectInputStream in = null;
        try {
//            byte[] buf = Base64Coder.decode(s);
//            ByteArrayInputStream bais = new ByteArrayInputStream(buf);
//            in = new ObjectInputStream(bais);
//            ret = in.readObject();
        } finally {
            if (in != null) {
                in.close();
            }
        }
        return ret;
    }

    /**
     * Adds the properties in the provided file to the static props map and returns the map.
     * If <code>propertiesFile</code> is null, the current map is returned.
     * @param propertiesFile
     * @return
     * @throws IOException
     */
    public static HashMap<String, String> parseProperties(File propertiesFile) throws IOException {
        Properties propsIn = new Properties();
        propsIn.load(new FileInputStream(propertiesFile));
        for (Iterator<Entry<Object, Object>> iter = propsIn.entrySet().iterator(); iter.hasNext();) {
            Entry<Object, Object> entry = iter.next();
            props.put((String) entry.getKey(), (String) entry.getValue());
        }
        return props;
    }

    public static Cdmdatatype.DataType getOoiDataType(DataType ucarDT) {
        Cdmdatatype.DataType ret = null;

        switch (ucarDT) {
            case BOOLEAN:
                ret = Cdmdatatype.DataType.BOOLEAN;
                break;
            case BYTE:
                ret = Cdmdatatype.DataType.BYTE;
                break;
            case SHORT:
                ret = Cdmdatatype.DataType.SHORT;
                break;
            case INT:
                ret = Cdmdatatype.DataType.INT;
                break;
            case LONG:
                ret = Cdmdatatype.DataType.LONG;
                break;
            case FLOAT:
                ret = Cdmdatatype.DataType.FLOAT;
                break;
            case DOUBLE:
                ret = Cdmdatatype.DataType.DOUBLE;
                break;
            case CHAR:
                ret = Cdmdatatype.DataType.CHAR;
                break;
            case STRING:
                ret = Cdmdatatype.DataType.STRING;
                break;
            case STRUCTURE:
                ret = Cdmdatatype.DataType.STRUCTURE;
                break;
            case SEQUENCE:
                ret = Cdmdatatype.DataType.SEQUENCE;
                break;
            case ENUM1:
            case ENUM2:
            case ENUM4:
                ret = Cdmdatatype.DataType.ENUM;
                break;
            case OPAQUE:
                ret = Cdmdatatype.DataType.OPAQUE;
                break;
            default:
                ret = Cdmdatatype.DataType.STRING;
        }
        return ret;
    }

    public static DataType getNcDataType(Cdmdatatype.DataType ooiDT) {
        DataType ret = null;

        switch (ooiDT) {
            case BOOLEAN:
                ret = DataType.BOOLEAN;
                break;
            case BYTE:
                ret = DataType.BYTE;
                break;
            case SHORT:
                ret = DataType.SHORT;
                break;
            case INT:
                ret = DataType.INT;
                break;
            case LONG:
                ret = DataType.LONG;
                break;
            case FLOAT:
                ret = DataType.FLOAT;
                break;
            case DOUBLE:
                ret = DataType.DOUBLE;
                break;
            case CHAR:
                ret = DataType.CHAR;
                break;
            case STRING:
                ret = DataType.STRING;
                break;
            case STRUCTURE:
                ret = DataType.STRUCTURE;
                break;
            case SEQUENCE:
                ret = DataType.SEQUENCE;
                break;
            case ENUM:
                ret = DataType.ENUM1;
                break;
            case OPAQUE:
                ret = DataType.OPAQUE;
                break;
            default:
                ret = DataType.STRING;
        }
        return ret;
    }
}
