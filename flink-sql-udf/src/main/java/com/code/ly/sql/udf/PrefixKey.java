package com.code.ly.sql.udf;

import com.getui.axe.gid.service.GidBaseService;
import com.getui.axe.v3.thrift.gid.DeviceType;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

public class PrefixKey extends ScalarFunction {
    GidBaseService service = null;

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        service = new GidBaseService();
    }

    public String eval(String id, String idType) {
        byte[] key = null;
        switch (idType) {
            case "cid":
                key = service.getId2GidKey(id, DeviceType.cid);
                break;
            default:
                break;
        }

        if (key == null) {
            return null;
        }

        return new String(key);
    }
}
