package com.spotify.scio.hollow.api;

import com.netflix.hollow.api.custom.HollowObjectTypeAPI;
import com.netflix.hollow.core.read.dataaccess.HollowObjectTypeDataAccess;

@SuppressWarnings("all")
public class KVTypeAPI extends HollowObjectTypeAPI {

    private final KVDelegateLookupImpl delegateLookupImpl;

    KVTypeAPI(KVAPI api, HollowObjectTypeDataAccess typeDataAccess) {
        super(api, typeDataAccess, new String[] {
            "key",
            "value"
        });
        this.delegateLookupImpl = new KVDelegateLookupImpl(this);
    }

    public byte[] getKey(int ordinal) {
        if(fieldIndex[0] == -1)
            return missingDataHandler().handleBytes("KV", ordinal, "key");
        boxedFieldAccessSampler.recordFieldAccess(fieldIndex[0]);
        return getTypeDataAccess().readBytes(ordinal, fieldIndex[0]);
    }



    public byte[] getValue(int ordinal) {
        if(fieldIndex[1] == -1)
            return missingDataHandler().handleBytes("KV", ordinal, "value");
        boxedFieldAccessSampler.recordFieldAccess(fieldIndex[1]);
        return getTypeDataAccess().readBytes(ordinal, fieldIndex[1]);
    }



    public KVDelegateLookupImpl getDelegateLookupImpl() {
        return delegateLookupImpl;
    }

    @Override
    public KVAPI getAPI() {
        return (KVAPI) api;
    }

}