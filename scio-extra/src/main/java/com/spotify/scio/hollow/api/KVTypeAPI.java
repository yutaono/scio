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

    public String getKey(int ordinal) {
        if(fieldIndex[0] == -1)
            return missingDataHandler().handleString("KV", ordinal, "key");
        boxedFieldAccessSampler.recordFieldAccess(fieldIndex[0]);
        return getTypeDataAccess().readString(ordinal, fieldIndex[0]);
    }

    public boolean isKeyEqual(int ordinal, String testValue) {
        if(fieldIndex[0] == -1)
            return missingDataHandler().handleStringEquals("KV", ordinal, "key", testValue);
        return getTypeDataAccess().isStringFieldEqual(ordinal, fieldIndex[0], testValue);
    }

    public String getValue(int ordinal) {
        if(fieldIndex[1] == -1)
            return missingDataHandler().handleString("KV", ordinal, "value");
        boxedFieldAccessSampler.recordFieldAccess(fieldIndex[1]);
        return getTypeDataAccess().readString(ordinal, fieldIndex[1]);
    }

    public boolean isValueEqual(int ordinal, String testValue) {
        if(fieldIndex[1] == -1)
            return missingDataHandler().handleStringEquals("KV", ordinal, "value", testValue);
        return getTypeDataAccess().isStringFieldEqual(ordinal, fieldIndex[1], testValue);
    }

    public KVDelegateLookupImpl getDelegateLookupImpl() {
        return delegateLookupImpl;
    }

    @Override
    public KVAPI getAPI() {
        return (KVAPI) api;
    }

}