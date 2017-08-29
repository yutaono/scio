package com.spotify.scio.hollow.api;

import com.netflix.hollow.api.objects.delegate.HollowObjectAbstractDelegate;
import com.netflix.hollow.core.read.dataaccess.HollowObjectTypeDataAccess;
import com.netflix.hollow.core.schema.HollowObjectSchema;

@SuppressWarnings("all")
public class KVDelegateLookupImpl extends HollowObjectAbstractDelegate implements KVDelegate {

    private final KVTypeAPI typeAPI;

    public KVDelegateLookupImpl(KVTypeAPI typeAPI) {
        this.typeAPI = typeAPI;
    }

    public byte[] getKey(int ordinal) {
        return typeAPI.getKey(ordinal);
    }

    public byte[] getValue(int ordinal) {
        return typeAPI.getValue(ordinal);
    }

    public KVTypeAPI getTypeAPI() {
        return typeAPI;
    }

    @Override
    public HollowObjectSchema getSchema() {
        return typeAPI.getTypeDataAccess().getSchema();
    }

    @Override
    public HollowObjectTypeDataAccess getTypeDataAccess() {
        return typeAPI.getTypeDataAccess();
    }

}