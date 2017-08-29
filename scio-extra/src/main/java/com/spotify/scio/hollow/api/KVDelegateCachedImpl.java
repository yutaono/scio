package com.spotify.scio.hollow.api;

import com.netflix.hollow.api.objects.delegate.HollowObjectAbstractDelegate;
import com.netflix.hollow.core.read.dataaccess.HollowObjectTypeDataAccess;
import com.netflix.hollow.core.schema.HollowObjectSchema;
import com.netflix.hollow.api.custom.HollowTypeAPI;
import com.netflix.hollow.api.objects.delegate.HollowCachedDelegate;

@SuppressWarnings("all")
public class KVDelegateCachedImpl extends HollowObjectAbstractDelegate implements HollowCachedDelegate, KVDelegate {

    private final byte[] key;
    private final byte[] value;
   private KVTypeAPI typeAPI;

    public KVDelegateCachedImpl(KVTypeAPI typeAPI, int ordinal) {
        this.key = typeAPI.getKey(ordinal);
        this.value = typeAPI.getValue(ordinal);
        this.typeAPI = typeAPI;
    }

    public byte[] getKey(int ordinal) {
        return key;
    }

    public byte[] getValue(int ordinal) {
        return value;
    }

    @Override
    public HollowObjectSchema getSchema() {
        return typeAPI.getTypeDataAccess().getSchema();
    }

    @Override
    public HollowObjectTypeDataAccess getTypeDataAccess() {
        return typeAPI.getTypeDataAccess();
    }

    public KVTypeAPI getTypeAPI() {
        return typeAPI;
    }

    public void updateTypeAPI(HollowTypeAPI typeAPI) {
        this.typeAPI = (KVTypeAPI) typeAPI;
    }

}