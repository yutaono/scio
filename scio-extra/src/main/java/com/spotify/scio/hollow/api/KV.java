package com.spotify.scio.hollow.api;

import com.netflix.hollow.api.objects.HollowObject;
import com.netflix.hollow.core.schema.HollowObjectSchema;

@SuppressWarnings("all")
public class KV extends HollowObject {

    public KV(KVDelegate delegate, int ordinal) {
        super(delegate, ordinal);
    }

    public byte[] getKey() {
        return delegate().getKey(ordinal);
    }

    public byte[] getValue() {
        return delegate().getValue(ordinal);
    }

    public KVAPI api() {
        return typeApi().getAPI();
    }

    public KVTypeAPI typeApi() {
        return delegate().getTypeAPI();
    }

    protected KVDelegate delegate() {
        return (KVDelegate)delegate;
    }

}