package com.spotify.scio.hollow.api;

import com.netflix.hollow.api.objects.delegate.HollowObjectDelegate;


@SuppressWarnings("all")
public interface KVDelegate extends HollowObjectDelegate {

    public byte[] getKey(int ordinal);

    public byte[] getValue(int ordinal);

    public KVTypeAPI getTypeAPI();

}