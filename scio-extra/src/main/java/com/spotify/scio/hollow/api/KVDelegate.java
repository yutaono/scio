package com.spotify.scio.hollow.api;

import com.netflix.hollow.api.objects.delegate.HollowObjectDelegate;


@SuppressWarnings("all")
public interface KVDelegate extends HollowObjectDelegate {

    public String getKey(int ordinal);

    public boolean isKeyEqual(int ordinal, String testValue);

    public String getValue(int ordinal);

    public boolean isValueEqual(int ordinal, String testValue);

    public KVTypeAPI getTypeAPI();

}