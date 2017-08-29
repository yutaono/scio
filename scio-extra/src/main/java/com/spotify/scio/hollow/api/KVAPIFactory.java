package com.spotify.scio.hollow.api;

import com.netflix.hollow.api.client.HollowAPIFactory;
import com.netflix.hollow.api.custom.HollowAPI;
import com.netflix.hollow.api.objects.provider.HollowFactory;
import com.netflix.hollow.core.read.dataaccess.HollowDataAccess;
import java.util.Collections;
import java.util.Set;

public class KVAPIFactory implements HollowAPIFactory {

    private final Set<String> cachedTypes;

    public KVAPIFactory() {
        this(Collections.<String>emptySet());
    }

    public KVAPIFactory(Set<String> cachedTypes) {
        this.cachedTypes = cachedTypes;
    }

    @Override
    public HollowAPI createAPI(HollowDataAccess dataAccess) {
        return new KVAPI(dataAccess, cachedTypes);
    }

    @Override
    public HollowAPI createAPI(HollowDataAccess dataAccess, HollowAPI previousCycleAPI) {
        return new KVAPI(dataAccess, cachedTypes, Collections.<String, HollowFactory<?>>emptyMap(), (KVAPI) previousCycleAPI);
    }

}