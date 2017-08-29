package com.spotify.scio.hollow.api;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.Map;
import com.netflix.hollow.api.custom.HollowAPI;
import com.netflix.hollow.core.read.dataaccess.HollowDataAccess;
import com.netflix.hollow.core.read.dataaccess.HollowTypeDataAccess;
import com.netflix.hollow.core.read.dataaccess.HollowObjectTypeDataAccess;
import com.netflix.hollow.core.read.dataaccess.HollowListTypeDataAccess;
import com.netflix.hollow.core.read.dataaccess.HollowSetTypeDataAccess;
import com.netflix.hollow.core.read.dataaccess.HollowMapTypeDataAccess;
import com.netflix.hollow.core.read.dataaccess.missing.HollowObjectMissingDataAccess;
import com.netflix.hollow.core.read.dataaccess.missing.HollowListMissingDataAccess;
import com.netflix.hollow.core.read.dataaccess.missing.HollowSetMissingDataAccess;
import com.netflix.hollow.core.read.dataaccess.missing.HollowMapMissingDataAccess;
import com.netflix.hollow.api.objects.provider.HollowFactory;
import com.netflix.hollow.api.objects.provider.HollowObjectProvider;
import com.netflix.hollow.api.objects.provider.HollowObjectCacheProvider;
import com.netflix.hollow.api.objects.provider.HollowObjectFactoryProvider;
import com.netflix.hollow.api.sampling.HollowObjectCreationSampler;
import com.netflix.hollow.api.sampling.HollowSamplingDirector;
import com.netflix.hollow.api.sampling.SampleResult;
import com.netflix.hollow.core.util.AllHollowRecordCollection;

@SuppressWarnings("all")
public class KVAPI extends HollowAPI {

    private final HollowObjectCreationSampler objectCreationSampler;

    private final KVTypeAPI kVTypeAPI;

    private final HollowObjectProvider kVProvider;

    public KVAPI(HollowDataAccess dataAccess) {
        this(dataAccess, Collections.<String>emptySet());
    }

    public KVAPI(HollowDataAccess dataAccess, Set<String> cachedTypes) {
        this(dataAccess, cachedTypes, Collections.<String, HollowFactory<?>>emptyMap());
    }

    public KVAPI(HollowDataAccess dataAccess, Set<String> cachedTypes, Map<String, HollowFactory<?>> factoryOverrides) {
        this(dataAccess, cachedTypes, factoryOverrides, null);
    }

    public KVAPI(HollowDataAccess dataAccess, Set<String> cachedTypes, Map<String, HollowFactory<?>> factoryOverrides, KVAPI previousCycleAPI) {
        super(dataAccess);
        HollowTypeDataAccess typeDataAccess;
        HollowFactory factory;

        objectCreationSampler = new HollowObjectCreationSampler("KV");

        typeDataAccess = dataAccess.getTypeDataAccess("KV");
        if(typeDataAccess != null) {
            kVTypeAPI = new KVTypeAPI(this, (HollowObjectTypeDataAccess)typeDataAccess);
        } else {
            kVTypeAPI = new KVTypeAPI(this, new HollowObjectMissingDataAccess(dataAccess, "KV"));
        }
        addTypeAPI(kVTypeAPI);
        factory = factoryOverrides.get("KV");
        if(factory == null)
            factory = new KVHollowFactory();
        if(cachedTypes.contains("KV")) {
            HollowObjectCacheProvider previousCacheProvider = null;
            if(previousCycleAPI != null && (previousCycleAPI.kVProvider instanceof HollowObjectCacheProvider))
                previousCacheProvider = (HollowObjectCacheProvider) previousCycleAPI.kVProvider;
            kVProvider = new HollowObjectCacheProvider(typeDataAccess, kVTypeAPI, factory, previousCacheProvider);
        } else {
            kVProvider = new HollowObjectFactoryProvider(typeDataAccess, kVTypeAPI, factory);
        }

    }

    public void detachCaches() {
        if(kVProvider instanceof HollowObjectCacheProvider)
            ((HollowObjectCacheProvider)kVProvider).detach();
    }

    public KVTypeAPI getKVTypeAPI() {
        return kVTypeAPI;
    }
    public Collection<KV> getAllKV() {
        return new AllHollowRecordCollection<KV>(getDataAccess().getTypeDataAccess("KV").getTypeState()) {
            protected KV getForOrdinal(int ordinal) {
                return getKV(ordinal);
            }
        };
    }
    public KV getKV(int ordinal) {
        objectCreationSampler.recordCreation(0);
        return (KV)kVProvider.getHollowObject(ordinal);
    }
    public void setSamplingDirector(HollowSamplingDirector director) {
        super.setSamplingDirector(director);
        objectCreationSampler.setSamplingDirector(director);
    }

    public Collection<SampleResult> getObjectCreationSamplingResults() {
        return objectCreationSampler.getSampleResults();
    }

}
