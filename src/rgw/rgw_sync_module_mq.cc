#include "rgw_common.h"
#include "rgw_coroutine.h"
#include "rgw_cr_rados.h"
#include "rgw_sync_module.h"
#include "rgw_data_sync.h"
#include "rgw_sync_module_mq.h"

#define dout_subsys ceph_subsys_rgw

class RGWMQStatRemoteObjCBCR : public RGWStatRemoteObjCBCR {
public:
  RGWMQStatRemoteObjCBCR(RGWDataSyncEnv *_sync_env,
                          RGWBucketInfo& _bucket_info, rgw_obj_key& _key) : RGWStatRemoteObjCBCR(_sync_env, _bucket_info, _key) {}
  int operate() override {
    ldout(sync_env->cct, 0) << "SYNC_MQ: stat of remote obj: z=" << sync_env->source_zone
                            << " b=" << bucket_info.bucket << " k=" << key << " size=" << size << " mtime=" << mtime
                            << " attrs=" << attrs << dendl;
    return set_cr_done();
  }

};

class RGWMQStatRemoteObjCR : public RGWCallStatRemoteObjCR {
public:
  RGWMQStatRemoteObjCR(RGWDataSyncEnv *_sync_env,
                        RGWBucketInfo& _bucket_info, rgw_obj_key& _key) : RGWCallStatRemoteObjCR(_sync_env, _bucket_info, _key) {
  }

  ~RGWMQStatRemoteObjCR() override {}

  RGWStatRemoteObjCBCR *allocate_callback() override {
    return new RGWMQStatRemoteObjCBCR(sync_env, bucket_info, key);
  }
};

class RGWMQDataSyncModule : public RGWDataSyncModule {
  string endpoint;
public:
  RGWMQDataSyncModule(const string& _endpoint) : endpoint(_endpoint) {}

  RGWCoroutine *sync_object(RGWDataSyncEnv *sync_env, RGWBucketInfo& bucket_info, rgw_obj_key& key, uint64_t versioned_epoch, rgw_zone_set *zones_trace) override {
    ldout(sync_env->cct, 0) << endpoint << ": SYNC_MQ: sync_object: b=" << bucket_info.bucket << " k=" << key << " versioned_epoch=" << versioned_epoch << dendl;
    return new RGWMQStatRemoteObjCR(sync_env, bucket_info, key);
  }
  RGWCoroutine *remove_object(RGWDataSyncEnv *sync_env, RGWBucketInfo& bucket_info, rgw_obj_key& key, real_time& mtime, bool versioned, uint64_t versioned_epoch, rgw_zone_set *zones_trace) override {
    ldout(sync_env->cct, 0) << endpoint << ": SYNC_MQ: rm_object: b=" << bucket_info.bucket << " k=" << key << " mtime=" << mtime << " versioned=" << versioned << " versioned_epoch=" << versioned_epoch << dendl;
    return NULL;
  }
  RGWCoroutine *create_delete_marker(RGWDataSyncEnv *sync_env, RGWBucketInfo& bucket_info, rgw_obj_key& key, real_time& mtime,
                                     rgw_bucket_entry_owner& owner, bool versioned, uint64_t versioned_epoch, rgw_zone_set *zones_trace) override {
    ldout(sync_env->cct, 0) << endpoint << ": SYNC_MQ: create_delete_marker: b=" << bucket_info.bucket << " k=" << key << " mtime=" << mtime
                            << " versioned=" << versioned << " versioned_epoch=" << versioned_epoch << dendl;
    return NULL;
  }
};

class RGWMQSyncModuleInstance : public RGWSyncModuleInstance {
  RGWMQDataSyncModule data_handler;
public:
  RGWMQSyncModuleInstance(const string& endpoint) : data_handler(endpoint) {}
  RGWDataSyncModule *get_data_handler() override {
    return &data_handler;
  }
};

int RGWMQSyncModule::create_instance(CephContext *cct, map<string, string, ltstr_nocase>& config, RGWSyncModuleInstanceRef *instance) {
  string endpoint;
  auto i = config.find("endpoint");
  if (i != config.end()) {
    endpoint = i->second;
  }
  instance->reset(new RGWMQSyncModuleInstance(endpoint));
  return 0;
}

