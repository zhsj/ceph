#include "rgw_common.h"
#include "rgw_coroutine.h"
#include "rgw_cr_rados.h"
#include "rgw_sync_module.h"
#include "rgw_data_sync.h"
#include "rgw_sync_module_mq.h"
#include "rgw_rest_conn.h"
#include "rgw_cr_rest.h"


#include <boost/asio/yield.hpp>

#define dout_subsys ceph_subsys_rgw

struct MQConfig {
    string id;
    std::unique_ptr<RGWRESTConn> conn;

    void init(CephContext *cct, const map<string, string, ltstr_nocase>& config) {
        string mq_endpoint = rgw_conf_get(config, "endpoint", "");
        id = string("mq:") + mq_endpoint;
        conn.reset(new RGWRESTConn(cct, nullptr, id, {mq_endpoint}));
    }
};

using MQConfigRef = std::shared_ptr<MQConfig>;

struct obj_message {
    RGWBucketInfo bucket_info;
    rgw_obj_key key;

    obj_message(const RGWBucketInfo& _bucket_info,const rgw_obj_key& _key)
        :bucket_info(_bucket_info), key(_key) {}
    void dump(Formatter *f) const {
        encode_json("bucket", bucket_info.bucket.name, f);
        encode_json("name", key.name, f);
    }
};

class RGWMQStatRemoteObjCBCR : public RGWStatRemoteObjCBCR {
    MQConfigRef conf;
    public:
    RGWMQStatRemoteObjCBCR(RGWDataSyncEnv *_sync_env,
            RGWBucketInfo& _bucket_info, rgw_obj_key& _key, MQConfigRef _conf)
        : RGWStatRemoteObjCBCR(_sync_env, _bucket_info, _key), conf(_conf) {}
    int operate() override {
        reenter(this) {
            ldout(sync_env->cct, 0) << "SYNC_MQ: stat of remote obj: z=" << sync_env->source_zone
                << " b=" << bucket_info.bucket << " k=" << key << " size=" << size << " mtime=" << mtime
                << " attrs=" << attrs << dendl;
            yield {
                obj_message doc(bucket_info, key);
                call(new RGWPutRESTResourceCR<obj_message, int>(sync_env->cct, conf->conn.get(),
                            sync_env->http_manager,
                            nullptr, nullptr,
                            doc, nullptr));
            }
            if (retcode < 0) {
                return set_cr_error(retcode);
            }
            return set_cr_done();
        }
        return 0;
    }
};

class RGWMQStatRemoteObjCR : public RGWCallStatRemoteObjCR {
    MQConfigRef conf;
    public:
    RGWMQStatRemoteObjCR(RGWDataSyncEnv *_sync_env,
            RGWBucketInfo& _bucket_info, rgw_obj_key& _key, MQConfigRef _conf)
        : RGWCallStatRemoteObjCR(_sync_env, _bucket_info, _key), conf(_conf) {}

    ~RGWMQStatRemoteObjCR() override {}

    RGWStatRemoteObjCBCR *allocate_callback() override {
        return new RGWMQStatRemoteObjCBCR(sync_env, bucket_info, key, conf);
    }
};

class RGWMQDataSyncModule : public RGWDataSyncModule {
    MQConfigRef conf;
    public:
    RGWMQDataSyncModule(CephContext *cct, const map<string, string, ltstr_nocase>& config) : conf(std::make_shared<MQConfig>()) {
        conf->init(cct, config);
    }

    RGWCoroutine *sync_object(RGWDataSyncEnv *sync_env, RGWBucketInfo& bucket_info, rgw_obj_key& key, uint64_t versioned_epoch, rgw_zone_set *zones_trace) override {
        ldout(sync_env->cct, 0) << ": SYNC_MQ: sync_object: b=" << bucket_info.bucket << " k=" << key << " versioned_epoch=" << versioned_epoch << dendl;
        return new RGWMQStatRemoteObjCR(sync_env, bucket_info, key, conf);
    }

    RGWCoroutine *remove_object(RGWDataSyncEnv *sync_env, RGWBucketInfo& bucket_info, rgw_obj_key& key, real_time& mtime, bool versioned, uint64_t versioned_epoch, rgw_zone_set *zones_trace) override {
        ldout(sync_env->cct, 0) << ": SYNC_MQ: rm_object: b=" << bucket_info.bucket << " k=" << key << " mtime=" << mtime << " versioned=" << versioned << " versioned_epoch=" << versioned_epoch << dendl;
        return NULL;
    }

    RGWCoroutine *create_delete_marker(RGWDataSyncEnv *sync_env, RGWBucketInfo& bucket_info, rgw_obj_key& key, real_time& mtime,
            rgw_bucket_entry_owner& owner, bool versioned, uint64_t versioned_epoch, rgw_zone_set *zones_trace) override {
        ldout(sync_env->cct, 0) << ": SYNC_MQ: create_delete_marker: b=" << bucket_info.bucket << " k=" << key << " mtime=" << mtime
            << " versioned=" << versioned << " versioned_epoch=" << versioned_epoch << dendl;
        return NULL;
    }
};

class RGWMQSyncModuleInstance : public RGWSyncModuleInstance {
    std::unique_ptr<RGWMQDataSyncModule> data_handler;
    public:
    RGWMQSyncModuleInstance(CephContext *cct, const map<string, string, ltstr_nocase>& config) :
        data_handler(std::unique_ptr<RGWMQDataSyncModule>(new RGWMQDataSyncModule(cct, config))) {}
    RGWDataSyncModule *get_data_handler() override {
        return data_handler.get();
    }
};

int RGWMQSyncModule::create_instance(CephContext *cct, map<string, string, ltstr_nocase>& config, RGWSyncModuleInstanceRef *instance) {
    instance->reset(new RGWMQSyncModuleInstance(cct, config));
    return 0;
}
