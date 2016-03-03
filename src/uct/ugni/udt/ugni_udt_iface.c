
/**
 * Copyright (c) UT-Battelle, LLC. 2014-2015. ALL RIGHTS RESERVED.
 * Copyright (C) Mellanox Technologies Ltd. 2001-2014.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#include <pmi.h>
#include "ucs/type/class.h"
#include "uct/base/uct_pd.h"

#include <ucs/arch/cpu.h>
#include <uct/ugni/base/ugni_iface.h>
#include "ugni_udt_iface.h"
#include "ugni_udt_ep.h"

#define UCT_UGNI_UDT_TL_NAME "ugni_udt"

static ucs_config_field_t uct_ugni_udt_iface_config_table[] = {
    {"", "ALLOC=huge,mmap,heap", NULL,
    ucs_offsetof(uct_ugni_iface_config_t, super),
    UCS_CONFIG_TYPE_TABLE(uct_iface_config_table)},

    UCT_IFACE_MPOOL_CONFIG_FIELDS("UDT", -1, 0, "udt",
                                  ucs_offsetof(uct_ugni_iface_config_t, mpool),
                                  "\nAttention: Setting this param with value != -1 is a dangerous thing\n"
                                  "and could cause deadlock or performance degradation."),

    {NULL}
};

static void uct_ugni_udt_progress(void *arg);

static UCS_F_ALWAYS_INLINE ucs_time_t uct_ugni_udt_iface_get_async_time(uct_ugni_udt_iface_t *iface){
    return iface->super.super.worker->async->last_wakeup;
}

static inline ucs_time_t uct_ugni_udt_slow_tick(){
    return ucs_time_from_msec(100);
}

typedef enum {
    UCT_UGNI_SYNC,
    UCT_UGNI_ASYNC,
} async_status_t;

static void uct_ugni_udt_queue_rx_desc(uct_ugni_udt_iface_t *iface, uct_ugni_udt_desc_t *desc)
{
    uct_ugnu_udt_queued_am_t *queue_element;

    UCT_TL_IFACE_GET_TX_DESC(&iface->super.super, &iface->free_queue,
                             queue_element, queue_element = NULL);

    ucs_assert_always(queue_element != NULL);
    queue_element->desc = desc;
    ucs_queue_push(&iface->sync_am_events, &queue_element->queue);
}

static int requires_sync(uct_ugni_udt_iface_t *iface, uint8_t am_id)
{
    return iface->super.super.am[am_id].flags & UCT_AM_CB_FLAG_SYNC;
}

static ucs_status_t attempt_am(uct_ugni_udt_iface_t *iface, uct_ugni_udt_desc_t *desc, async_status_t is_async)
{
    ucs_status_t status;
    uct_ugni_udt_header_t *header;
    void *payload, *user_desc;

    header = uct_ugni_udt_get_rheader(desc, iface);
    payload = uct_ugni_udt_get_rpayload(desc, iface);
    user_desc = uct_ugni_udt_get_user_desc(desc, iface);

    uct_iface_trace_am(&iface->super.super, UCT_AM_TRACE_TYPE_RECV,
                       header->am_id, payload, header->length, "RX: AM");

    status = uct_iface_invoke_am(&iface->super.super, header->am_id, payload,
                                 header->length, user_desc);

    return status;
}

static void uct_ugni_udt_process_wildcard_datagram(uct_ugni_udt_iface_t *iface, uct_ugni_udt_desc_t *desc, async_status_t is_async)
{
    ucs_status_t status;
    uint32_t rem_addr,
             rem_id;
    gni_post_state_t post_state;
    uct_ugni_udt_header_t *header = uct_ugni_udt_get_rheader(desc, iface);
    void *user_desc = uct_ugni_udt_get_user_desc(desc, iface);
    gni_return_t ugni_rc;

    pthread_mutex_lock(&uct_ugni_global_lock);
    ugni_rc = GNI_EpPostDataWaitById(iface->ep_any, UCT_UGNI_UDT_ANY, -1, &post_state, &rem_addr, &rem_id);
    pthread_mutex_unlock(&uct_ugni_global_lock);

    if (ucs_unlikely(GNI_RC_SUCCESS != ugni_rc)) {
        ucs_error("GNI_EpPostDataWaitById, Error status: %s %d",
                  gni_err_str[ugni_rc], ugni_rc);
        return;
    }

    ucs_assert_always(header->type == UCT_UGNI_UDT_PAYLOAD);
    if(requires_sync(iface, header->am_id) && UCT_UGNI_ASYNC == is_async){
        uct_ugni_udt_desc_t *new_desc;
        /* Allocate a new element */
        UCT_TL_IFACE_GET_TX_DESC(&iface->super.super, &iface->free_desc,
                                 new_desc, new_desc = NULL);
        /* Queue request to be processed in a syncronous context */
        uct_ugni_udt_queue_rx_desc(iface, desc);
        /* set the new desc */
        iface->desc_any = new_desc;
    } else {
        status = attempt_am(iface, desc, is_async);

        if (UCS_OK != status) {
            uct_ugni_udt_desc_t *new_desc;
            /* set iface for a later release call */
            uct_recv_desc_iface(user_desc) = &iface->super.super.super;
            /* Allocate a new element */
            UCT_TL_IFACE_GET_TX_DESC(&iface->super.super, &iface->free_desc,
                                     new_desc, new_desc = NULL);
            ucs_debug("Keeping the wildcard desc for AM release. New desc for wildcard: %p old desc: %p", new_desc, iface->desc_any);
            /* set the new desc */
            iface->desc_any = new_desc;
        }
    }
    uct_ugni_udt_ep_any_post(iface);
}

void uct_ugni_udt_process_reply_datagram(uct_ugni_udt_iface_t *iface, uint64_t id, async_status_t is_async)
{
    ucs_status_t status;
    gni_return_t ugni_rc;
    uct_ugni_udt_header_t *header;
    void *user_desc;
    uint32_t rem_addr,
        rem_id;
    gni_post_state_t post_state;
    uct_ugni_udt_ep_t *ep = ucs_derived_of(uct_ugni_iface_lookup_ep(&iface->super, id),
                                           uct_ugni_udt_ep_t);
    if (ucs_unlikely(NULL == ep)) {
        ucs_error("Can not lookup ep with id %"PRIx64,id);
        return;
    }

    ucs_assert_always(NULL != ep);

    pthread_mutex_lock(&uct_ugni_global_lock);
    ugni_rc = GNI_EpPostDataWaitById(ep->super.ep, id, -1, &post_state, &rem_addr, &rem_id);
    pthread_mutex_unlock(&uct_ugni_global_lock);

    if (ucs_unlikely(GNI_RC_SUCCESS != ugni_rc)) {
        ucs_error("GNI_EpPostDataWaitById, Error status: %s %d",
                  gni_err_str[ugni_rc], ugni_rc);
        return;
    }

    header = uct_ugni_udt_get_rheader(ep->posted_desc, iface);
    user_desc = uct_ugni_udt_get_user_desc(ep->posted_desc, iface);

    if (header->type == UCT_UGNI_UDT_PAYLOAD) {
        /* data message was received */
        if(requires_sync(iface, header->am_id) && UCT_UGNI_ASYNC == is_async){
            uct_ugni_udt_queue_rx_desc(iface, ep->posted_desc);
        } else {
            status = attempt_am(iface, ep->posted_desc, is_async);

            if (UCS_OK == status) {
                uct_ugni_udt_reset_desc(ep->posted_desc, iface);
                ucs_mpool_put(ep->posted_desc);
            } else {
                /* set iface for a later release call */
                uct_recv_desc_iface(user_desc) = &iface->super.super.super;
                ucs_debug("Keeping ep desc for later release by am. old desc: %p", ep->posted_desc);
            }
        }
    } else {
        uct_ugni_udt_reset_desc(ep->posted_desc, iface);
        ucs_mpool_put(ep->posted_desc);
    }
    /* no data, just an ack */
    --iface->super.outstanding;
    --ep->super.outstanding;
    ep->posted_desc = NULL;
}

static void _uct_ugni_udt_progress(void *arg, async_status_t is_async)
{
    uint64_t id;
    uct_ugni_udt_iface_t * iface = (uct_ugni_udt_iface_t *)arg;

    gni_return_t ugni_rc;

    pthread_mutex_lock(&uct_ugni_global_lock);
    ugni_rc = GNI_PostDataProbeById(iface->super.nic_handle, &id);
    pthread_mutex_unlock(&uct_ugni_global_lock);
    if (ucs_unlikely(GNI_RC_SUCCESS != ugni_rc)) {
        if (GNI_RC_NO_MATCH != ugni_rc) {
            ucs_error("GNI_PostDataProbeById , Error status: %s %d",
                      gni_err_str[ugni_rc], ugni_rc);
        }
        return;
    }

    if (UCT_UGNI_UDT_ANY == id) {
        uct_ugni_udt_process_wildcard_datagram(iface, iface->desc_any, is_async);
    } else {
        uct_ugni_udt_process_reply_datagram(iface, id, is_async);        
    }
}    

static void uct_ugni_udt_dispatch_rx_queue(uct_ugni_udt_iface_t *iface)
{
    uct_ugnu_udt_queued_am_t *queued_am = ucs_queue_pull_elem_non_empty(&iface->sync_am_events, uct_ugnu_udt_queued_am_t, queue);
    uct_ugni_udt_desc_t *desc = queued_am->desc;
    ucs_mpool_put(queued_am);
    ucs_status_t status = attempt_am(iface, desc, UCT_UGNI_SYNC);
    if(UCS_OK == status) {
        uct_ugni_udt_reset_desc(desc, iface);
        ucs_mpool_put(desc);
    } else {
        ucs_debug("Keeping desc for AM from sync dispatcher. desc: %p", desc);
        void *user_desc = uct_ugni_udt_get_user_desc(desc, iface);
        uct_recv_desc_iface(user_desc) = &iface->super.super.super;
    }
}

static void uct_ugni_udt_progress(void *arg)
{
    uct_ugni_udt_iface_t * iface = (uct_ugni_udt_iface_t *)arg;

    UCS_ASYNC_BLOCK(iface->super.super.worker->async);

    while(!ucs_queue_is_empty(&iface->sync_am_events)) {
        uct_ugni_udt_dispatch_rx_queue(iface);
    }

    _uct_ugni_udt_progress(arg, UCT_UGNI_SYNC);

    /* have a go a processing the pending queue */
    ucs_arbiter_dispatch(&iface->super.arbiter, 1, uct_ugni_ep_process_pending, NULL);

    UCS_ASYNC_UNBLOCK(iface->super.super.worker->async);
}

static void uct_ugni_udt_iface_release_am_desc(uct_iface_t *tl_iface, void *desc)
{
    uct_ugni_udt_desc_t *ugni_desc;
    uct_ugni_udt_iface_t *iface = ucs_derived_of(tl_iface, uct_ugni_udt_iface_t);

    ucs_debug("Called uct_ugni_udt_iface_release_am_desc");
    ugni_desc = (uct_ugni_udt_desc_t *)((uct_am_recv_desc_t *)desc - 1);
    ucs_assert_always(NULL != ugni_desc);
    uct_ugni_udt_reset_desc(ugni_desc, iface);
    ucs_mpool_put(ugni_desc);
}

static ucs_status_t uct_ugni_udt_query_tl_resources(uct_pd_h pd,
                                                    uct_tl_resource_desc_t **resource_p,
                                                    unsigned *num_resources_p)
{
    return uct_ugni_query_tl_resources(pd, UCT_UGNI_UDT_TL_NAME,
                                       resource_p, num_resources_p);
}

static ucs_status_t uct_ugni_udt_iface_query(uct_iface_h tl_iface, uct_iface_attr_t *iface_attr)
{
    uct_ugni_udt_iface_t *iface = ucs_derived_of(tl_iface, uct_ugni_udt_iface_t);

    memset(iface_attr, 0, sizeof(uct_iface_attr_t));
    iface_attr->cap.am.max_short       = iface->config.udt_seg_size -
                                         sizeof(uct_ugni_udt_header_t);
    iface_attr->cap.am.max_bcopy       = iface->config.udt_seg_size -
                                         sizeof(uct_ugni_udt_header_t);
    iface_attr->iface_addr_len         = sizeof(uct_sockaddr_ugni_t);
    iface_attr->ep_addr_len            = 0;
    iface_attr->cap.flags              = UCT_IFACE_FLAG_AM_SHORT |
                                         UCT_IFACE_FLAG_AM_BCOPY |
                                         UCT_IFACE_FLAG_CONNECT_TO_IFACE |
                                         UCT_IFACE_FLAG_PENDING |
                                         UCT_IFACE_FLAG_AM_CB_SYNC |
                                         UCT_IFACE_FLAG_AM_CB_ASYNC;

    iface_attr->overhead               = 1e-6;  /* 1 usec */
    iface_attr->latency                = 40e-6; /* 40 usec */
    iface_attr->bandwidth              = pow(1024, 2); /* bytes */
    return UCS_OK;
}

static void uct_ugni_udt_iface_timer(void *arg){
    uct_ugni_udt_iface_t *iface = (uct_ugni_udt_iface_t *)arg;
    ucs_time_t now;

    UCS_ASYNC_BLOCK(iface->super.super.worker->async);
    now = uct_ugni_udt_iface_get_async_time(iface);
    ucs_trace_async("iface(%p) slow_timer_sweep: now %llu", iface, now);
    ucs_twheel_sweep(&iface->async.slow_timer, now);
    _uct_ugni_udt_progress((void *)iface, UCT_UGNI_ASYNC);
    ucs_arbiter_dispatch(&iface->super.arbiter, 1, uct_ugni_ep_process_pending, NULL);
    UCS_ASYNC_UNBLOCK(iface->super.super.worker->async);
}

static UCS_CLASS_CLEANUP_FUNC(uct_ugni_udt_iface_t)
{
    gni_return_t ugni_rc;
    ucs_status_t status;

    uct_worker_progress_unregister(self->super.super.worker,
                                   uct_ugni_udt_progress, self);
    if (!self->super.activated) {
        /* We done with release */
        return;
    }

    UCS_ASYNC_BLOCK(self->super.super.worker->async);
    status = ucs_async_remove_timer(self->async.timer_id);
    ucs_assert_always(UCS_OK == status);
    ucs_twheel_cleanup(&self->async.slow_timer);
    ugni_rc = GNI_EpPostDataCancel(self->ep_any);
    if (GNI_RC_SUCCESS != ugni_rc) {
        ucs_debug("GNI_EpPostDataCancel failed, Error status: %s %d",
                  gni_err_str[ugni_rc], ugni_rc);
        //return;
    }
    ucs_mpool_put(self->desc_any);
    while(!ucs_queue_is_empty(&self->sync_am_events)) {
      uct_ugni_udt_dispatch_rx_queue(self);
    }
    ucs_mpool_cleanup(&self->free_desc, 1);
    ucs_mpool_cleanup(&self->free_queue, 1);
    UCS_ASYNC_UNBLOCK(self->super.super.worker->async);
}

static UCS_CLASS_DEFINE_DELETE_FUNC(uct_ugni_udt_iface_t, uct_iface_t);

uct_iface_ops_t uct_ugni_udt_iface_ops = {
    .iface_query           = uct_ugni_udt_iface_query,
    .iface_flush           = uct_ugni_iface_flush,
    .iface_close           = UCS_CLASS_DELETE_FUNC_NAME(uct_ugni_udt_iface_t),
    .iface_get_address     = uct_ugni_iface_get_address,
    .iface_get_device_address = (void*)ucs_empty_function_return_success,
    .iface_is_reachable    = uct_ugni_iface_is_reachable,
    .iface_release_am_desc = uct_ugni_udt_iface_release_am_desc,
    .ep_create_connected   = UCS_CLASS_NEW_FUNC_NAME(uct_ugni_udt_ep_t),
    .ep_destroy            = UCS_CLASS_DELETE_FUNC_NAME(uct_ugni_udt_ep_t),
    .ep_pending_add        = uct_ugni_ep_pending_add,
    .ep_pending_purge      = uct_ugni_ep_pending_purge,
    .ep_am_short           = uct_ugni_udt_ep_am_short,
    .ep_am_bcopy           = uct_ugni_udt_ep_am_bcopy,
    .ep_flush              = uct_ugni_ep_flush,
};

static ucs_mpool_ops_t uct_ugni_udt_desc_mpool_ops = {
    .chunk_alloc   = ucs_mpool_hugetlb_malloc,
    .chunk_release = ucs_mpool_hugetlb_free,
    .obj_init      = NULL,
    .obj_cleanup   = NULL
};

static UCS_CLASS_INIT_FUNC(uct_ugni_udt_iface_t, uct_pd_h pd, uct_worker_h worker,
                           const char *dev_name, size_t rx_headroom,
                           const uct_iface_config_t *tl_config)
{
    uct_ugni_iface_config_t *config = ucs_derived_of(tl_config, uct_ugni_iface_config_t);
    ucs_status_t status;
    uct_ugni_udt_desc_t *desc;
    gni_return_t ugni_rc;

    pthread_mutex_lock(&uct_ugni_global_lock);

    UCS_CLASS_CALL_SUPER_INIT(uct_ugni_iface_t, pd, worker, dev_name, &uct_ugni_udt_iface_ops,
                              &config->super UCS_STATS_ARG(NULL));

    /* Setting initial configuration */
    self->config.udt_seg_size = GNI_DATAGRAM_MAXSIZE;
    self->config.rx_headroom  = rx_headroom;

    ucs_queue_head_init(&self->sync_am_events);

    status = ucs_mpool_init(&self->free_desc,
                            0,
                            uct_ugni_udt_get_diff(self) + self->config.udt_seg_size * 2,
                            uct_ugni_udt_get_diff(self),
                            UCS_SYS_CACHE_LINE_SIZE,      /* alignment */
                            128,                          /* grow */
                            config->mpool.max_bufs,       /* max buffers */
                            &uct_ugni_udt_desc_mpool_ops,
                            "UGNI-UDT-DESC");

    if (UCS_OK != status) {
        ucs_error("Mpool creation failed");
        goto exit;
    }
    //TODO review error chain
    status = ucs_mpool_init(&self->free_queue,
                            0,
                            sizeof(uct_ugnu_udt_queued_am_t),
                            0,
                            UCS_SYS_CACHE_LINE_SIZE,      /* alignment */
                            128,                          /* grow */
                            config->mpool.max_bufs,       /* max buffers */
                            &uct_ugni_udt_desc_mpool_ops,
                            "UGNI-UDT-DESC-QUEUE");

    if (UCS_OK != status) {
        ucs_error("Mpool creation failed");
        goto clean_desc;
    }

    status = ugni_activate_iface(&self->super);
    if (UCS_OK != status) {
        ucs_error("Failed to activate the interface");
        goto clean_queue;
    }

    ugni_rc = GNI_EpCreate(self->super.nic_handle, NULL, &self->ep_any);
    if (GNI_RC_SUCCESS != ugni_rc) {
        ucs_error("GNI_CdmCreate failed, Error status: %s %d",
                  gni_err_str[ugni_rc], ugni_rc);
        status = UCS_ERR_NO_DEVICE;
        goto clean_iface;
    }


    UCT_TL_IFACE_GET_TX_DESC(&self->super.super, &self->free_desc,
                             desc, goto clean_ep);

    /* Init any desc */
    self->desc_any = desc;
    status = uct_ugni_udt_ep_any_post(self);

    pthread_mutex_unlock(&uct_ugni_global_lock);

    if (UCS_OK != status) {
        /* We can't continue if we can't post the first receive */
        ucs_error("Failed to post wildcard request");
        goto clean_ep;
    }

    status = ucs_twheel_init(&self->async.slow_timer, uct_ugni_udt_slow_tick() / 4,
                             uct_ugni_udt_iface_get_async_time(self));
    if (UCS_OK != status) {
        goto clean_ep;
    }

    status = ucs_async_add_timer(self->super.super.worker->async->mode,
                                 uct_ugni_udt_slow_tick(),
                                 uct_ugni_udt_iface_timer, self,
                                 self->super.super.worker->async,
                                 &self->async.timer_id);


    /* TBD: eventually the uct_ugni_progress has to be moved to
     * udt layer so each ugni layer will have own progress */
    uct_worker_progress_register(worker, uct_ugni_udt_progress, self);

    return UCS_OK;

clean_ep:
    pthread_mutex_lock(&uct_ugni_global_lock);
    ugni_rc = GNI_EpDestroy(self->ep_any);
    if (GNI_RC_SUCCESS != ugni_rc) {
        ucs_warn("GNI_EpDestroy failed, Error status: %s %d",
                  gni_err_str[ugni_rc], ugni_rc);
    }
clean_iface:
    ugni_deactivate_iface(&self->super);
clean_queue:
    ucs_mpool_cleanup(&self->free_queue, 1);
clean_desc:
    ucs_mpool_cleanup(&self->free_desc, 1);
exit:
    ucs_error("Failed to activate interface");
    pthread_mutex_unlock(&uct_ugni_global_lock);
    return status;
}

UCS_CLASS_DEFINE(uct_ugni_udt_iface_t, uct_ugni_iface_t);
UCS_CLASS_DEFINE_NEW_FUNC(uct_ugni_udt_iface_t, uct_iface_t,
                          uct_pd_h, uct_worker_h,
                          const char*, size_t, const uct_iface_config_t *);

UCT_TL_COMPONENT_DEFINE(uct_ugni_udt_tl_component,
                        uct_ugni_udt_query_tl_resources,
                        uct_ugni_udt_iface_t,
                        UCT_UGNI_UDT_TL_NAME,
                        "UGNI_UDT",
                        uct_ugni_udt_iface_config_table,
                        uct_ugni_iface_config_t);

UCT_PD_REGISTER_TL(&uct_ugni_pd_component, &uct_ugni_udt_tl_component);
