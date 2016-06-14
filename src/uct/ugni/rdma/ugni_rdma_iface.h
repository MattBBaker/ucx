/**
 * Copyright (c) UT-Battelle, LLC. 2014-2015. ALL RIGHTS RESERVED.
 * Copyright (C) Mellanox Technologies Ltd. 2001-2014.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#ifndef UCT_UGNI_IFACE_H
#define UCT_UGNI_IFACE_H

#include <uct/ugni/base/ugni_pd.h>
#include <uct/ugni/base/ugni_device.h>
#include <uct/ugni/base/ugni_iface.h>
#include "ugni_rdma_ep.h"

#include <uct/base/uct_iface.h>

#define UCT_UGNI_MAX_FMA     (65536)
#define UCT_UGNI_MAX_RDMA    (512*1024*1024);

//void uct_ugni_rdma_progress(void *arg);

struct uct_ugni_iface;

typedef struct uct_ugni_rdma_iface {
    uct_ugni_iface_t        super;                       /**< Super type */
    ucs_mpool_t             free_desc;                   /**< Pool of FMA descriptors for
                                                              requests without bouncing buffers */
    ucs_mpool_t             free_desc_get;               /**< Pool of FMA descriptors for
                                                              unaligned get requests without
                                                              bouncing buffers */
    ucs_mpool_t             free_desc_buffer;            /**< Pool of FMA descriptors for
                                                              requests with bouncing buffer*/
    ucs_mpool_t             free_desc_famo;              /**< Pool of FMA descriptors for
                                                              64/32 bit fetched-atomic operations
                                                              (registered memory) */
    ucs_mpool_t             free_desc_get_buffer;        /**< Pool of FMA descriptors for
                                                              FMA_SIZE fetch operations
                                                              (registered memory) */
    struct {
        unsigned            fma_seg_size;                /**< FMA Segment size */
        unsigned            rdma_max_size;               /**< Max RDMA size */
    } config;
} uct_ugni_rdma_iface_t;

typedef struct uct_ugni_rdma_iface_config {
    uct_iface_config_t       super;
    uct_iface_mpool_config_t mpool;
} uct_ugni_rdma_iface_config_t;

typedef struct uct_ugni_rdma_fetch_desc {
    uct_ugni_base_desc_t super;
    uct_completion_t tmp;
    uct_completion_t *orig_comp_cb;
    size_t padding;

    /* Handling unalined composed get messages */
    size_t expected_bytes;          /**< Number of bytes expected to be delivered
                                         including padding */
    size_t network_completed_bytes; /**< Total number of delivered bytes */
    struct uct_ugni_rdma_fetch_desc* head; /**< Pointer to the head descriptor
                                         that manages the completion of the operation */
    void *user_buffer;              /**< Pointer to user's buffer, here to ensure it's always available for composed messages */
    size_t tail;                    /**< Tail parameter to specify how many bytes at the end of a fma/rdma are garbage*/
} uct_ugni_rdma_fetch_desc_t;

#if 1
static inline ucs_status_t uct_ugni_rdma_progress_events(void *arg)
{
    gni_cq_entry_t  event_data = 0;
    gni_post_descriptor_t *event_post_desc_ptr;
    uct_ugni_base_desc_t *desc;
    gni_return_t ugni_rc;
    uct_ugni_iface_t *iface = ucs_derived_of(arg, uct_ugni_iface_t);

    ugni_rc = GNI_CqGetEvent(iface->local_cq, &event_data);
    if (ucs_likely(GNI_RC_NOT_DONE == ugni_rc)) {
        return UCS_OK;
    }

    if (ucs_unlikely((GNI_RC_SUCCESS != ugni_rc && !event_data) || GNI_CQ_OVERRUN(event_data))) {
        ucs_error("GNI_CqGetEvent falied. Error status %s %d ",
                  gni_err_str[ugni_rc], ugni_rc);
        return UCS_ERR_NO_RESOURCE;
    }

    ugni_rc = GNI_GetCompleted(iface->local_cq, event_data, &event_post_desc_ptr);
    if (ucs_unlikely(GNI_RC_SUCCESS != ugni_rc && GNI_RC_TRANSACTION_ERROR != ugni_rc)) {
        ucs_error("GNI_GetCompleted falied. Error status %s %d %d",
                  gni_err_str[ugni_rc], ugni_rc, GNI_RC_TRANSACTION_ERROR);
        return UCS_ERR_NO_RESOURCE;
    }

    desc = (uct_ugni_base_desc_t *)event_post_desc_ptr;
    ucs_trace_async("Completion received on %p", desc);

    if (NULL != desc->comp_cb) {
        uct_invoke_completion(desc->comp_cb, UCS_OK);
    }
    --iface->outstanding;
    --desc->ep->outstanding;

    if (ucs_likely(0 == desc->not_ready_to_free)) {
        ucs_mpool_put(desc);
    }

    return UCS_INPROGRESS;
}

static inline void uct_ugni_rdma_progress(void *arg)
{
    uct_ugni_iface_t *iface = (uct_ugni_iface_t *)arg;
    ucs_status_t status;

    do {
        status = uct_ugni_rdma_progress_events(arg);
    } while (UCS_INPROGRESS == status);

    /* have a go a processing the pending queue */
    ucs_arbiter_dispatch(&iface->arbiter, 1, uct_ugni_ep_process_pending, NULL);

}
#endif
#endif
