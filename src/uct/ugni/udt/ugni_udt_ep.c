/**
* Copyright (C) UT-Battelle, LLC. 2015. ALL RIGHTS RESERVED.
* Copyright (C) Mellanox Technologies Ltd. 2001-2014.  ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
#include <ucs/datastruct/sglib_wrapper.h>
#include <ucs/debug/memtrack.h>
#include <ucs/debug/log.h>
#include <uct/base/uct_log.h>

#include "ugni_udt_ep.h"
#include "ugni_udt_iface.h"
#include <uct/ugni/base/ugni_device.h>
#include <uct/ugni/base/ugni_ep.h>

static UCS_CLASS_INIT_FUNC(uct_ugni_udt_ep_t, uct_iface_t *tl_iface,
                           const uct_device_addr_t *dev_addr,
                           const uct_iface_addr_t *iface_addr)
{
    UCS_CLASS_CALL_SUPER_INIT(uct_ugni_ep_t, tl_iface, dev_addr, iface_addr);
    self->posted_desc = NULL;
    return UCS_OK;
}

typedef enum {
    UCT_UGNI_SYNC,
    UCT_UGNI_ASYNC,
} async_status_t;

void uct_ugni_udt_process_reply_datagram(uct_ugni_udt_iface_t *iface, uint64_t id, async_status_t is_async);

static UCS_CLASS_CLEANUP_FUNC(uct_ugni_udt_ep_t)
{
    gni_return_t ugni_rc;
    uint32_t rem_addr,
        rem_id;
    gni_post_state_t post_state;
    uct_ugni_udt_iface_t *iface = ucs_derived_of(self->super.super.super.iface, uct_ugni_udt_iface_t);
    UCS_ASYNC_BLOCK(iface->super.super.worker->async);
    if (self->posted_desc) {
        ugni_rc = GNI_EpPostDataCancel(self->super.ep);
        if (GNI_RC_SUCCESS != ugni_rc) {
            ucs_error("GNI_EpPostDataWaitById, Error status: %s %d",
                      gni_err_str[ugni_rc], ugni_rc);
        }

        ugni_rc = GNI_EpPostDataWaitById(self->super.ep, self->super.hash_key, -1, &post_state, &rem_addr, &rem_id);

        if (ucs_unlikely(GNI_RC_SUCCESS != ugni_rc)) {
            ucs_error("GNI_EpPostDataWaitById, Error status: %s %d",
                      gni_err_str[ugni_rc], ugni_rc);
            return;
        }

        ucs_assert_always(GNI_POST_TERMINATED == post_state);

        iface->super.outstanding--;
        self->super.outstanding--;
        ucs_mpool_put(self->posted_desc);
    }
    UCS_ASYNC_UNBLOCK(iface->super.super.worker->async);
}

UCS_CLASS_DEFINE(uct_ugni_udt_ep_t, uct_ugni_ep_t);
UCS_CLASS_DEFINE_NEW_FUNC(uct_ugni_udt_ep_t, uct_ep_t, uct_iface_t*,
                          const uct_device_addr_t *, const uct_iface_addr_t*);
UCS_CLASS_DEFINE_DELETE_FUNC(uct_ugni_udt_ep_t, uct_ep_t);

enum {
    UCT_UGNI_UDT_AM_BCOPY,
    UCT_UGNI_UDT_AM_SHORT,
};

/* A common mm active message sending function.
 * The first parameter indicates the origin of the call.
 * 1 - perform am short sending
 * 0 - perform am bcopy sending
 */

uint64_t seq_id = 0;

static UCS_F_ALWAYS_INLINE ssize_t
uct_ugni_udt_ep_am_common_send(const unsigned is_short, uct_ugni_udt_ep_t *ep, uct_ugni_udt_iface_t *iface,
                               uint8_t am_id, unsigned length, uint64_t header,
                               const void *payload, uct_pack_callback_t pack_cb, void *arg)
{
    gni_return_t ugni_rc;
    uint16_t msg_length;
    uct_ugni_udt_desc_t *desc;
    uct_ugni_udt_header_t *sheader,
                          *rheader;
    ssize_t packed_length;

    UCT_CHECK_AM_ID(am_id);
    if (ucs_unlikely(NULL != ep->posted_desc)) {
        UCT_TL_IFACE_STAT_TX_NO_RES(&iface->super.super);
        return UCS_ERR_NO_RESOURCE;
    }

    UCT_TL_IFACE_GET_TX_DESC(&iface->super.super, &iface->free_desc,
                             desc, return UCS_ERR_NO_RESOURCE);
    ucs_debug("Got desc %p for common send.", desc);

    rheader = uct_ugni_udt_get_rheader(desc, iface);
    rheader->type = UCT_UGNI_UDT_EMPTY;

    sheader = uct_ugni_udt_get_sheader(desc, iface);

    if (is_short) {
        uint64_t *hdr = (uint64_t *)uct_ugni_udt_get_spayload(desc, iface);
        *hdr = header;
        memcpy((void*)(hdr + 1), payload, length);

        sheader->length = length + sizeof(header);
        msg_length = sheader->length + sizeof(*sheader);
        UCT_TL_EP_STAT_OP(ucs_derived_of(ep, uct_base_ep_t), AM, SHORT, sizeof(header) + length);
    } else {
        packed_length = pack_cb((void *)uct_ugni_udt_get_spayload(desc, iface),
                                arg);
        sheader->length = packed_length;
        msg_length = sheader->length + sizeof(*sheader);
        UCT_TL_EP_STAT_OP(ucs_derived_of(ep, uct_base_ep_t), AM, BCOPY, packed_length);
    }

    uct_iface_trace_am(&iface->super.super, UCT_AM_TRACE_TYPE_SEND, am_id,
                       uct_ugni_udt_get_spayload(desc, iface), length,
                       is_short ? "TX: AM_SHORT" : "TX: AM_BCOPY");

    sheader->am_id = am_id;
    sheader->type = UCT_UGNI_UDT_PAYLOAD;

    ucs_assert_always(sheader->length <= GNI_DATAGRAM_MAXSIZE);

    pthread_mutex_lock(&uct_ugni_global_lock);
    ugni_rc = GNI_EpPostDataWId(ep->super.ep,
                                sheader, msg_length,
                                rheader, (uint16_t)iface->config.udt_seg_size,
                                ep->super.hash_key);
    pthread_mutex_unlock(&uct_ugni_global_lock);

    ucs_assert_always(ugni_rc != GNI_RC_INVALID_PARAM);
    UCT_UGNI_UDT_CHECK_RC(ugni_rc, desc);

    ep->posted_desc = desc;
    ++ep->super.outstanding;
    ++iface->super.outstanding;

    return is_short ? UCS_OK : packed_length;
}

ucs_status_t uct_ugni_udt_ep_am_short(uct_ep_h tl_ep, uint8_t id, uint64_t header,
                                const void *payload, unsigned length)
{
    uct_ugni_udt_iface_t *iface = ucs_derived_of(tl_ep->iface, uct_ugni_udt_iface_t);
    uct_ugni_udt_ep_t *ep = ucs_derived_of(tl_ep, uct_ugni_udt_ep_t);

    UCS_ASYNC_BLOCK(iface->super.super.worker->async);

    UCT_CHECK_LENGTH(length, iface->config.udt_seg_size - sizeof(header) -
                     sizeof(uct_ugni_udt_header_t), "am_short");
    ucs_trace_data("AM_SHORT [%p] am_id: %d buf=%p length=%u",
                   iface, id, payload, length);
    ucs_status_t status = uct_ugni_udt_ep_am_common_send(UCT_UGNI_UDT_AM_SHORT, ep, iface, id, length,
                                                         header, payload, NULL, NULL);

    UCS_ASYNC_UNBLOCK(iface->super.super.worker->async);

    return status;
}

ssize_t uct_ugni_udt_ep_am_bcopy(uct_ep_h tl_ep, uint8_t id,
                                 uct_pack_callback_t pack_cb,
                                 void *arg)
{
    uct_ugni_udt_iface_t *iface = ucs_derived_of(tl_ep->iface, uct_ugni_udt_iface_t);
    uct_ugni_udt_ep_t *ep = ucs_derived_of(tl_ep, uct_ugni_udt_ep_t);

    UCS_ASYNC_BLOCK(iface->super.super.worker->async);

    ucs_trace_data("AM_BCOPY [%p] am_id: %d buf=%p",
                   iface, id, arg );
    ucs_status_t status = uct_ugni_udt_ep_am_common_send(UCT_UGNI_UDT_AM_BCOPY, ep, iface, id, 0,
                                          0, NULL, pack_cb, arg);
    UCS_ASYNC_UNBLOCK(iface->super.super.worker->async);

    return status;
}
