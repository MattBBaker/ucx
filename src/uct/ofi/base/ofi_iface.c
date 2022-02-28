#include <uct/base/uct_iface.h>
#include "ofi_iface.h"

int uct_ofi_iface_is_reachable(uct_iface_h tl_iface, const uct_device_addr_t *dev_addr, const uct_iface_addr_t *iface_addr)
{
    ucs_trace("iface reachable");
    //TODO: This is where the call to fi_getinfo() goes
    return 0;
}

UCS_CLASS_INIT_FUNC(uct_ofi_iface_t, uct_md_h tl_md, uct_worker_h worker,
                    const uct_iface_params_t *params,
                    uct_iface_ops_t *uct_ofi_iface_ops,
                    const uct_iface_config_t *tl_config
                    UCS_STATS_ARG(ucs_stats_node_t *stats_parent))
{
    struct fi_av_attr av_attr = {0};
    uct_ofi_md_t *md = ucs_derived_of(tl_md, uct_ofi_md_t);
    int status;

    ucs_trace("OFI init iface");
    
    UCS_CLASS_CALL_SUPER_INIT(uct_base_iface_t, uct_ofi_iface_ops, NULL, tl_md,
                              worker, params,
                              tl_config UCS_STATS_ARG(params->stats_root)
                              UCS_STATS_ARG(UCT_OFI_MD_NAME));
    av_attr.type = FI_AV_MAP;
    status = fi_av_open(md->dom_ctx,
                        &av_attr,
                        &self->av,
                        NULL);
    if( !status ) {
        ucs_debug("OFI iface made successfully");
        return UCS_OK;
    } else {
        ucs_error("OFI iface creation failed");
        return UCS_ERR_NO_DEVICE;
    }
}

UCS_CLASS_DEFINE_NEW_FUNC(uct_ofi_iface_t, uct_iface_t, uct_md_h, uct_worker_h,
                          const uct_iface_params_t*, uct_iface_ops_t *,
                          const uct_iface_config_t * UCS_STATS_ARG(ucs_stats_node_t *));


void uct_ofi_cleanup_base_iface(uct_ofi_iface_t *iface)
{
}

static UCS_CLASS_CLEANUP_FUNC(uct_ofi_iface_t)
{
    uct_ofi_cleanup_base_iface(self);
}

UCS_CLASS_DEFINE(uct_ofi_iface_t, uct_base_iface_t);
