#ifndef UCT_UGNI_TYPES_H
#define UCT_UGNI_TYPES_H

#include <rdma/fabric.h>
#include <rdma/fi_domain.h>
#include <uct/api/uct.h>
#include <uct/base/uct_md.h>
#include <uct/base/uct_iface.h>
#include <ucs/datastruct/arbiter.h>

typedef struct uct_ofi_md {
    uct_md_t super;         /**< Domain info */
    int ref_count;
    struct fi_info *fab_info;
    struct fid_fabric *fab_ctx;
    struct fid_domain *dom_ctx;
} uct_ofi_md_t;

typedef struct uct_ofi_iface {
    uct_base_iface_t        super;
    unsigned                outstanding;                 /**< Counter for outstanding packets */
    ucs_arbiter_t           arbiter;                     /**< arbiter structure for pending operations */
    struct fid_av            *av;                          /**< libfabric address vector */
} uct_ofi_iface_t;

typedef struct uct_ofi_iface_config {
    uct_iface_config_t       super;
    uct_iface_mpool_config_t mpool;
} uct_ofi_iface_config_t;

#endif
