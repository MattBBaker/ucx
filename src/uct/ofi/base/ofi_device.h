#ifndef UCT_OFI_DEV_H
#define UCT_OFI_DEV_H
#include <ucs/type/status.h>
#include <rdma/fabric.h>
#include <rdma/fi_domain.h>
#include "ofi_types.h"

ucs_status_t uct_ofi_init_fabric(uct_ofi_md_t*, char*);

#endif
