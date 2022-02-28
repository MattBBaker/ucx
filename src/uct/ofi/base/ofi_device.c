#include <ucs/sys/sys.h>
#include <ucs/sys/string.h>
#include <ucs/debug/log.h>

#include "ofi_device.h"

/* TODO: more flexible in capabilities */
ucs_status_t uct_ofi_init_fabric(uct_ofi_md_t *md, char *domain_name)
{
    struct fi_info *hints;
    int ret = 1;

    ucs_trace("Init fabric");

    hints = fi_allocinfo();
    /* TODO: Maybe FI_FENCE? */
    hints->caps = FI_RMA | FI_ATOMIC | FI_TAGGED;
    hints->addr_format = FI_FORMAT_UNSPEC;

    ret = fi_getinfo(fi_version(), NULL, NULL, 0, hints, &md->fab_info);
    if( ret != 0 || !md->fab_info) {
        ucs_error("No device was found");
	goto out;
    }

    /* TODO: Make this work so fabrics can be selected by name */
    if( domain_name ) {
        ucs_error("Selecting dev by name not yet supported");
	ret = 1;
        goto out;
    }

    /* TODO: version missmatch check here */
    /* Third param is a context for async ops. Could be useful */
    /* TODO: is fab_info needed after this? */
    ret = fi_fabric(md->fab_info->fabric_attr, &md->fab_ctx, NULL);
    if( ret != 0 ) {
        ucs_error("No fabric was found");
	goto out;
    }

    /* this should be an iface */
    /* or maybe not? */
    ret = fi_domain(md->fab_ctx, md->fab_info, &md->dom_ctx, NULL);
    if( ret != 0 ) {
        ucs_error("Failed to creat domain");
    }

out:
   fi_freeinfo(hints);
   if (ret != 0) {
       return UCS_ERR_NO_DEVICE;
   } else {
       ucs_debug("Init successful. Using fabric named: %s", md->fab_info->fabric_attr->name);
       return UCS_OK;
   }
}
