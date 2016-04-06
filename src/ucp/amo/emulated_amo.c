#include "emulated_amo.h"

ucs_status_t ucp_emulate_amo32(ucp_ep_h ep, uint32_t local_flag, 
                                             uint32_t swap, uint32_t compare, uint32_t add, 
                                             uint64_t remote_addr, ucp_rkey_h rkey, 
                                             uint32_t *result)
{
    uint64_t fetch, new_value, temp_result_value, comp_value;
    ucs_status_t status;
    uintptr_t new_remote_addr;

    if(!ucs_check_if_align_pow2(remote_addr, 8)) {
        local_flag |= AMO32_LSB;
        new_remote_addr = (uintptr_t)remote_addr;
    } else {
        local_flag |= AMO32_MSB;
        new_remote_addr = (uintptr_t)remote_addr - 0x4;
    }

    do {
        status = ucp_atomic_fadd64(ep, 0, new_remote_addr, rkey, &fetch);
        if(UCS_OK != status) {
            return status;
        }

        new_value = calc32_swap(local_flag, fetch, add, swap);
        comp_value = calc32_comp(local_flag, fetch, compare);

        status = ucp_atomic_cswap64(ep, comp_value, new_value, 
                                    new_remote_addr, rkey, 
                                    &temp_result_value);
        if(UCS_OK != status) {
            return status;
        }
    } while (!is_completed(local_flag, temp_result_value, comp_value));

    *result = get_return_value(local_flag, temp_result_value);

    return UCS_OK;
}
