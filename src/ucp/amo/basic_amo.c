/**
* Copyright (C) Mellanox Technologies Ltd. 2001-2015.  ALL RIGHTS RESERVED.
*
* See file LICENSE for terms.
*/

#include <ucp/core/ucp_mm.h>
#include <ucs/sys/preprocessor.h>
#include <ucs/debug/log.h>
#include <inttypes.h>


#define UCP_RMA_CHECK_ATOMIC(_remote_addr, _size) \
    if (ENABLE_PARAMS_CHECK && (((_remote_addr) % (_size)) != 0)) { \
        ucs_debug("Error: Atomic variable must be naturally aligned " \
                  "(got address 0x%"PRIx64", atomic size %zu)", (_remote_addr),\
                  (_size)); \
        return UCS_ERR_INVALID_PARAM; \
    }

#define UCP_AMO_WITHOUT_RESULT(_ep, _param, _remote_addr, _rkey, _uct_func, _size) \
    { \
        ucs_status_t status; \
        uct_rkey_t uct_rkey; \
        \
        UCP_RMA_CHECK_ATOMIC(_remote_addr, _size); \
        uct_rkey = UCP_RKEY_LOOKUP(_ep, _rkey, ep->amo_dst_pdi); \
        for (;;) { \
            status = _uct_func((_ep)->uct_eps[UCP_EP_OP_AMO], _param, \
                               _remote_addr, uct_rkey); \
            if (ucs_likely(status != UCS_ERR_NO_RESOURCE)) { \
                return status; \
            } \
            ucp_worker_progress((_ep)->worker); \
        } \
        return UCS_OK; \
    }

#define UCP_AMO_WITH_RESULT(_ep, _params, _remote_addr, _rkey, _result, _uct_func, _size) \
    { \
        uct_completion_t comp; \
        ucs_status_t status; \
        uct_rkey_t uct_rkey; \
        \
        UCP_RMA_CHECK_ATOMIC(_remote_addr, _size); \
        uct_rkey   = UCP_RKEY_LOOKUP(_ep, _rkey, ep->amo_dst_pdi); \
        comp.count = 2; \
        \
        for (;;) { \
            status = _uct_func((_ep)->uct_eps[UCP_EP_OP_AMO], \
                               UCS_PP_TUPLE_BREAK _params, \
                               _remote_addr, uct_rkey, \
                               _result, &comp); \
            if (ucs_likely(status == UCS_OK)) { \
                goto out; \
            } else if (status == UCS_INPROGRESS) { \
                goto out_wait; \
            } else if (status != UCS_ERR_NO_RESOURCE) { \
                return status; \
            } \
            ucp_worker_progress((_ep)->worker); \
        } \
    out_wait: \
        do { \
            ucp_worker_progress((_ep)->worker); \
        } while (comp.count != 1); \
    out: \
        return UCS_OK; \
    }

/* Code handling 32bit atomics over 64bit atomics */

enum {
    AMO32_MSB           = 1<<17,
    AMO32_LSB           = 1<<18,
    AMO32_ADD           = 1<<19,
    AMO32_FETCH_AND_ADD = 1<<20,
    AMO32_SWAP          = 1<<21,
    AMO32_CSWAP         = 1<<22,
};

#define MSB32 (0xFFFFFFFF00000000ULL)
#define LSB32 (0x00000000FFFFFFFFULL)

#define SET_OPERAND(dst, val) ((((dst)&LSB32)|(uint64_t)val)<<32)
#define GET_OPERAND(dst)      ((uint32_t)((((uint64_t)dst)&MSB32)>>32))
#define SET_SWAP(dst, val)    (((dst)&MSB32)|(uint64_t)val)
#define GET_SWAP(dst)         ((uint32_t)((dst)&LSB32))


static inline
uint64_t calc32_swap(uint32_t flag, uint64_t fetched, uint32_t add, uint32_t swap)
{
    if(AMO32_MSB & flag) {
        if (AMO32_ADD & flag || AMO32_FETCH_AND_ADD & flag)
            return (((uint64_t)(((MSB32 & fetched)>>32)+add)<<32)|(LSB32 & fetched));
        else if (AMO32_SWAP & flag || AMO32_CSWAP & flag)
            return (((0x0ULL|swap)<<32)|(LSB32 & fetched));
    } else {
        ucs_assert_always (AMO32_LSB & flag);
        if (AMO32_ADD & flag || AMO32_FETCH_AND_ADD & flag)
            return ((0x0ULL|((uint32_t)(LSB32 & fetched)+add))|(MSB32 & fetched));
        else if (AMO32_SWAP & flag || AMO32_CSWAP & flag)
            return ((0x0ULL|swap)|(MSB32 & fetched));
    }
    ucs_assert_always(0);
    return 0;
}

static inline
uint64_t calc32_comp(uint32_t flag, uint64_t fetched, uint32_t comp)
{
    if(AMO32_CSWAP & flag) {
        if(AMO32_MSB & flag) {
            return (((0x0ULL|comp)<<32)|(LSB32 & fetched));
        } else {
            ucs_assert_always(AMO32_LSB & flag);
            return ((0x0ULL|comp)|(MSB32 & fetched));
        }
    } else {
        return fetched;
    }
    ucs_assert_always(0);
    return 0;
}

static inline
uint32_t get_return_value(uint32_t flag, uint64_t fetched)
{
    return (AMO32_MSB & flag) ?
        (uint32_t)((MSB32 & fetched) >> 32):
        (uint32_t)(LSB32 & fetched);
}

static inline
uint32_t is_completed(uint32_t flag, uint64_t fetched, uint64_t compare)
{
    if (compare == fetched)
        return 1;
    else if (flag & AMO32_CSWAP) {
        if (get_return_value(flag, fetched) != get_return_value(flag, compare)) {
            /* This means that "compare" segment was changed,
             * but for CSWAP operations it is normal completion  */
            return 1;
        }
    }
    return 0;
}

/*
ucs_status_t ucp_atomic_fadd64(ucp_ep_h ep, uint64_t add, uint64_t remote_addr,
                               ucp_rkey_h rkey, uint64_t *result)
 */
ucs_status_t ucp_emulate_atomic_add32(ucp_ep_h ep, uint32_t add,
                                      uint64_t remote_addr, ucp_rkey_h rkey)
{
    uint64_t fetch, new_value, temp_result_value;
    uint32_t local_flag = AMO32_ADD;
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

        new_value = calc32_swap(local_flag, fetch, add, 0);

        status = ucp_atomic_cswap64(ep, fetch, new_value, new_remote_addr, rkey, &temp_result_value);
        if(UCS_OK != status) {
            return status;
        }
    } while (!is_completed(local_flag, temp_result_value, fetch));
    return UCS_OK;
}

ucs_status_t ucp_emulate_atomic_fadd32(ucp_ep_h ep, uint32_t add,
                                       uint64_t remote_addr, ucp_rkey_h rkey,
                                       uint32_t *result)
{
    uint64_t fetch, new_value, temp_result_value;
    uint32_t local_flag = AMO32_FETCH_AND_ADD;
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

        new_value = calc32_swap(local_flag, fetch, add, 0);

        status = ucp_atomic_cswap64(ep, fetch, new_value, new_remote_addr, rkey, &temp_result_value);
        if(UCS_OK != status) {
            return status;
        }
    } while (!is_completed(local_flag, temp_result_value, fetch));

    *result = get_return_value(local_flag, temp_result_value);

    return UCS_OK;
}

/*
ucs_status_t ucp_atomic_cswap32(ucp_ep_h ep, uint32_t compare, uint32_t swap,
                                uint64_t remote_addr, ucp_rkey_h rkey, uint32_t *result)
 */

ucs_status_t ucp_emulate_atomic_cswap32(ucp_ep_h ep, uint32_t compare, uint32_t swap,
                                       uint64_t remote_addr, ucp_rkey_h rkey,
                                       uint32_t *result)
{
    uint64_t fetch, new_value, temp_result_value, comp_value;
    uint32_t local_flag = AMO32_CSWAP;
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

        new_value = calc32_swap(local_flag, fetch, 0, swap);
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

/*
 */
ucs_status_t ucp_emulate_atomic_swap32(ucp_ep_h ep, uint32_t swap,
                                       uint64_t remote_addr, ucp_rkey_h rkey,
                                       uint32_t *result)
{
    uint64_t fetch, new_value, temp_result_value;
    uint32_t local_flag = AMO32_SWAP;
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

        new_value = calc32_swap(local_flag, fetch, 0, swap);

        status = ucp_atomic_cswap64(ep, fetch, new_value, new_remote_addr, rkey, &temp_result_value);
        if(UCS_OK != status) {
            return status;
        }
    } while (!is_completed(local_flag, temp_result_value, fetch));

    *result = get_return_value(local_flag, temp_result_value);

    return UCS_OK;
}

ucs_status_t ucp_atomic_add32(ucp_ep_h ep, uint32_t add,
                              uint64_t remote_addr, ucp_rkey_h rkey)
{
    return ucp_emulate_atomic_add32(ep, add, remote_addr, rkey);
    /*
    UCP_AMO_WITHOUT_RESULT(ep, add, remote_addr, rkey,
                           uct_ep_atomic_add32, sizeof(uint32_t));
    */
}

ucs_status_t ucp_atomic_add64(ucp_ep_h ep, uint64_t add,
                              uint64_t remote_addr, ucp_rkey_h rkey)
{
    UCP_AMO_WITHOUT_RESULT(ep, add, remote_addr, rkey,
                           uct_ep_atomic_add64, sizeof(uint64_t));
}

ucs_status_t ucp_atomic_fadd32(ucp_ep_h ep, uint32_t add, uint64_t remote_addr,
                               ucp_rkey_h rkey, uint32_t *result)
{
    return ucp_emulate_atomic_fadd32(ep, add, remote_addr, rkey, result);
    /*
    UCP_AMO_WITH_RESULT(ep, (add), remote_addr, rkey, result,
                        uct_ep_atomic_fadd32, sizeof(uint32_t));
    */
}

ucs_status_t ucp_atomic_fadd64(ucp_ep_h ep, uint64_t add, uint64_t remote_addr,
                               ucp_rkey_h rkey, uint64_t *result)
{
    UCP_AMO_WITH_RESULT(ep, (add), remote_addr, rkey, result,
                        uct_ep_atomic_fadd64, sizeof(uint64_t));
}

ucs_status_t ucp_atomic_swap32(ucp_ep_h ep, uint32_t swap, uint64_t remote_addr,
                               ucp_rkey_h rkey, uint32_t *result)
{
    return ucp_emulate_atomic_swap32(ep, swap, remote_addr, rkey, result);
    /*
    UCP_AMO_WITH_RESULT(ep, (swap), remote_addr, rkey, result,
                               uct_ep_atomic_swap32, sizeof(uint32_t));
    */
}


ucs_status_t ucp_emulate_atomic_swap64(ucp_ep_h ep, uint64_t swap, uint64_t remote_addr,
                                       ucp_rkey_h rkey, uint64_t *result)
{
    uint64_t old;
    uint64_t temp_result_value;

    ucs_status_t status;

    do {
        status = ucp_atomic_fadd64(ep, 0, remote_addr, rkey, &old);
        if(UCS_OK != status) {
            return status;
        }

        status = ucp_atomic_cswap64(ep, old, swap, remote_addr, rkey, &temp_result_value);
        if(UCS_OK != status) {
            return status;
        }
    } while(old != temp_result_value);

    *result = old;

    return UCS_OK;
}

ucs_status_t ucp_atomic_swap64(ucp_ep_h ep, uint64_t swap, uint64_t remote_addr,
                               ucp_rkey_h rkey, uint64_t *result)
{
    return ucp_emulate_atomic_swap64(ep, swap, remote_addr, rkey, result);
    /*
    UCP_AMO_WITH_RESULT(ep, (swap), remote_addr, rkey, result,
                        uct_ep_atomic_swap64, sizeof(uint64_t));
    */
    
}

ucs_status_t ucp_atomic_cswap32(ucp_ep_h ep, uint32_t compare, uint32_t swap,
                                uint64_t remote_addr, ucp_rkey_h rkey, uint32_t *result)
{
    return ucp_emulate_atomic_cswap32(ep, compare, swap, remote_addr, rkey, result);
    /*
    UCP_AMO_WITH_RESULT(ep, (compare, swap), remote_addr, rkey, result,
                        uct_ep_atomic_cswap32, sizeof(uint32_t));
    */
}

ucs_status_t ucp_atomic_cswap64(ucp_ep_h ep, uint64_t compare, uint64_t swap,
                                uint64_t remote_addr, ucp_rkey_h rkey, uint64_t *result)
{
    UCP_AMO_WITH_RESULT(ep, (compare, swap), remote_addr, rkey, result,
                        uct_ep_atomic_cswap64, sizeof(uint64_t));
}
