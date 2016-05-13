#ifndef _EMULATED_AMO_H
#define _EMULATED_AMO_H

#include <ucp/core/ucp_mm.h>
#include <ucs/sys/preprocessor.h>
#include <ucs/debug/log.h>
#include <inttypes.h>

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

ucs_status_t ucp_emulate_amo32(ucp_ep_h ep, uint32_t local_flag,
                               uint32_t swap, uint32_t compare, uint32_t add,
                               uint64_t remote_addr, ucp_rkey_h rkey,
                               uint32_t *result);

static inline
ucs_status_t ucp_emulate_atomic_add32(ucp_ep_h ep, uint32_t add,
                                      uint64_t remote_addr, ucp_rkey_h rkey)
{
    uint32_t dummy;
    return ucp_emulate_amo32(ep, AMO32_ADD, 0, 0, add, remote_addr, rkey, &dummy);
}

static inline
ucs_status_t ucp_emulate_atomic_fadd32(ucp_ep_h ep, uint32_t add,
                                       uint64_t remote_addr, ucp_rkey_h rkey,
                                       uint32_t *result)
{
    return ucp_emulate_amo32(ep, AMO32_FETCH_AND_ADD, 0, 0, add, remote_addr, rkey, result);
}

static inline
ucs_status_t ucp_emulate_atomic_cswap32(ucp_ep_h ep, uint32_t compare, uint32_t swap,
                                       uint64_t remote_addr, ucp_rkey_h rkey,
                                       uint32_t *result)
{
    return ucp_emulate_amo32(ep, AMO32_CSWAP, swap, compare, 0, remote_addr, rkey, result);
}

static inline
ucs_status_t ucp_emulate_atomic_swap32(ucp_ep_h ep, uint32_t swap,
                                       uint64_t remote_addr, ucp_rkey_h rkey,
                                       uint32_t *result)
{
    return ucp_emulate_amo32(ep, AMO32_SWAP, swap, 0, 0, remote_addr, rkey, result);
}

static inline 
ucs_status_t ucp_emulate_atomic_swap64(ucp_ep_h ep, uint64_t swap, 
                                       uint64_t remote_addr, ucp_rkey_h rkey, 
                                       uint64_t *result)
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

#endif
