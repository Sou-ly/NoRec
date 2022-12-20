/**
 * @file   tm.c
 * @author [...]
 *
 * @section LICENSE
 *
 * [...]
 *
 * @section DESCRIPTION
 *
 * Implementation of your own transaction manager.
 * You can completely rewrite this file (and create more files) as you wish.
 * Only the interface (i.e. exported symbols and semantic) must be preserved.
 **/

// Requested features
#define _GNU_SOURCE
#define _POSIX_C_SOURCE 200809L
#ifdef __STDC_NO_ATOMICS__
    #error Current C11 compiler does not support atomic operations
#endif

// External headers
#include <iostream>
#include <atomic>
#include <vector>
#include <unordered_map>

// Internal headers
#include <tm.hpp>
#include "macros.h"

#define MAX_SEGMENTS 65536 // 2^16

typedef uint64_t word_t;

struct region
{
    region(size_t size, size_t align)
        : size(size), align(align), segments(MAX_SEGMENTS) {}
    std::atomic_uint64_t global_lock;
    size_t size;
    size_t align;
    std::atomic_uint64_t next_ptr = 2;
    std::vector<std::vector<word_t>> segments;
};

inline word_t tm_read_word(region *reg, uintptr_t addr)
{
    return reg->segments[addr >> 48][((addr << 48) >> 48) / reg->align];
};

inline void tm_write_word(region *reg, uintptr_t addr, word_t val)
{
    reg->segments[addr >> 48][((addr << 48) >> 48) / reg->align] = val;
};

struct transaction
{
    uint64_t snapshot;
    std::vector<std::pair<uintptr_t, word_t>> read_set;
    std::unordered_map<uintptr_t, word_t> write_set;
};

bool tm_validate(region *reg, transaction *trx) noexcept
{
    //////std::cout << "... validation ...\n";
    while (true)
    {
        uint64_t timestamp = reg->global_lock.load();
        if ((timestamp & 1) != 0)
        {
            continue;
        }
        for (auto [addr, val] : trx->read_set)
        {
            if (tm_read_word(reg, addr) != val)
            {
                return false;
            }
        }
        if (timestamp == reg->global_lock.load())
        {
            trx->snapshot = timestamp;
            return true;
        }
    }
}

/** Create (i.e. allocate + init) a new shared memory region, with one first non-free-able allocated segment of the requested size and alignment.
 * @param size  Size of the first shared segment of memory to allocate (in bytes), must be a positive multiple of the alignment
 * @param align Alignment (in bytes, must be a power of 2) that the shared memory region must support
 * @return Opaque shared memory region handle, 'invalid_shared' on failure
 **/
shared_t tm_create(size_t size, size_t align) noexcept
{
    //////std::cout << "CREATE, ALIGN: " << align << ", SIZE: " << size << "\n";
    region *region = new struct region(size, align);
    if (unlikely(!region))
    {
        return invalid_shared;
    }
    region->segments[1] = std::vector<word_t>(size/align);
    return region;
}

/** Destroy (i.e. clean-up + free) a given shared memory region.
 * @param shared Shared memory region to destroy, with no running transaction
 **/
void tm_destroy(shared_t shared) noexcept
{
    delete static_cast<region *>(shared);
}

/** [thread-safe] Return the start address of the first allocated segment in the shared memory region.
 * @param shared Shared memory region to query
 * @return Start address of the first allocated segment
 **/
void *tm_start(shared_t unused(shared)) noexcept
{
    return (void *)((uint64_t)1 << 48);
}

/** [thread-safe] Return the size (in bytes) of the first allocated segment of the shared memory region.
 * @param shared Shared memory region to query
 * @return First allocated segment size
 **/
size_t tm_size(shared_t shared) noexcept
{
    return static_cast<region *>(shared)->size;
}

/** [thread-safe] Return the alignment (in bytes) of the memory accesses on the given shared memory region.
 * @param shared Shared memory region to query
 * @return Alignment used globally
 **/
size_t tm_align(shared_t shared) noexcept
{
    return static_cast<region *>(shared)->align;
}

/** [thread-safe] Begin a new transaction on the given shared memory region.
 * @param shared Shared memory region to start a transaction on
 * @param is_ro  Whether the transaction is read-only
 * @return Opaque transaction ID, 'invalid_tx' on failure
 **/
tx_t tm_begin(shared_t shared, bool unused(is_ro)) noexcept
{
    //std::cout << "BEGIN CALL\n";
    region *reg = static_cast<region *>(shared);
    transaction *tx = new transaction();
    do
    {
        tx->snapshot = reg->global_lock.load();
    } while ((tx->snapshot & 1) != 0);
    tx->read_set.reserve(reg->size/reg->align);
    tx->write_set.reserve(reg->size/reg->align);
    return reinterpret_cast<uintptr_t>(tx);
}

/** [thread-safe] End the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to end
 * @return Whether the whole transaction committed
 **/
bool tm_end(shared_t shared, tx_t tx) noexcept
{
    region *reg = static_cast<region *>(shared);
    transaction *trx = reinterpret_cast<transaction *>(tx);
    ////std::cout << "COMMIT\n";

    if (trx->write_set.empty())
    {
        delete trx;
        return true;
    }

    while (!reg->global_lock.compare_exchange_strong(trx->snapshot, trx->snapshot + 1))
    {
        if (!tm_validate(reg, trx))
        {
            delete trx;
            return false;
        }
    }

    for (auto [addr, val] : trx->write_set)
    {
        //std::cout << "  write " << val << " to " << ((addr << 48) >> 48) / 8 << "\n";
        tm_write_word(reg, addr, val);
    }

    reg->global_lock.store(trx->snapshot + 2);
    delete trx;
    return true;
}

/** [thread-safe] Read operation in the given transaction, source in the shared region and target in a private region.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param source Source start address (in the shared region)
 * @param size   Length to copy (in bytes), must be a positive multiple of the alignment
 * @param target Target start address (in a private region)
 * @return Whether the whole transaction can continue
 **/
bool tm_read(shared_t shared, tx_t tx, void const *source, size_t size, void *target) noexcept
{
    region *reg = static_cast<region *>(shared);
    transaction *trx = reinterpret_cast<transaction *>(tx);

    for (size_t i = 0; i < size / reg->align; i++)
    {
        word_t *target_addr = reinterpret_cast<word_t *>((uintptr_t)target + reg->align * i);
        uintptr_t source_addr = (uintptr_t)source + i * reg->align;

        if (trx->write_set.find(source_addr) != trx->write_set.end())
        {
            *target_addr = trx->write_set[source_addr];
            return true;
        }

        word_t val = tm_read_word(reg, source_addr);
        while (trx->snapshot != reg->global_lock.load())
        {
            if (!tm_validate(reg, trx))
            {
                delete trx;
                return false;
            }
            val = tm_read_word(reg, source_addr);
        }
        ////////std::cout << "      read " << val << " from " << source_addr << "\n";
        trx->read_set.push_back(std::make_pair(source_addr, val));
        *target_addr = val;
        ////std::cout << "  read " << val << " from " << source << "\n";
    }
    return true;
}

/** [thread-safe] Write operation in the given transaction, source in a private region and target in the shared region.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param source Source start address (in a private region)
 * @param size   Length to copy (in bytes), must be a positive multiple of the alignment
 * @param target Target start address (in the shared region)
 * @return Whether the whole transaction can continue
 **/
bool tm_write(shared_t shared, tx_t tx, void const *source, size_t size, void *target) noexcept
{
    region *reg = static_cast<region *>(shared);
    transaction *trx = reinterpret_cast<transaction *>(tx);
    for (size_t i = 0; i < size / reg->align; i++)
    {
        uintptr_t target_addr = (uintptr_t)target + i * reg->align;
        const word_t *source_addr = reinterpret_cast<const word_t *>((uintptr_t)source + reg->align * i);
        trx->write_set[target_addr] = *source_addr;
        //////std::cout << "      write " << *source_addr << " to " << target_addr << "\n";
    }
    return true;
}

/** [thread-safe] Memory allocation in the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param size   Allocation requested size (in bytes), must be a positive multiple of the alignment
 * @param target Pointer in private memory receiving the address of the first byte of the newly allocated, aligned segment
 * @return Whether the whole transaction can continue (success/nomem), or not (abort_alloc)
 **/
Alloc tm_alloc(shared_t shared, tx_t unused(tx), size_t size, void **target) noexcept
{
    region *reg = static_cast<region *>(shared);
    uintptr_t ptr = reg->next_ptr.fetch_add(1);
    *target = (void*) (ptr << 48);
    reg->segments[ptr] = std::vector<word_t>(size/reg->align);
    return Alloc::success;
}

/** [thread-safe] Memory freeing in the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param target Address of the first byte of the previously allocated segment to deallocate
 * @return Whether the whole transaction can continue
 **/
bool tm_free(shared_t shared, tx_t unused(tx), void *target) noexcept
{
    return true;
}