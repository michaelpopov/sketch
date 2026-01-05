#pragma once

#include <pthread.h>
#include <system_error>

namespace sketch {

class RWLock {
public:
    RWLock() {
        pthread_rwlockattr_t attr;
        pthread_rwlockattr_init(&attr);

        // Set the lock to prefer writers to avoid starvation
        // PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP ensures that as long 
        // as a writer is waiting, new readers cannot take the lock.
        pthread_rwlockattr_setkind_np(&attr, PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);

        int res = pthread_rwlock_init(&_rwlock, &attr);
        
        // Cleanup attribute object (no longer needed after init)
        pthread_rwlockattr_destroy(&attr);

        if (res != 0) {
            throw std::system_error(res, std::generic_category(), "Failed to init rwlock");
        }
    }

    ~RWLock() {
        pthread_rwlock_destroy(&_rwlock);
    }

    // Disable copying
    RWLock(const RWLock&) = delete;
    RWLock& operator=(const RWLock&) = delete;

    void lock_shared() { pthread_rwlock_rdlock(&_rwlock); }
    void lock()        { pthread_rwlock_wrlock(&_rwlock); }
    void unlock()      { pthread_rwlock_unlock(&_rwlock); }

    bool try_lock_shared() { return pthread_rwlock_tryrdlock(&_rwlock) == 0; }
    bool try_lock()        { return pthread_rwlock_trywrlock(&_rwlock) == 0; }

private:
    pthread_rwlock_t _rwlock;
};

// RAII Guard for Reading
class ReadGuard {
public:
    explicit ReadGuard(RWLock& lock) : _lock(lock) { _lock.lock_shared(); }
    ~ReadGuard() { _lock.unlock(); }
    
    // Non-copyable
    ReadGuard(const ReadGuard&) = delete;
    ReadGuard& operator=(const ReadGuard&) = delete;
private:
    RWLock& _lock;
};

// RAII Guard for Writing
class WriteGuard {
public:
    explicit WriteGuard(RWLock& lock) : _lock(lock) { _lock.lock(); }
    ~WriteGuard() { _lock.unlock(); }

    // Non-copyable
    WriteGuard(const WriteGuard&) = delete;
    WriteGuard& operator=(const WriteGuard&) = delete;
private:
    RWLock& _lock;
};

} // namespace sketch