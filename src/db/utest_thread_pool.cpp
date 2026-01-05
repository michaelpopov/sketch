#include "thread_pool.h"
#include "log.h"
#include "gtest/gtest.h"

#include <unistd.h>

using namespace sketch;

struct ChunkResult {
    size_t count;
    // Other result data
};

ChunkResult process_chunk(size_t chunk_index) {
    ChunkResult result;
    result.count = chunk_index;
    return result;
}

TEST(THREAD_POOL, Basics) {
    size_t num_chunks = 10;

    size_t num_threads = std::thread::hardware_concurrency();
    if (num_threads == 0) num_threads = 4;  // fallback
    ThreadPool pool(num_threads);

    std::vector<std::future<ChunkResult>> futures;
    futures.reserve(num_chunks);

    size_t check_count = 0;
    for (size_t i = 0; i < num_chunks; ++i) {
        check_count += i;

        futures.push_back(pool.submit(
            [i] {
                return process_chunk(i);
            }
        ));
    }

    size_t total_count = 0;
    for (auto& f : futures) {
        ChunkResult r = f.get();
        total_count  += r.count;
    }

    ASSERT_EQ(check_count, total_count);
}
