#pragma once
#include "lmdb.h"

#include <memory>
#include <string>
#include <thread>

#include <string.h>

namespace sketch {

static constexpr const char* MapTableName = "records";
static constexpr const char* IndexTableName = "index";
static constexpr const uint16_t InvalidClusterId = 0xFFFF;

enum class LmdbMode {
    Read,
    Write,
};

class Lmdb {
public:
    Lmdb(MDB_env* env);
    ~Lmdb();

    int create();
    int open(LmdbMode mode = LmdbMode::Read);

    int write_record(uint64_t tag, uint32_t record_id, uint16_t cluster_id = InvalidClusterId);
    int read_record(uint64_t tag, uint32_t& record_id, uint16_t& cluster_id);
    int delete_record(uint64_t tag, uint32_t record_id, uint16_t cluster_id = InvalidClusterId);
    int delete_index(uint16_t cluster_id, uint32_t record_id);

    int open_cursor(uint16_t cluster_id);
    void close_cursor();
    int next(uint32_t& record_id);

    int commit();
    void abort();

private:
    const std::thread::id tid_;
    MDB_env* env_ = nullptr;
    LmdbMode mode_ = LmdbMode::Read;
    MDB_dbi table_dbi_;
    MDB_dbi index_dbi_;
    MDB_txn *txn_ = nullptr;
    MDB_cursor *cursor_ = nullptr;
    MDB_val cursor_key_;
    uint16_t current_cluster_id_ = 0;
    bool cursor_positioned_ = false;

};

using LmdbPtr = std::unique_ptr<Lmdb>;

class LmdbEnv {
public:
    LmdbEnv(const std::string& path);
    ~LmdbEnv();

    int init();
    int create_db();
    LmdbPtr open_db(LmdbMode mode = LmdbMode::Read);

private:
    const int max_dbs = 16;
    const int max_readers = 16;
    const int db_size = 1024 * 1024 * 1024;
    const int permissions = 0664;
    const int env_flags = 0; // MDB_NOTLS

private:
    const std::string path_;
    MDB_env *env_ = nullptr;
};



} // namespace sketch
