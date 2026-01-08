#include "lmdb2.h"
#include "shared_types.h"
#include "log.h"
#include <experimental/scope>
#include <cassert>

namespace sketch {

#define LMDB_CHECK(x) { \
    auto ret = (x); \
    if (MDB_SUCCESS != ret) { \
        LOG_ERROR << ret << "    " << #x; \
        return -1; \
    }} \

/*********************************************************************
 *    LmdbEnv
 */

LmdbEnv::LmdbEnv(const std::string& path)
  : path_(path) 
{}

LmdbEnv::~LmdbEnv() {
    if (env_) {
        mdb_env_close(env_);
    }
}

int LmdbEnv::init() {
    assert(env_ == nullptr);
    LMDB_CHECK(mdb_env_create(&env_));
    LMDB_CHECK(mdb_env_set_maxdbs(env_, max_dbs));
    LMDB_CHECK(mdb_env_set_maxreaders(env_, max_readers));
    LMDB_CHECK(mdb_env_set_mapsize(env_, db_size));
    LMDB_CHECK(mdb_env_open(env_, path_.c_str(), env_flags, permissions));
    return 0;
}

int LmdbEnv::create_db() {
    auto db = std::make_unique<Lmdb>(env_);
    return db->create();
}

LmdbPtr LmdbEnv::open_db(LmdbMode mode) {
    auto db = std::make_unique<Lmdb>(env_);
    if (db->open(mode) != 0) {
        return nullptr;
    }

    return db;
}

/*********************************************************************
 *    Lmdb
 */

Lmdb::Lmdb(MDB_env* env)
  : tid_(std::this_thread::get_id()),
    env_(env)
{
}

Lmdb::~Lmdb() {
    if (txn_) {
        mdb_txn_abort(txn_);
    }

    close_cursor();
    mdb_dbi_close(env_, table_dbi_);
    mdb_dbi_close(env_, index_dbi_);
}

int Lmdb::create() {
    assert(tid_ == std::this_thread::get_id());

    MDB_txn *txn = nullptr;
    LMDB_CHECK(mdb_txn_begin(env_, NULL, 0, &txn));
    std::experimental::scope_exit txn_aborter([&] {
        if (txn) {
            mdb_txn_abort(txn);
        }
    });

    MDB_dbi table_dbi;
    LMDB_CHECK(mdb_dbi_open(txn, MapTableName, MDB_CREATE, &table_dbi));

    MDB_dbi index_dbi;
    LMDB_CHECK(mdb_dbi_open(txn, IndexTableName, MDB_CREATE | MDB_DUPSORT, &index_dbi));

    mdb_txn_commit(txn);
    txn = nullptr;

    mdb_dbi_close(env_, table_dbi);
    mdb_dbi_close(env_, index_dbi);

    return 0;
}

int Lmdb::open(LmdbMode mode) {
    assert(tid_ == std::this_thread::get_id());

    mode_ = mode;
    int txn_flags = mode_ == LmdbMode::Read ? MDB_RDONLY : 0;
    LMDB_CHECK(mdb_txn_begin(env_, NULL, txn_flags, &txn_));
    LMDB_CHECK(mdb_dbi_open(txn_, MapTableName, 0, &table_dbi_));
    LMDB_CHECK(mdb_dbi_open(txn_, IndexTableName, 0, &index_dbi_));

    return 0;
}

int Lmdb::write_record(uint64_t tag, uint32_t record_id, uint16_t cluster_id) {
    assert(tid_ == std::this_thread::get_id());

    if (mode_ != LmdbMode::Write) {
        return -1;
    }

    if (!txn_) {
        int ret = open(mode_);
        if (ret != 0) {
            return ret;
        }
    }

    const size_t MaxBufSize = sizeof(record_id) + sizeof(cluster_id);
    char buf[MaxBufSize];
    size_t buf_size = cluster_id == InvalidClusterId ? sizeof(record_id) : sizeof(record_id) + sizeof(cluster_id);
    memcpy(buf, &record_id, sizeof(record_id));
    if (cluster_id != InvalidClusterId) {
        memcpy(buf + sizeof(record_id), &cluster_id, sizeof(cluster_id));
    }

    {
        MDB_val mdb_key {
            .mv_size = sizeof(tag),
            .mv_data = &tag,
        };
        MDB_val mdb_data {
            .mv_size = buf_size,
            .mv_data = buf,
        };

        int ret = mdb_put(txn_, table_dbi_, &mdb_key, &mdb_data, 0);
        if (ret != 0) {
            return ret;
        }
    }

    if (cluster_id != InvalidClusterId) {
        MDB_val mdb_key {
            .mv_size = sizeof(cluster_id),
            .mv_data = &cluster_id
        };
        MDB_val mdb_data {
            .mv_size = sizeof(record_id),
            .mv_data = &record_id
        };

        int ret = mdb_put(txn_, index_dbi_, &mdb_key, &mdb_data, 0);
        if (ret != 0) {
            return ret;
        }
    }

    return 0;
}

int Lmdb::delete_record(uint64_t tag, uint32_t record_id, uint16_t cluster_id) {
    assert(tid_ == std::this_thread::get_id());

    if (mode_ != LmdbMode::Write) {
        return -1;
    }

    if (!txn_) {
        int ret = open(mode_);
        if (ret != 0) {
            return ret;
        }
    }

    MDB_val mdb_key {
        .mv_size = sizeof(tag),
        .mv_data = &tag
    };

    MDB_val mdb_data;

    int ret = mdb_del(txn_, table_dbi_, &mdb_key, &mdb_data);
    if (ret != 0) {
        return ret;
    }

    return delete_index(cluster_id, record_id);
}

int Lmdb::delete_index(uint16_t cluster_id, uint32_t record_id) {
    assert(tid_ == std::this_thread::get_id());

    if (mode_ != LmdbMode::Write) {
        return -1;
    }

    if (!txn_) {
        int ret = open(mode_);
        if (ret != 0) {
            return ret;
        }
    }

    if (cluster_id != InvalidClusterId) {
        MDB_val mdb_key {
            .mv_size = sizeof(cluster_id),
            .mv_data = &cluster_id
        };

        MDB_val mdb_data {
            .mv_size = sizeof(record_id),
            .mv_data = &record_id
        };

        int ret = mdb_del(txn_, index_dbi_, &mdb_key, &mdb_data);
        if (ret != 0) {
            return ret;
        }
    }

    return 0;
}

int Lmdb::read_record(uint64_t tag, uint32_t& record_id, uint16_t& cluster_id) {
    assert(tid_ == std::this_thread::get_id());

    if (mode_ != LmdbMode::Read) {
        return -1;
    }

    if (!txn_) {
        int ret = open(mode_);
        if (ret != 0) {
            return ret;
        }
    }

    MDB_val mdb_key {
        .mv_size = sizeof(tag),
        .mv_data = &tag
    };
    MDB_val mdb_data;

    int ret = mdb_get(txn_, table_dbi_, &mdb_key, &mdb_data);
    if (ret != 0) {
        return ret;
    }

    assert(mdb_data.mv_size >= sizeof(record_id));
    memcpy(&record_id, mdb_data.mv_data, sizeof(record_id));

    const size_t MaxBufSize = sizeof(record_id) + sizeof(cluster_id);
    if (mdb_data.mv_size == MaxBufSize) {
        assert(mdb_data.mv_size >= MaxBufSize);
        memcpy(&cluster_id, (char*)mdb_data.mv_data + sizeof(record_id), sizeof(cluster_id));
    } else {
        cluster_id = InvalidClusterId;
    }

    return 0;
}


int Lmdb::commit() {
    assert(tid_ == std::this_thread::get_id());

    int ret = mdb_txn_commit(txn_);
    txn_ = nullptr;
    return ret;
}

void Lmdb::abort() {
    assert(tid_ == std::this_thread::get_id());

    mdb_txn_abort(txn_);
    txn_ = nullptr;
}

int Lmdb::open_cursor(uint16_t current_cluster_id) {
    assert(tid_ == std::this_thread::get_id());
    assert(cursor_ == nullptr);

    if (mode_ != LmdbMode::Read) {
        return -1;
    }

    if (!txn_) {
        int ret = open(mode_);
        if (ret != 0) {
            return ret;
        }
    }

    int iret = mdb_cursor_open(txn_, index_dbi_, &cursor_);
    if (iret != 0) {
        return iret;
    }

    current_cluster_id_ = current_cluster_id;
    cursor_key_.mv_size = sizeof(current_cluster_id_);
    cursor_key_.mv_data = &current_cluster_id_;

    cursor_positioned_ = false;
    
    return 0;
}

void Lmdb::close_cursor() {
    if (cursor_) {
        mdb_cursor_close(cursor_);
        cursor_ = nullptr;
    }
}

int Lmdb::next(uint32_t& record_id) {
    if (!cursor_) {
        return -110;
    }

    int iret = 0;
    MDB_val mdb_data;
    if (!cursor_positioned_) {
        cursor_positioned_ = true;
        iret = mdb_cursor_get(cursor_, &cursor_key_, &mdb_data, MDB_SET);
    } else {
        iret = mdb_cursor_get(cursor_, &cursor_key_, &mdb_data, MDB_NEXT_DUP);
    }

    if (iret != 0) {
        return iret;
    }

    if (mdb_data.mv_size != sizeof(record_id)) {
        return -111;
    }

    memcpy(&record_id, mdb_data.mv_data, sizeof(record_id));
    return 0;
}


} // namespace sketch
