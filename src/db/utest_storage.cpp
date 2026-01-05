#include "storage.h"
#include "log.h"
#include "gtest/gtest.h"

#include <unistd.h>

using namespace sketch;

TEST(STORAGE, CtorDtor) {
    const std::string path = "/tmp/test_storage.dat";
    unlink(path.c_str());

    const uint64_t record_size = 128;

    {
        Storage storage(path, record_size);
    }
    // No crash on dtor.

    unlink(path.c_str());
}

TEST(STORAGE, Init) {
    const std::string path = "/tmp/test_storage.dat";
    const std::string path_info = "/tmp/test_storage.dat.info";
    unlink(path.c_str());
    unlink(path_info.c_str());

    const uint64_t record_size = 128;

    {
        Storage storage(path, record_size);
        auto ret = storage.create(1000);
        ASSERT_EQ(0, ret) << "Failed to create storage: " << ret.message();
    }

    {
        Storage storage(path, record_size);
        ASSERT_EQ(0, storage.init());
        ASSERT_EQ(0, storage.upper_record_id());
        ASSERT_EQ(1000, storage.records_limit());
        ASSERT_EQ(0, storage.uninit());
    }

    // Reading info file.
    {
        Storage storage(path, record_size);
        ASSERT_EQ(0, storage.init());
        ASSERT_EQ(0, storage.uninit());
    }

    unlink(path_info.c_str());

    // Init without info file.
    {
        Storage storage(path, record_size);
        ASSERT_EQ(0, storage.init());
        ASSERT_EQ(0, storage.uninit());
    }

    unlink(path.c_str());
    unlink(path_info.c_str());
}

TEST(STORAGE, Basics) {
    //TempLogLevel temp_level(LL_DEBUG);

    const std::string path = "/tmp/test_storage.dat";
    const std::string path_info = "/tmp/test_storage.dat.info";
    unlink(path.c_str());
    unlink(path_info.c_str());

    const uint64_t header_size = HeaderSize;
    const uint64_t record_size = 128;

    {
        Storage storage(path, record_size);
        auto ret = storage.create(1000);
        ASSERT_EQ(0, ret) << "Failed to create storage: " << ret.message();
    }

    {
        Storage storage(path, record_size);
        ASSERT_EQ(0, storage.init());
        ASSERT_EQ(0, storage.upper_record_id());

        DataBuffer buf(record_size, header_size);
        DataBuffer check_buf(record_size, header_size);

        buf.set_header(1);
        ASSERT_EQ(1, buf.get_header());
        memset(buf.record_ptr(), 'A', record_size);
        auto [ record_id, ret ] = storage.put_record(buf);
        ASSERT_EQ(0, ret) << "Failed to put record: " << ret.message();
        ASSERT_EQ(0, record_id);
        ASSERT_EQ(1, storage.upper_record_id());

        buf.set_header(2);
        ASSERT_EQ(2, buf.get_header());
        memset(buf.record_ptr(), 'B', record_size);
        std::tie(record_id, ret) = storage.put_record(buf);
        ASSERT_EQ(0, ret) << "Failed to put record: " << ret.message();
        ASSERT_EQ(1, record_id);
        ASSERT_EQ(2, storage.upper_record_id());

        memset(check_buf.record_ptr(), 'A', record_size);
        record_id = 0;
        auto [record, get_ret] = storage.get_record(record_id);
        ret = get_ret;
        ASSERT_EQ(0, ret) << "Failed to get record: " << ret.message();
        ASSERT_EQ(1, record.tag);
        ASSERT_EQ(0, memcmp(check_buf.record_ptr(), record.data, record_size));

        memset(check_buf.record_ptr(), 'B', record_size);
        record_id = 1;
        std::tie(record, get_ret) = storage.get_record(record_id);
        ret = get_ret;
        ASSERT_EQ(0, ret) << "Failed to get record: " << ret.message();
        ASSERT_EQ(2, record.tag);
        ASSERT_EQ(0, memcmp(check_buf.record_ptr(), record.data, record_size));

        ASSERT_EQ(0, storage.deleted_count());

        record_id = 0;
        ret = storage.delete_record(record_id);
        ASSERT_EQ(0, ret) << "Failed to delete record: " << ret.message();
        ASSERT_EQ(1, storage.deleted_count());


        record_id = 1;
        memset(buf.record_ptr(), 'C', record_size);
        ret = storage.update_record(record_id, buf);
        ASSERT_EQ(0, ret) << "Failed to update record: " << ret.message();

        memset(check_buf.record_ptr(), 'C', record_size);
        std::tie(record, get_ret) = storage.get_record(record_id);
        ret = get_ret;
        ASSERT_EQ(0, ret) << "Failed to get record: " << ret.message();
        ASSERT_EQ(2, record.tag);
        ASSERT_EQ(0, memcmp(check_buf.record_ptr(), record.data, record_size));

        ASSERT_EQ(0, storage.uninit());
    }

    // Reading info file.
    {
        Storage storage(path, record_size);
        ASSERT_EQ(0, storage.init());

        ASSERT_EQ(1, storage.records_count());
        ASSERT_EQ(2, storage.upper_record_id());
        ASSERT_EQ(1000, storage.records_limit());
        ASSERT_EQ(1, storage.deleted_count());

        ASSERT_EQ(0, storage.uninit());
    }

    unlink(path_info.c_str());

    // Init without info file.
    {
        Storage storage(path, record_size);
        ASSERT_EQ(0, storage.init());

        ASSERT_EQ(1, storage.records_count());
        ASSERT_EQ(2, storage.upper_record_id());
        ASSERT_EQ(1000, storage.records_limit());
        ASSERT_EQ(1, storage.deleted_count());

        ASSERT_EQ(0, storage.uninit());
    }

    unlink(path.c_str());
    unlink(path_info.c_str());
}

TEST(STORAGE, ReadCheckModifyCheck) {
    //TempLogLevel temp_level(LL_DEBUG);

    const std::string path = "/tmp/test_storage.dat";
    const std::string path_info = "/tmp/test_storage.dat.info";
    unlink(path.c_str());
    unlink(path_info.c_str());

    const uint64_t header_size = HeaderSize;
    const uint64_t record_size = 128;

    {
        Storage storage(path, record_size);
        auto ret = storage.create(1000);
        ASSERT_EQ(0, ret) << "Failed to create storage: " << ret.message();
    }

    {
        Storage storage(path, record_size);
        ASSERT_EQ(0, storage.init());
        ASSERT_EQ(0, storage.upper_record_id());

        DataBuffer buf(record_size, header_size);

        //-------- PUT -------------------
        buf.set_header(1);
        ASSERT_EQ(1, buf.get_header());
        memset(buf.record_ptr(), 'A', record_size);
        auto [ record_id, ret ] = storage.put_record(buf);
        ASSERT_EQ(0, ret) << "Failed to put record: " << ret.message();
        ASSERT_EQ(0, record_id);
        ASSERT_EQ(1, storage.upper_record_id());
        ASSERT_EQ(1, storage.records_count());

        //-------- GET --------------------
        record_id = 0;
        auto [record, get_ret] = storage.get_record(record_id);
        ret = get_ret;
        ASSERT_EQ(0, ret) << "Failed to get record: " << ret.message();
        ASSERT_EQ(1, record.tag);

        //-------- UPDATE -----------------
        memset(buf.record_ptr(), 'C', record_size);
        ret = storage.update_record(record_id, buf);
        ASSERT_EQ(0, ret) << "Failed to update record: " << ret.message();
    }

    unlink(path.c_str());
    unlink(path_info.c_str());
}

TEST(STORAGE, HitUpperLimit) {
    //TempLogLevel temp_level(LL_DEBUG);

    const std::string path = "/tmp/test_storage.dat";
    const std::string path_info = "/tmp/test_storage.dat.info";
    unlink(path.c_str());
    unlink(path_info.c_str());

    const uint64_t header_size = HeaderSize;
    const uint64_t record_size = 128;
    const uint64_t max_records = 3;

    {
        Storage storage(path, record_size);
        auto ret = storage.create(max_records);
        ASSERT_EQ(0, ret) << "Failed to create storage: " << ret.message();
    }

    {
        Storage storage(path, record_size);
        ASSERT_EQ(0, storage.init());
        ASSERT_EQ(0, storage.upper_record_id());

        DataBuffer buf(record_size, header_size);
        memset(buf.record_ptr(), 'A', record_size);

        //-------- FILL UP -------------------
        for (uint64_t i = 0; i < max_records; i++) {
            buf.set_header(i + 1);
            auto [record_id, ret] = storage.put_record(buf);
            ASSERT_EQ(0, ret) << "Failed to put record: " << ret.message();
            ASSERT_EQ(i, record_id);
            ASSERT_EQ(i + 1, storage.upper_record_id());
            ASSERT_EQ(i + 1, storage.records_count());
        }

        //-------- PUT -------------------
        buf.set_header(max_records);
        ASSERT_EQ(max_records, buf.get_header());
        auto [record_id, ret] = storage.put_record(buf);
        ASSERT_NE(0, ret) << "Failed to fail when put record";
        ASSERT_EQ(max_records, storage.upper_record_id());
        ASSERT_EQ(max_records, storage.records_count());
    }

    unlink(path.c_str());
    unlink(path_info.c_str());
}