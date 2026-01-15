#include "engine.h"
#include "command_router.h"
#include "string_utils.h"
#include "log.h"
#include "gtest/gtest.h"

#include <unistd.h>
#include <filesystem>
#include <memory>
#include <iostream>
#include <fstream>
#include <format>
#include <experimental/scope>

using namespace sketch;

#ifdef CLOUD
static const char* GeneratedFile = "/mnt/data/test/generated.data";
static const char* Path = "/mnt/data/test";
#else
static const char* GeneratedFile = "/home/mpopov/test/generated.data";
static const char* Path = "/home/mpopov/test";
#endif

class DmlTestSettings {
public:
    DmlTestSettings(uint64_t dim = 128, uint64_t count = 8) {
        //TempLogLevel temp_level(LL_DEBUG);

        cfg_.data_path = path_;
        std::filesystem::remove_all(cfg_.data_path);
        mkdir(cfg_.data_path.c_str(), 0755);

        engine_ = std::make_unique<Engine>(cfg_);
        Ret ret = engine_->init();
        if (ret != 0)  std::cerr << ret.message() << std::endl;

        router_ = std::make_unique<CommandRouter>(*engine_);
        ret = router_->init();
        if (ret != 0)  std::cerr << ret.message() << std::endl;

        ret = router_->process_command("CREATE CATALOG test;");
        if (ret != 0)  std::cerr << "ERROR: " << ret.message() << std::endl;
        ret = router_->process_command(std::format("CREATE DATASET test.ds TYPE=f32 DIM={} NODES={};", dim, count));
        if (ret != 0)  std::cerr << "ERROR: " << ret.message() << std::endl;
        ret = router_->process_command("USE test.ds;");
        if (ret != 0)  std::cerr << "ERROR: " << ret.message() << std::endl;
    }

    ~DmlTestSettings() {
        engine_ = nullptr;
        std::filesystem::remove_all(cfg_.data_path);
    }

    Engine& engine() { return *engine_; }
    CommandRouter& router() { return *router_; }

private:
    const char* path_ = Path;
    Config cfg_;
    std::unique_ptr<Engine> engine_;
    std::unique_ptr<CommandRouter> router_;
};

static void write_text(const std::string& path, const std::string* text, uint64_t count) {
    std::ofstream file(path);
    for (uint64_t i = 0; i < count; i++) {
        file << text[i] << std::endl;
    }
    file.close();
}

TEST(DML, RouterLoadDump) {
    DmlTestSettings dts(/*dim=*/3, /*count=*/1);
    CommandRouter& router = dts.router();

    auto ret = router.process_command(std::format("GENERATE {} 8 3", GeneratedFile));
    std::experimental::scope_exit closer([&] {
        unlink(GeneratedFile);
    });
    ASSERT_EQ(0, ret) << "ERROR: " << ret.message();

    std::string base_path = "/tmp/test/";
    std::filesystem::remove_all(base_path);

    TempLogLevel temp_level(LL_DEBUG);
    {
        LOG_DEBUG << "============ INITIAL =============";
        auto cmd = std::format("LOAD {}", GeneratedFile);
        auto ret = router.process_command(cmd);
        ASSERT_EQ(0, ret) << "ERROR: " << ret.message();

        std::string data_path = base_path + "1/";
        std::filesystem::create_directories(data_path);
        cmd = std::format("DUMP {}", data_path);
        ret = router.process_command(cmd);
        ASSERT_EQ(0, ret);
        system("cat /tmp/test/1/aaa/*");
    }

    {
        auto ret = router.process_command("DUMP");
        ASSERT_EQ(0, ret);
    }
    
    {
        LOG_DEBUG << "============ DELETE =============";
        const std::string lines[] = {
            "0: []",
            "1: []",
            "2: []",
            "3: []",
        };
        std::string input_path = base_path + "delete_input";
        write_text(input_path, lines, sizeof(lines) / sizeof(lines[0]));

        auto cmd = std::format("LOAD {}", input_path);
        auto ret = router.process_command(cmd);
        ASSERT_EQ(0, ret) << "ERROR: " << ret.message();

        std::string data_path = base_path + "2/";
        std::filesystem::create_directories(data_path);
        cmd = std::format("DUMP {}", data_path);
        ret = router.process_command(cmd);
        ASSERT_EQ(0, ret);
        system("cat /tmp/test/2/aaa/*");
    }

    {
        LOG_DEBUG << "============ UPDATE =============";
        const std::string lines[] = {
            "4 : [ 44.1, 44.2, 44.3 ]",
            "5 : [ 55.1, 55.2, 55.3 ]",
        };
        std::string input_path = base_path + "update_input";
        write_text(input_path, lines, sizeof(lines) / sizeof(lines[0]));

        auto cmd = std::format("LOAD {}", input_path);
        auto ret = router.process_command(cmd);
        ASSERT_EQ(0, ret) << "ERROR: " << ret.message();

        std::string data_path = base_path + "3/";
        std::filesystem::create_directories(data_path);
        cmd = std::format("DUMP {}", data_path);
        ret = router.process_command(cmd);
        ASSERT_EQ(0, ret);
        system("cat /tmp/test/3/aaa/*");
    }

    {
        LOG_DEBUG << "============ COMBINED =============";
        const std::string lines[] = {
            "4 : []",
            "7 : [ 77.1, 77.2, 77.3 ]",
            "8 : [ 88.1, 88.2, 88.3 ]",
            "9 : [ 999.1, 999.2, 999.3 ]",
        };
        std::string input_path = base_path + "combined_input";
        write_text(input_path, lines, sizeof(lines) / sizeof(lines[0]));

        auto cmd = std::format("LOAD {}", input_path);
        auto ret = router.process_command(cmd);
        ASSERT_EQ(0, ret) << "ERROR: " << ret.message();

        std::string data_path = base_path + "4/";
        std::filesystem::create_directories(data_path);
        cmd = std::format("DUMP {}", data_path);
        ret = router.process_command(cmd);
        ASSERT_EQ(0, ret) << "ERROR: " << ret.message();
        system("cat /tmp/test/4/aaa/*");
    }
}

TEST(DML, RouterGenerateLoadFindKnn) {
//    TempLogLevel temp_level(LL_DEBUG);
    DmlTestSettings dts;
    CommandRouter& router = dts.router();

    auto ret = router.process_command(std::format("GENERATE {} 16 128", GeneratedFile));
    std::experimental::scope_exit closer([&] {
        unlink(GeneratedFile);
    });
    ASSERT_EQ(0, ret) << "ERROR: " << ret.message();

    {
        auto cmd = std::format("LOAD {}", GeneratedFile);
        auto ret = router.process_command(cmd);
        ASSERT_EQ(0, ret) << "ERROR: " << ret.message();
    }

    {
        auto ret = router.process_command(std::format("FIND TAG 5 {}", GeneratedFile));
        ASSERT_EQ(0, ret) << "ERROR: " << ret.message();
    }

    {
        auto ret = router.process_command(std::format("FIND DATA #1 {}", GeneratedFile));
        ASSERT_EQ(0, ret) << "ERROR: " << ret.message();
    }

    {
        auto ret = router.process_command(std::format("KNN L1 3 #8 {}", GeneratedFile));
        ASSERT_EQ(0, ret) << "ERROR: " << ret.message();
        //std::cerr << "RESULT:\n" << ret.message() << std::endl;
    }

    {
        auto ret = router.process_command(std::format("KNN L2 3 #8 {}", GeneratedFile));
        ASSERT_EQ(0, ret) << "ERROR: " << ret.message();
        //std::cerr << "RESULT:\n" << ret.message() << std::endl;
    }

    {
        auto ret = router.process_command(std::format("KNN COS 3 #8 {}", GeneratedFile));
        ASSERT_EQ(0, ret) << "ERROR: " << ret.message();
        //std::cerr << "RESULT:\n" << ret.message() << std::endl;
    }
}

TEST(DML, RouterLoadLarge) {
    DmlTestSettings dts;

    size_t threads_count = 8;
    dts.engine().start_tread_pool(threads_count);
    
    size_t count = 100'000;
    //size_t count = 1'000'000;
    //size_t count = 10'000'000;

    CommandRouter& router = dts.router();

    std::experimental::scope_exit closer([&] {
        unlink(GeneratedFile);
    });

    {
        const Timer t("GENERATE");
        auto ret = router.process_command(std::format("GENERATE {} {} 128 1", GeneratedFile, count));
        ASSERT_EQ(0, ret) << "ERROR: " << ret.message();
    }

    {
        const Timer t("LOAD");
        auto cmd = std::format("LOAD {}", GeneratedFile);
        auto ret = router.process_command(cmd);
        ASSERT_EQ(0, ret) << "ERROR: " << ret.message();
    }

    {
        uint64_t tag = count - 1;
        const Timer t(std::format("FIND TAG {}", tag));
        auto ret = router.process_command(std::format("FIND TAG {} {}", tag, GeneratedFile));
        ASSERT_EQ(0, ret) << "ERROR: " << ret.message();
    }

    {
        uint64_t index = count - 1;
        const Timer t(std::format("FIND DATA {}", index));
        auto ret = router.process_command(std::format("FIND DATA #{} {};", index, GeneratedFile));
        ASSERT_EQ(0, ret) << "ERROR: " << ret.message();
        //std::cerr << "RESULT: " << ret.message() << std::endl;
    }

    {
        uint64_t index = count / 2;
        const Timer t("KNN");
        auto ret = router.process_command(std::format("KNN L2 1000 #{} {}", index, GeneratedFile));
        ASSERT_EQ(0, ret) << "ERROR: " << ret.message();
        //std::cerr << "RESULT: " << ret.message() << std::endl;
    }

    if (0) {
        const Timer t("KMEANS++");
        auto ret = router.process_command("KMEANS++ 10 10000");
        ASSERT_EQ(0, ret) << "ERROR: " << ret.message();
        std::cerr << "KMEANS++ RESULT: " << ret.message() << std::endl;
    }

    if (0) {
        const Timer t("MAKE_CENTROIDS");
        auto ret = router.process_command("MAKE_CENTROIDS 10 10000 16");
        ASSERT_EQ(0, ret) << "ERROR: " << ret.message();
        std::cerr << "MAKE_CENTROIDS RESULT: " << ret.message() << std::endl;
    }

    {
        uint64_t index = count / 2;
        auto ret = router.process_command(std::format("ANN 20 5 #{} {}", index, GeneratedFile));
        ASSERT_NE(0, ret);
    }

    //TempLogLevel temp_level(LL_DEBUG);
    for (int i = 0; i < 3; i++) {
        {
            const Timer t("MAKE_IVF");
            auto ret = router.process_command("MAKE_IVF 10 10000 16");
            ASSERT_EQ(0, ret) << "ERROR: " << ret.message();
            std::cerr << "MAKE_IVF RESULT: " << ret.message() << std::endl;
        }

        {
            uint64_t tag = count - 1;
            const Timer t(std::format("FIND TAG {}", tag));
            auto ret = router.process_command(std::format("FIND TAG {} {}", tag, GeneratedFile));
            ASSERT_EQ(0, ret) << "ERROR: " << ret.message();
        }

        {
            uint64_t index = count - 1;
            const Timer t(std::format("FIND DATA {}", index));
            auto ret = router.process_command(std::format("FIND DATA #{} {};", index, GeneratedFile));
            ASSERT_EQ(0, ret) << "ERROR: " << ret.message();
            //std::cerr << "RESULT: " << ret.message() << std::endl;
        }
    }

    {
        auto ret = router.process_command(std::format("GC"));
        ASSERT_EQ(0, ret) << "ERROR: " << ret.message();
    }

    {
        uint64_t index = count / 2;
        const Timer t("ANN");
        auto ret = router.process_command(std::format("ANN 20 5 #{} {}", index, GeneratedFile));
        ASSERT_EQ(0, ret) << "ERROR: " << ret.message();
        //std::cerr << "RESULT: " << ret.message() << std::endl;
    }
}
