#include "engine.h"
#include "log.h"
#include "gtest/gtest.h"

#include <unistd.h>
#include <filesystem>

using namespace sketch;

TEST(DDL, Catalog) {
    Config cfg;
    cfg.data_path = "/tmp/sketch_test_db";
    std::filesystem::remove_all(cfg.data_path);
    mkdir(cfg.data_path.c_str(), 0755);

    const std::string test_catalog_name = "test_catalog";
    Ret ret(0);

    {
        Engine engine(cfg);
        ret = engine.init();
        ASSERT_EQ(0, ret) << "Failed to initialize engine: " << ret.message();

        CmdCreateCatalog create_catalog_cmd {
            .catalog_name = test_catalog_name,
        };
        Ret ret = engine.create_catalog(create_catalog_cmd);
        ASSERT_EQ(0, ret) << "Failed to create catalog: " << ret.message();

        ret = engine.list_catalogs(CmdListCatalogs {});
        ASSERT_EQ(0, ret) << "Failed to list catalogs: " << ret.message();
        auto pos = ret.message().find(test_catalog_name);
        ASSERT_NE(std::string::npos, pos) << "Catalog not found in list";
    }

    {
        Engine engine(cfg);
        ret = engine.init();
        ASSERT_EQ(0, ret) << "Failed to initialize engine: " << ret.message();

        ret = engine.list_catalogs(CmdListCatalogs {});
        ASSERT_EQ(0, ret) << "Failed to list catalogs: " << ret.message();
        auto pos = ret.message().find(test_catalog_name);
        ASSERT_NE(std::string::npos, pos) << "Catalog not found in list";

        ret = engine.drop_catalog(CmdDropCatalog {
            .catalog_name = test_catalog_name,
        });
        ASSERT_EQ(0, ret) << "Failed to drop catalog: " << ret.message();

        ret = engine.list_catalogs(CmdListCatalogs {});
        ASSERT_EQ(0, ret) << "Failed to list catalogs: " << ret.message();
        pos = ret.message().find(test_catalog_name);
        ASSERT_EQ(std::string::npos, pos) << "Catalog not found in list";
    }

    std::filesystem::remove_all(cfg.data_path);
}


TEST(DDL, Dataset) {
    TempLogLevel temp_level(LL_DEBUG);

    Config cfg;
    cfg.data_path = "/tmp/sketch_test_db";
    std::filesystem::remove_all(cfg.data_path);
    mkdir(cfg.data_path.c_str(), 0755);

    const std::string catalog_name_A = "aaa";
    const std::string catalog_name_B = "bbb";
    const std::string dataset_name_1 = "d111";
    const std::string dataset_name_2 = "d222";
    Ret ret(0);

    {
        Engine engine(cfg);
        ret = engine.init();
        ASSERT_EQ(0, ret) << "Failed to initialize engine: " << ret.message();

        {
            CmdCreateCatalog create_catalog_cmd {
                .catalog_name = catalog_name_A,
            };
            Ret ret = engine.create_catalog(create_catalog_cmd);
            ASSERT_EQ(0, ret) << "Failed to create catalog: " << ret.message();
        }

        {
            CmdCreateCatalog create_catalog_cmd {
                .catalog_name = catalog_name_B,
            };
            Ret ret = engine.create_catalog(create_catalog_cmd);
            ASSERT_EQ(0, ret) << "Failed to create catalog: " << ret.message();
        }

    }

    {
        Engine engine(cfg);
        ret = engine.init();
        ASSERT_EQ(0, ret) << "Failed to initialize engine: " << ret.message();

        ret = engine.list_catalogs(CmdListCatalogs {});
        ASSERT_EQ(0, ret) << "Failed to list catalogs: " << ret.message();
        auto pos = ret.message().find(catalog_name_A);
        ASSERT_NE(std::string::npos, pos) << "Catalog AAA not found in list";
        pos = ret.message().find(catalog_name_B);
        ASSERT_NE(std::string::npos, pos) << "Catalog BBB not found in list";
    }

    {
        Engine engine(cfg);
        ret = engine.init();
        ASSERT_EQ(0, ret) << "Failed to initialize engine: " << ret.message();

        {
            CmdCreateDataset cmd {
                .catalog_name = catalog_name_A,
                .dataset_name = dataset_name_1,
                .type = DatasetType::f32,
                .dim = 768,
                .nodes_count = 16,
            };
            ret = engine.create_dataset(cmd);
            ASSERT_EQ(0, ret) << "Failed to create dataset: " << ret.message();
        }

        {
            CmdCreateDataset cmd {
                .catalog_name = catalog_name_A,
                .dataset_name = dataset_name_2,
                .type = DatasetType::f16,
                .dim = 1536,
                .nodes_count = 4,
            };
            ret = engine.create_dataset(cmd);
            ASSERT_EQ(0, ret) << "Failed to create dataset: " << ret.message();
        }
    }

    {
        Engine engine(cfg);
        ret = engine.init();
        ASSERT_EQ(0, ret) << "Failed to initialize engine: " << ret.message();

        {
            CmdListDatasets cmd {
                .catalog_name = catalog_name_A,
            };
            ret = engine.list_datasets(cmd);
            ASSERT_EQ(0, ret) << "Failed to list datasets: " << ret.message();
            auto pos = ret.message().find(dataset_name_1);
            ASSERT_NE(std::string::npos, pos) << "Dataset d111 not found in list";
            pos = ret.message().find(dataset_name_2);
            ASSERT_NE(std::string::npos, pos) << "Dataset d222 not found in list";
        }

        {
            CmdShowDataset cmd {
                .catalog_name = catalog_name_A,
                .dataset_name = dataset_name_1,
            };
            ret = engine.show_dataset(cmd);
            ASSERT_EQ(0, ret) << "Failed to show a dataset: " << ret.message();
            auto pos = ret.message().find("Type: f32");
            ASSERT_NE(std::string::npos, pos) << "Not found: TYPE=f32";
            pos = ret.message().find("Dim: 768");
            ASSERT_NE(std::string::npos, pos) << "Not found: DIM=768";
            pos = ret.message().find("Nodes: 16");
            ASSERT_NE(std::string::npos, pos) << "Not found: NODES=16";
        }

        {
            CmdShowDataset cmd {
                .catalog_name = catalog_name_A,
                .dataset_name = dataset_name_2,
            };
            ret = engine.show_dataset(cmd);
            ASSERT_EQ(0, ret) << "Failed to show a dataset: " << ret.message();
            auto pos = ret.message().find("Type: f16");
            ASSERT_NE(std::string::npos, pos) << "Not found: TYPE=f16";
            pos = ret.message().find("Dim: 1536");
            ASSERT_NE(std::string::npos, pos) << "Not found: DIM=1536";
            pos = ret.message().find("Nodes: 4");
            ASSERT_NE(std::string::npos, pos) << "Not found: NODES=4";
        }

        {
            CmdDropDataset cmd {
                .catalog_name = catalog_name_A,
                .dataset_name = dataset_name_1,
            };
            ret = engine.drop_dataset(cmd);
            ASSERT_EQ(0, ret) << "Failed to drop a dataset: " << ret.message();
        }

        {
            CmdListDatasets cmd {
                .catalog_name = catalog_name_A,
            };
            ret = engine.list_datasets(cmd);
            ASSERT_EQ(0, ret) << "Failed to list datasets: " << ret.message();
            auto pos = ret.message().find(dataset_name_1);
            ASSERT_EQ(std::string::npos, pos) << "Dataset d111 is found in list";
            pos = ret.message().find(dataset_name_2);
            ASSERT_NE(std::string::npos, pos) << "Dataset d222 not found in list";
        }
    }

    std::filesystem::remove_all(cfg.data_path);
}


