#include "input_data.h"
#include "string_utils.h"
#include "log.h"
#include "gtest/gtest.h"
#include <experimental/scope>

using namespace sketch;

TEST(TEST_DATA, Basics) {
    //TempLogLevel temp_level(LL_DEBUG);

    const char* test_str =
        "tag1 : data1 line1\n"
        "tag2 : data2 line2\n"
        "tag3 : data3 line3\n";

    InputData input_data;
    ASSERT_EQ(0, input_data.init(test_str, strlen(test_str)));
    ASSERT_EQ(3, input_data.count());

    {
        auto item_opt = input_data.get(0);
        ASSERT_TRUE(item_opt.has_value());
        const auto& item = item_opt.value();
        ASSERT_EQ(std::string_view("tag1 "), item.tag);
        ASSERT_EQ(std::string_view(" data1 line1\n"), item.data);
    }

    {
        auto item_opt = input_data.get(1);
        ASSERT_TRUE(item_opt.has_value());
        auto item = item_opt.value();
        ASSERT_EQ(std::string_view("tag2 "), item.tag);
        ASSERT_EQ(std::string_view(" data2 line2\n"), item.data);
    }

    {
        auto item_opt = input_data.get(2);
        ASSERT_TRUE(item_opt.has_value());
        auto item = item_opt.value();
        ASSERT_EQ(std::string_view("tag3 "), item.tag);
        ASSERT_EQ(std::string_view(" data3 line3\n"), item.data);
    }

    {
        auto item_opt = input_data.get(3);
        ASSERT_FALSE(item_opt.has_value());
    }
}

TEST(TEST_DATA, FullCycle) {
    //TempLogLevel temp_level(LL_DEBUG);

    const std::string path = "/tmp/input_data.txt";
    std::experimental::scope_exit closer([&] {
        //unlink(path.c_str());
    });

    int ret = InputDataGenerator::generate(path, 3, 15);
    ASSERT_EQ(0, ret) << "Failed to generate test data";

    InputData input_data;
    ASSERT_EQ(0, input_data.init(path));
    ASSERT_EQ(15, input_data.count());

    {
        auto item_opt = input_data.get(1);
        ASSERT_TRUE(item_opt.has_value());
        const auto& item = item_opt.value();
        ASSERT_EQ(std::string_view("1 "), item.tag);
        ASSERT_EQ(std::string_view(" [ 1.1, 1.1, 1.1 ]\n"), item.data);
    }

    {
        auto item_opt = input_data.get(5);
        ASSERT_TRUE(item_opt.has_value());
        const auto& item = item_opt.value();
        ASSERT_EQ(std::string_view("5 "), item.tag);
        ASSERT_EQ(std::string_view(" [ 5.1, 5.1, 5.1 ]\n"), item.data);
    }
}

TEST(TEST_DATA, FullCycleFloat) {
    //TempLogLevel temp_level(LL_DEBUG);

    const std::string path = "/tmp/input_data.txt";
    std::experimental::scope_exit closer([&] {
//        unlink(path.c_str());
    });

    int ret = InputDataGenerator::generate(path, 3, 120);
    ASSERT_EQ(0, ret) << "Failed to generate test data";

    InputData input_data;
    ASSERT_EQ(0, input_data.init(path));
    ASSERT_EQ(120, input_data.count());

    {
        auto item_opt = input_data.get(1);
        ASSERT_TRUE(item_opt.has_value());
        const auto& item = item_opt.value();
        ASSERT_EQ(std::string_view("1 "), item.tag);
        ASSERT_EQ(std::string_view(" [ 1.1, 1.1, 1.1 ]\n"), item.data);
    }

    {
        auto item_opt = input_data.get(20);
        ASSERT_TRUE(item_opt.has_value());
        const auto& item = item_opt.value();
        ASSERT_EQ(std::string_view("20 "), item.tag);
        ASSERT_EQ(std::string_view(" [ 20.1, 20.1, 20.1 ]\n"), item.data);
    }
}

TEST(TEST_DATA, FullCycleStart) {
    //TempLogLevel temp_level(LL_DEBUG);

    const std::string path = "/tmp/input_data.txt";
    std::experimental::scope_exit closer([&] {
        unlink(path.c_str());
    });

    int ret = InputDataGenerator::generate(path, 3, 15, 10);
    ASSERT_EQ(0, ret) << "Failed to generate test data";

    InputData input_data;
    ASSERT_EQ(0, input_data.init(path));
    ASSERT_EQ(15, input_data.count());

    {
        auto item_opt = input_data.get(1);
        ASSERT_TRUE(item_opt.has_value());
        const auto& item = item_opt.value();
        ASSERT_EQ(std::string_view("11 "), item.tag);
        ASSERT_EQ(std::string_view(" [ 11.1, 11.1, 11.1 ]\n"), item.data);
    }

    {
        auto item_opt = input_data.get(5);
        ASSERT_TRUE(item_opt.has_value());
        const auto& item = item_opt.value();
        ASSERT_EQ(std::string_view("15 "), item.tag);
        ASSERT_EQ(std::string_view(" [ 15.1, 15.1, 15.1 ]\n"), item.data);
    }
}

TEST(TEST_DATA, ParseVector) {
    {
        const std::string_view str = "1.1, 2.2, 3.3\n";
        std::vector<uint8_t> vec(3 * sizeof(float));
        int ret = convert_vector_f32(str, vec);
        ASSERT_EQ(0, ret);
        for (size_t i = 0; i < 3; i++) {
            size_t index = i * sizeof(float);
            float value = *reinterpret_cast<float*>(vec.data() + index);
            switch(i) {
                case 0: ASSERT_FLOAT_EQ(1.1, value); break;
                case 1: ASSERT_FLOAT_EQ(2.2, value); break;
                case 2: ASSERT_FLOAT_EQ(3.3, value); break;
            }
        }
    }
}