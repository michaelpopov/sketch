all: debug_bld

CXXFLAGS += -c -Wall -Wextra -Werror -std=c++20
INCS := -I$(PROJECT_HOME)/src -I$(PROJECT_HOME)/include

SOURCES := config.cpp command_router.cpp ddl_command_processor.cpp \
           data_command_processor.cpp engine.cpp string_utils.cpp core.cpp \
		   storage.cpp input_data.cpp dataset_node.cpp dataset.cpp \
		   catalog.cpp ivf_builder.cpp lmdb2.cpp centroids.cpp dataset_ivf.cpp \
		   dataset_node_ivf.cpp
OBJS := $(subst .cpp,.o,$(SOURCES))

TEST_SOURCES := utest_main.cpp utest_storage.cpp utest_thread_pool.cpp utest_ddl.cpp \
				utest_test_data.cpp utest_dml.cpp utest_lmdb.cpp utest_math.cpp
TEST_OBJS := $(subst .cpp,.o,$(TEST_SOURCES))

LIBS :=  -lgtest -lpthread
STATIC_LIBS := 

export

.PHONY: release_bld
release_bld:
	$(MAKE) -C release

.PHONY: debug_bld
debug_bld:
	$(MAKE) -C debug

.PHONY: test
test:
	$(MAKE) -C debug test

PHONY: clean
clean:
	$(MAKE) -C debug clean
	$(MAKE) -C release clean

