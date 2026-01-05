all: debug

CXXFLAGS += -Wall -Wextra -Werror -std=c++20

.PHONY: test
test:
	$(PROJECT_HOME)/script/test.sh

.PHONY: deploy
deploy:
	$(PROJECT_HOME)/script/deploy.sh

#.PHONY: utest
#utest:
#	$(MAKE) -C net test

.PHONY: prepare
prepare:

.PHONY: release
release:
	cp $(PROJECT_HOME)/third-party/include/* $(PROJECT_HOME)/include
	cp $(PROJECT_HOME)/third-party/lib/* $(PROJECT_HOME)/lib
	$(MAKE) -C db release_bld
	$(MAKE) -C client release_bld
	$(MAKE) -C server release_bld
	$(MAKE) -C tester release_bld

.PHONY: debug
debug:
	cp $(PROJECT_HOME)/third-party/include/* $(PROJECT_HOME)/include
	cp $(PROJECT_HOME)/third-party/lib/* $(PROJECT_HOME)/lib-dbg
	$(MAKE) -C db debug_bld
	$(MAKE) -C client debug_bld
	$(MAKE) -C server debug_bld
	$(MAKE) -C tester debug_bld

.PHONY: clean
clean:
	$(MAKE) -C db clean
	$(MAKE) -C client clean
	$(MAKE) -C server clean
	$(MAKE) -C tester clean
	rm -f $(PROJECT_HOME)/bin/* $(PROJECT_HOME)/bin-dbg/* $(PROJECT_HOME)/lib/* $(PROJECT_HOME)/lib-dbg/*


