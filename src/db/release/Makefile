all: libdb.a utest

CXXFLAGS += -O2
STATIC_LIBS += $(PROJECT_HOME)/lib/libdb.a \
               $(PROJECT_HOME)/lib/liblmdb.a               

%.o : ../%.cpp
	$(CXX) $(CXXFLAGS) $(INCS) $< -o $@ -MMD

libdb.a: $(OBJS)
	ar rcs $@ $(OBJS) && cp $@ $(PROJECT_HOME)/lib/.

utest: $(UTEST_MAIN) $(TEST_OBJS) $(STATIC_LIBS)
	$(CXX) $(CXXLFLAGS) $(INCS) -o $@ $^ $(STATIC_LIBS) $(LIBS)

test: utest
	./utest

clean:
	rm -f *.o
	rm -f *.d
	rm -f *.a
	rm -f utest

-include $(subst .cpp,.d,$(SOURCES))
-include $(subst .cpp,.d,$(TEST_SOURCES))

