all: libdb.a utest

CXXFLAGS += -g
STATIC_LIBS += $(PROJECT_HOME)/lib-dbg/libdb.a \
               $(PROJECT_HOME)/lib-dbg/liblmdb.a               

%.o : ../%.cpp
	$(CXX) $(CXXFLAGS) $(INCS) $< -o $@ -MMD

libdb.a: $(OBJS)
	ar rcs $@ $(OBJS) && cp $@ $(PROJECT_HOME)/lib-dbg/.

utest: $(TEST_OBJS) $(STATIC_LIBS)
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

