all: scli

CXXFLAGS += -g -O2
STATIC_LIBS += -lreadline \
               $(PROJECT_HOME)/lib/liblmdb.a               

%.o : ../%.cpp
	$(CXX) $(CXXFLAGS) $(INCS) $< -o $@ -MMD

scli: $(OBJS) $(PROJECT_HOME)/lib/libdb.a
	$(CXX) $(CXXLFLAGS) $(INCS) -o $@ $^ $(STATIC_LIBS) $(LIBS)

test: scli
	./scli

clean:
	rm -f *.o
	rm -f *.d
	rm -f *.a
	rm -f scli

-include $(subst .cpp,.d,$(SOURCES))
-include $(subst .cpp,.d,$(TEST_SOURCES))

