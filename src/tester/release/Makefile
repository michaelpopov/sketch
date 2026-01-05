all: tester

CXXFLAGS += -g -O2
STATIC_LIBS += -lreadline \
               $(PROJECT_HOME)/lib/liblmdb.a               

%.o : ../%.cpp
	$(CXX) $(CXXFLAGS) $(INCS) $< -o $@ -MMD

tester: $(OBJS) $(PROJECT_HOME)/lib/libdb.a
	$(CXX) $(CXXLFLAGS) $(INCS) -o $@ $^ $(STATIC_LIBS) $(LIBS)  && cp $@ $(PROJECT_HOME)/bin/.

test: tester
	./tester

clean:
	rm -f *.o
	rm -f *.d
	rm -f *.a
	rm -f tester

-include $(subst .cpp,.d,$(SOURCES))
-include $(subst .cpp,.d,$(TEST_SOURCES))

