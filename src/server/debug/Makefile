all: sketchd

CXXFLAGS += -g
STATIC_LIBS += -lreadline \
               $(PROJECT_HOME)/lib-dbg/liblmdb.a               

%.o : ../%.cpp
	$(CXX) $(CXXFLAGS) $(INCS) $< -o $@ -MMD

sketchd: $(OBJS) $(PROJECT_HOME)/lib-dbg/libdb.a
	$(CXX) $(CXXLFLAGS) $(INCS) -o $@ $^ $(STATIC_LIBS) $(LIBS)

test: sketchd
	./sketchd

clean:
	rm -f *.o
	rm -f *.d
	rm -f *.a
	rm -f sketchd

-include $(subst .cpp,.d,$(SOURCES))
-include $(subst .cpp,.d,$(TEST_SOURCES))

