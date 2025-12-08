CFLAGS := -c -Wall -Weffc++ -g -std=c++17 -Iinclude
LDFLAGS := -lboost_system -lpthread

# Source files excluding echoClient.cpp
SRC_FILES := $(filter-out src/echoClient.cpp, $(wildcard src/*.cpp))
OBJ_FILES := $(patsubst src/%.cpp,bin/%.o,$(SRC_FILES))

all: StompEMIClient

$(shell mkdir -p bin)

StompEMIClient: $(OBJ_FILES)
	@echo "Linking object files..."
	g++ -o bin/StompEMIClient $(OBJ_FILES) $(LDFLAGS)

bin/%.o: src/%.cpp
	@echo "Compiling $<..."
	g++ $(CFLAGS) -o $@ $<

.PHONY: clean
clean:
	rm -f bin/*
