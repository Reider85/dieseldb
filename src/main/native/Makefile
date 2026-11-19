CC=gcc
CFLAGS=-Wall -Wextra -fPIC -shared -I${JAVA_HOME}/include -I${JAVA_HOME}/include/linux
LDFLAGS=-shared

# Target library
TARGET=libdiesel_odirect.so

# Source files
SOURCES=diesel_odirect.c

# Object files
OBJECTS=$(SOURCES:.c=.o)

# Default target
all: $(TARGET)

# Compile source files
%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

# Link shared library
$(TARGET): $(OBJECTS)
	$(CC) $(LDFLAGS) -o $@ $(OBJECTS)

# Clean build artifacts
clean:
	rm -f $(OBJECTS) $(TARGET)

# Install to target directory
install: $(TARGET)
	cp $(TARGET) /usr/local/lib/

# Test compilation
test: $(TARGET)
	@echo "Library compiled successfully"

.PHONY: all clean install test