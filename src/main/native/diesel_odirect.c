#include "diesel_odirect.h"
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <errno.h>
#include <string.h>

#define DIESEL_CHECK_ALIGN(value, align) (((value) & ((align) - 1)) == 0)

diesel_result_t diesel_odirect_open(const char* path, int page_size, diesel_fd_t* out_fd) {
    // Validate parameters
    if (path == NULL || out_fd == NULL) {
        return EINVAL;
    }
    
    // Validate page size alignment (must be power of 2 and at least 512 bytes)
    if (page_size <= 0 || !DIESEL_CHECK_ALIGN(page_size, 512)) {
        return EINVAL;
    }
    
    // Open file with O_DIRECT flag
    int fd = open(path, O_RDWR | O_CREAT | O_DIRECT, 0666);
    if (fd == -1) {
        return errno;
    }
    
    *out_fd = fd;
    return 0;
}

diesel_result_t diesel_odirect_close(diesel_fd_t fd) {
    if (fd < 0) {
        return EINVAL;
    }
    
    if (close(fd) == -1) {
        return errno;
    }
    return 0;
}

diesel_result_t diesel_odirect_read(diesel_fd_t fd, int64_t offset, void* buffer, int size, int* bytes_read) {
    // Validate parameters
    if (fd < 0 || buffer == NULL || bytes_read == NULL || size <= 0) {
        return EINVAL;
    }
    
    // Check alignment requirements
    if (!DIESEL_CHECK_ALIGN(offset, 512) || !DIESEL_CHECK_ALIGN((uintptr_t)buffer, 512) || !DIESEL_CHECK_ALIGN(size, 512)) {
        return EINVAL; // Alignment error
    }
    
    ssize_t result = pread(fd, buffer, size, offset);
    if (result == -1) {
        return errno;
    }
    
    *bytes_read = (int)result;
    return 0;
}

diesel_result_t diesel_odirect_write(diesel_fd_t fd, int64_t offset, const void* buffer, int size, int* bytes_written) {
    // Validate parameters
    if (fd < 0 || buffer == NULL || bytes_written == NULL || size <= 0) {
        return EINVAL;
    }
    
    // Check alignment requirements
    if (!DIESEL_CHECK_ALIGN(offset, 512) || !DIESEL_CHECK_ALIGN((uintptr_t)buffer, 512) || !DIESEL_CHECK_ALIGN(size, 512)) {
        return EINVAL; // Alignment error
    }
    
    ssize_t result = pwrite(fd, buffer, size, offset);
    if (result == -1) {
        return errno;
    }
    
    *bytes_written = (int)result;
    return 0;
}

diesel_result_t diesel_odirect_sync(diesel_fd_t fd) {
    if (fd < 0) {
        return EINVAL;
    }
    
    if (fsync(fd) == -1) {
        return errno;
    }
    return 0;
}

diesel_result_t diesel_odirect_set_size(diesel_fd_t fd, int64_t size) {
    if (fd < 0) {
        return EINVAL;
    }
    
    if (ftruncate(fd, size) == -1) {
        return errno;
    }
    return 0;
}