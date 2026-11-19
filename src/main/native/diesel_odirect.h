#ifndef DIESEL_ODIRECT_H
#define DIESEL_ODIRECT_H

#include <stdint.h>
#include <unistd.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef int64_t diesel_fd_t;
typedef int64_t diesel_result_t;

// O_DIRECT file operations
diesel_result_t diesel_odirect_open(const char* path, int page_size, diesel_fd_t* out_fd);
diesel_result_t diesel_odirect_close(diesel_fd_t fd);
diesel_result_t diesel_odirect_read(diesel_fd_t fd, int64_t offset, void* buffer, int size, int* bytes_read);
diesel_result_t diesel_odirect_write(diesel_fd_t fd, int64_t offset, const void* buffer, int size, int* bytes_written);
diesel_result_t diesel_odirect_sync(diesel_fd_t fd);
diesel_result_t diesel_odirect_set_size(diesel_fd_t fd, int64_t size);

#ifdef __cplusplus
}
#endif

#endif // DIESEL_ODIRECT_H