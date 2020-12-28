#ifndef PARQUET_FDW_COMMON_HPP
#define PARQUET_FDW_COMMON_HPP

#include <cstdarg>
#include <cstddef>

#include "arrow/api.h"

extern "C"
{
#include "postgres.h"
}

#define ERROR_STR_LEN 512

#define to_postgres_timestamp(tstype, i, ts)                    \
    switch ((tstype)->unit()) {                                 \
        case arrow::TimeUnit::SECOND:                           \
            ts = time_t_to_timestamptz((i)); break;             \
        case arrow::TimeUnit::MILLI:                            \
            ts = time_t_to_timestamptz((i) / 1000); break;      \
        case arrow::TimeUnit::MICRO:                            \
            ts = time_t_to_timestamptz((i) / 1000000); break;   \
        case arrow::TimeUnit::NANO:                             \
            ts = time_t_to_timestamptz((i) / 1000000000); break;\
        default:                                                \
            elog(ERROR, "Timestamp of unknown precision: %d",   \
                 (tstype)->unit());                             \
    }


struct Error : std::exception
{
    char text[ERROR_STR_LEN];

    Error(char const* fmt, ...) __attribute__((format(printf,2,3))) {
        va_list ap;
        va_start(ap, fmt);
        vsnprintf(text, sizeof text, fmt, ap);
        va_end(ap);
    }

    char const* what() const throw() { return text; }
};


void *exc_palloc(std::size_t size);
Oid to_postgres_type(int arrow_type);
Datum bytes_to_postgres_type(const char *bytes, arrow::DataType *arrow_type);
int get_arrow_list_elem_type(arrow::DataType *type);

#endif
