#pragma once
#include "shared_types.h"
#include <string>
#include <string_view>

namespace sketch {

#define PARAM_CONV(var, str) \
    try { \
        var = u64_from_string_view(str); \
    } catch (const std::exception& e) { \
        return std::format("Failed to parse {} parameter: {}", #var,str); \
    }

#define PARAMS(var, str) \
    uint64_t var = 0; \
    PARAM_CONV(var, str)

#define PARAM(num,var)  PARAMS(var, commands[num])


char* trim_inplace(char* s);
void to_lowercase(std::string& s);
bool is_valid_identifier(const std::string_view& s);
void split_string(const std::string_view& s, char delimiter, std::vector<std::string_view>& tokens);
void parse_command(const std::string& line, Commands& commands);
uint64_t u64_from_string_view(const std::string_view& str);

int convert_vector_f32(const std::string_view& str, std::vector<uint8_t>& vec);
int convert_vector_f16(const std::string_view& str, std::vector<uint8_t>& vec);

int convert_ptr_f32(const std::string_view& str, uint8_t* ptr, size_t count, bool& is_empty);
int convert_ptr_f16(const std::string_view& str, uint8_t* ptr, size_t count, bool& is_empty);

const char* findchr(const char* start, char ch, size_t size);
size_t findchrpos(const char* start, char ch, size_t size);

} // namespace sketch