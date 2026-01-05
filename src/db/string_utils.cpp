#include "string_utils.h"
#include <charconv>
#include <cstring>
#include <iostream>

namespace sketch {

char* trim_inplace(char* s) {
    if (!s) return s;

    while (*s && isspace(*s)) ++s;
    if (*s == 0) return s;

    char* end = s + strlen(s) - 1;
    while (end > s && isspace(*end)) {
        *end = '\0';
        --end;
    }

    return s;
}

void to_lowercase(std::string& s) {
    for (char& c : s) {
        c = static_cast<char>(tolower(c));
    }
}

bool is_valid_identifier(const std::string_view& s) {
    if (s.empty() || !(isalpha(s[0]) || s[0] == '_')) {
        return false;
    }
    for (char c : s) {
        if (!(isalnum(c) || c == '_')) {
            return false;
        }
    }
    return true;
}

void split_string(const std::string_view& s, char delimiter, std::vector<std::string_view>& tokens) {
    tokens.clear();
    size_t start = 0;
    size_t end = s.find(delimiter);
    while (end != std::string::npos) {
        tokens.push_back(s.substr(start, end - start));
        start = end + 1;
        end = s.find(delimiter, start);
    }
    tokens.push_back(s.substr(start));
}

void parse_command(const std::string& line, Commands& commands) {
    const std::string whitespace = " \t\n\r";
    const std::string special_chars = "();,=";

    size_t current_pos = 0;
    while (current_pos < line.length()) {
        // Find the beginning of the next token (skip whitespace)
        size_t token_start = line.find_first_not_of(whitespace, current_pos);
        if (token_start == std::string::npos) {
            break; // No more tokens
        }

        // Check if the token is a special character
        if (special_chars.find(line[token_start]) != std::string::npos) {
            size_t len = 1;
            const char* start = line.data() + token_start;
            commands.push_back(std::string_view(start, start+len));
            current_pos = token_start + 1;
            continue;
        }

        // It's a word. Find its end.
        size_t token_end = line.find_first_of(whitespace + special_chars, token_start);
        if (token_end == std::string::npos) {
            token_end = line.length();
        }
        size_t len = token_end - token_start;
        const char* start = line.data() + token_start;
        commands.push_back(std::string_view(start, start+len));
        current_pos = (token_end == std::string::npos) ? line.length() : token_end;
    }
}

uint64_t u64_from_string_view(const std::string_view& str) {
    uint64_t value = 0;
    const auto result = std::from_chars(str.data(), str.data() + str.size(), value);
    if (result.ec != std::errc{}) {
        throw std::exception{};
    }

    return value;
}

template <typename T>
static int convert_vector(const std::string_view& str, std::vector<uint8_t>& vec) {
    assert(vec.size() % sizeof(T) == 0);
    size_t count = vec.size() / sizeof(T);
    size_t offset = 0;

    for (size_t i = 0; i < count; i++) {
        size_t index = i * sizeof(T);
        T* value = reinterpret_cast<T*>(vec.data() + index);

        std::string_view current_view(str.data()+offset, str.size()-offset);
        const auto delim_offset = findchrpos(current_view.data(), ',', current_view.size());

        {
            size_t len = 0;
            if (delim_offset == std::string::npos) {
                if (i != count - 1) {
                    return -1;
                }
                len = current_view.size();
            } else {
                len = delim_offset;
            }

            const char* ptr = current_view.data();
            while (!isalnum(*ptr) && len > 0) {
                ptr++;
                len--;
            }

            const auto result = std::from_chars(ptr, ptr + len, *value);
            if (result.ec != std::errc{}) {
                return -1;
            }
        }

        offset += delim_offset + 1;
    }

    return 0;
}

int convert_vector_f32(const std::string_view& str, std::vector<uint8_t>& vec) {
    return convert_vector<float>(str, vec);
}

int convert_vector_f16(const std::string_view& str, std::vector<uint8_t>& vec) {
    return convert_vector<float16_t>(str, vec);
}

template <typename T>
static int convert_ptr(const std::string_view& str, uint8_t* ptr, size_t count, bool& is_empty) {
    size_t offset = 0;
    const auto open_bracket_offset = findchrpos(str.data(), '[', str.size());
    if (open_bracket_offset == std::string::npos) {
        return -1;
    }

    offset = open_bracket_offset + 1;
    if (offset < str.size() && str[offset] == ']') {
        is_empty = true;
        return 0;
    }

    is_empty = false;

    for (size_t i = 0; i < count; i++) {
        size_t index = i * sizeof(T);
        T* value = reinterpret_cast<T*>(ptr + index);

        std::string_view current_view(str.data()+offset, str.size()-offset);
        const auto delim_offset = (i + 1 != count) ? findchrpos(current_view.data(), ',', current_view.size()) :
            findchrpos(current_view.data(), ']', current_view.size());

        if (delim_offset == std::string::npos) {
            return -1;
        }

        {
            size_t len = delim_offset;
            const char* ptr = current_view.data();
            while (!isalnum(*ptr) && len > 0) {
                ptr++;
                len--;
            }

            const auto result = std::from_chars(ptr, ptr + len, *value);
            if (result.ec != std::errc{}) {
                return -1;
            }
        }

        offset += delim_offset + 1;
    }

    return 0;
}

int convert_ptr_f32(const std::string_view& str, uint8_t* ptr, size_t count, bool&is_empty) {
    return convert_ptr<float>(str, ptr, count, is_empty);
}

int convert_ptr_f16(const std::string_view& str, uint8_t* ptr, size_t count, bool& is_empty) {
    return convert_ptr<float16_t>(str, ptr, count, is_empty);
}

const char* findchr(const char* start, char ch, size_t size) {
    for (size_t i = 0; i < size; i++) {
        if (start[i] == ch) {
            return start + i;
        }
    }
    return nullptr;
}

size_t findchrpos(const char* start, char ch, size_t size) {
    for (size_t i = 0; i < size; i++) {
        if (start[i] == ch) {
            return i;
        }
    }
    return std::string::npos;
}


} // namespace sketch