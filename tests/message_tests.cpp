#include "message.hpp"

#ifdef NDEBUG
#undef NDEBUG
#endif
#include <cassert>
#include <cstring>
#include <string>

int main() {
    Message original("hello\nworld");
    Message decoded;
    std::memcpy(decoded.header_data(), original.header_data(), Message::header_size);
    assert(decoded.decode_header());
    std::memcpy(decoded.body_data(), original.body_data(), original.body_size());
    assert(decoded.body() == "hello\nworld");
    Message largest(std::string(Message::max_body_size, 'x'));
    assert(largest.body_size() == Message::max_body_size);
    bool oversized_rejected = false;
    try { Message too_large(std::string(Message::max_body_size + 1, 'x')); }
    catch (const std::length_error&) { oversized_rejected = true; }
    assert(oversized_rejected);
    Message malformed;
    const unsigned char invalid_header[Message::header_size] = {0, 1, 0, 1};
    std::memcpy(malformed.header_data(), invalid_header, Message::header_size);
    assert(!malformed.decode_header());
}
