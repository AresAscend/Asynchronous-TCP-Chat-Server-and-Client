#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include <boost/endian/conversion.hpp>

// A wire message is a four-byte unsigned body length in network byte order,
// followed by that many body bytes. TCP packet boundaries are irrelevant.
class Message {
public:
    static constexpr std::size_t header_size = sizeof(std::uint32_t);
    static constexpr std::size_t max_body_size = 64 * 1024;

    Message() = default;
    explicit Message(std::string_view body) { set_body(body); }

    void set_body(std::string_view body) {
        if (body.size() > max_body_size) {
            throw std::length_error("message exceeds the 64 KiB protocol limit");
        }
        body_.assign(body.begin(), body.end());
        encode_header();
    }

    [[nodiscard]] char* header_data() noexcept { return header_.data(); }
    [[nodiscard]] const char* header_data() const noexcept { return header_.data(); }
    [[nodiscard]] char* body_data() noexcept { return body_.data(); }
    [[nodiscard]] const char* body_data() const noexcept { return body_.data(); }
    [[nodiscard]] std::size_t body_size() const noexcept { return body_.size(); }
    [[nodiscard]] std::string_view body() const noexcept { return {body_.data(), body_.size()}; }

    // Call after receiving a header. Never allocate an unbounded peer-provided size.
    [[nodiscard]] bool decode_header() {
        std::uint32_t network_length{};
        std::memcpy(&network_length, header_.data(), header_size);
        const auto length = static_cast<std::size_t>(boost::endian::big_to_native(network_length));
        if (length > max_body_size) {
            return false;
        }
        body_.resize(length);
        return true;
    }

private:
    void encode_header() noexcept {
        const auto network_length = boost::endian::native_to_big(static_cast<std::uint32_t>(body_.size()));
        std::memcpy(header_.data(), &network_length, header_size);
    }

    std::array<char, header_size> header_{};
    std::vector<char> body_;
};
