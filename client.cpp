#include "message.hpp"

#include <boost/asio.hpp>
#include <boost/asio/posix/stream_descriptor.hpp>

#include <array>
#include <charconv>
#include <cctype>
#include <deque>
#include <iostream>
#include <iterator>
#include <memory>
#include <string>
#include <string_view>
#include <unistd.h>

namespace asio = boost::asio;
using asio::ip::tcp;

class Client final : public std::enable_shared_from_this<Client> {
public:
    explicit Client(asio::io_context& context)
        : socket_(context),
          input_(context),
          strand_(asio::make_strand(context)),
          input_buffer_(Message::max_body_size + 1) {
        input_.assign(STDIN_FILENO);
    }

    void connect(const tcp::resolver::results_type& endpoints) {
        auto self = shared_from_this();
        asio::async_connect(socket_, endpoints, asio::bind_executor(strand_,
            [self](boost::system::error_code error, const tcp::endpoint&) {
                if (error) { self->stop("Connection failed", error); return; }
                self->connected_ = true;
                self->read_header();
                self->read_input();
            }));
    }

    [[nodiscard]] int exit_status() const noexcept { return failed_ ? 1 : 0; }

private:
    void read_input() {
        auto self = shared_from_this();
        asio::async_read_until(input_, input_buffer_, '\n',
            asio::bind_executor(strand_, [self](boost::system::error_code error, std::size_t) {
                if (error) {
                    if (error == asio::error::eof && self->input_buffer_.size() > 0) {
                        std::istream input(&self->input_buffer_);
                        std::string trailing((std::istreambuf_iterator<char>(input)), {});
                        self->enqueue_input(trailing);
                    } else if (error == asio::error::not_found) {
                        self->input_buffer_.consume(self->input_buffer_.size());
                        std::cerr << "Message rejected: input line exceeds the 64 KiB protocol limit\n";
                        self->failed_ = true;
                    }
                    if (error != asio::error::operation_aborted) self->request_close();
                    return;
                }
                std::istream input(&self->input_buffer_);
                std::string line;
                std::getline(input, line);
                self->enqueue_input(line);
                self->read_input();
            }));
    }

    void enqueue_input(const std::string& line) {
        try {
            enqueue(std::make_shared<Message>(line));
        } catch (const std::length_error& exception) {
            std::cerr << "Message rejected: " << exception.what() << '\n';
            failed_ = true;
        }
    }

    void read_header() {
        auto self = shared_from_this();
        asio::async_read(socket_, asio::buffer(incoming_.header_data(), Message::header_size),
            asio::bind_executor(strand_, [self](boost::system::error_code error, std::size_t) {
                if (error || !self->incoming_.decode_header()) { self->stop("Read error", error); return; }
                self->read_body();
            }));
    }

    void read_body() {
        auto self = shared_from_this();
        asio::async_read(socket_, asio::buffer(incoming_.body_data(), incoming_.body_size()),
            asio::bind_executor(strand_, [self](boost::system::error_code error, std::size_t) {
                if (error) { self->stop("Read error", error); return; }
                self->print_message(self->incoming_.body());
                self->incoming_ = Message{};
                self->read_header();
            }));
    }

    void enqueue(std::shared_ptr<const Message> message) {
        if (stopped_ || close_requested_ || !connected_) return;
        if (outgoing_.size() >= max_queued_messages) {
            failed_ = true;
            std::cerr << "Client queue is full; closing connection.\n";
            request_close();
            return;
        }
        const bool write_in_progress = !outgoing_.empty();
        outgoing_.push_back(std::move(message));
        if (!write_in_progress) write_next();
    }

    void write_next() {
        const auto& message = outgoing_.front();
        auto self = shared_from_this();
        std::array<asio::const_buffer, 2> buffers{
            asio::buffer(message->header_data(), Message::header_size),
            asio::buffer(message->body_data(), message->body_size())};
        asio::async_write(socket_, buffers, asio::bind_executor(strand_,
            [self](boost::system::error_code error, std::size_t) {
                if (error) { self->stop("Write error", error); return; }
                self->outgoing_.pop_front();
                if (!self->outgoing_.empty()) self->write_next();
                else if (self->close_requested_) self->finish_close();
            }));
    }

    void request_close() {
        close_requested_ = true;
        if (outgoing_.empty()) finish_close();
    }

    void finish_close() {
        if (stopped_ || send_closed_) return;
        send_closed_ = true;
        boost::system::error_code ignored;
        input_.cancel(ignored);
        socket_.shutdown(tcp::socket::shutdown_send, ignored);
    }

    void stop(std::string_view action, boost::system::error_code error) {
        if (stopped_) return;
        stopped_ = true;
        boost::system::error_code ignored;
        input_.cancel(ignored);
        socket_.close(ignored);
        const bool clean_half_close = send_closed_ && error == asio::error::eof;
        failed_ = !clean_half_close;
        if (!clean_half_close) {
            if (error) std::cerr << action << ": " << error.message() << '\n';
            else std::cerr << action << "\n";
        }
    }

    static void print_message(std::string_view body) {
        static constexpr char hex[] = "0123456789ABCDEF";
        std::cout << "Message: ";
        for (const unsigned char character : body) {
            if ((character >= 0x20 && character != 0x7F) && character != '\\') {
                std::cout << static_cast<char>(character);
            } else if (character == '\\') std::cout << "\\\\";
            else {
                std::cout << "\\x" << hex[character >> 4] << hex[character & 0x0F];
            }
        }
        std::cout << '\n';
    }

    tcp::socket socket_;
    asio::posix::stream_descriptor input_;
    asio::strand<asio::any_io_executor> strand_;
    asio::streambuf input_buffer_;
    Message incoming_;
    std::deque<std::shared_ptr<const Message>> outgoing_;
    static constexpr std::size_t max_queued_messages = 128;
    bool connected_ = false;
    bool close_requested_ = false;
    bool stopped_ = false;
    bool send_closed_ = false;
    bool failed_ = false;
};

int main(int argc, char* argv[]) {
    if (argc != 2 && argc != 3) {
        std::cerr << "Usage: clientApp [host] <port>\n";
        return 1;
    }
    const std::string_view host = argc == 3 ? argv[1] : "127.0.0.1";
    const char* port_text = argc == 3 ? argv[2] : argv[1];
    unsigned short port{};
    const auto [end, error] = std::from_chars(port_text, port_text + std::char_traits<char>::length(port_text), port);
    if (error != std::errc{} || *end != '\0' || port == 0) {
        std::cerr << "Port must be an integer from 1 to 65535.\n";
        return 1;
    }
    try {
        asio::io_context context;
        auto client = std::make_shared<Client>(context);
        tcp::resolver resolver(context);
        client->connect(resolver.resolve(std::string(host), std::to_string(port)));
        context.run();
        return client->exit_status();
    } catch (const std::exception& exception) {
        std::cerr << "Client error: " << exception.what() << '\n';
        return 1;
    }
}
