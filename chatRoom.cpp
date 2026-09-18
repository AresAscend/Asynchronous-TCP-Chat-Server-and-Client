#include "chatRoom.hpp"

#include <algorithm>
#include <array>
#include <charconv>
#include <chrono>
#include <csignal>
#include <iostream>
#include <thread>
#include <vector>

namespace asio = boost::asio;
using asio::ip::tcp;
using namespace std::chrono_literals;

Room::Room(asio::any_io_executor executor) : strand_(asio::make_strand(executor)) {}

void Room::join(ParticipantPointer participant) {
    asio::post(strand_, [this, participant = std::move(participant)] { participants_.insert(participant); });
}

void Room::leave(ParticipantPointer participant) {
    asio::post(strand_, [this, participant = std::move(participant)] { participants_.erase(participant); });
}

void Room::deliver(ParticipantPointer sender, std::shared_ptr<const Message> message) {
    asio::post(strand_, [this, sender = std::move(sender), message = std::move(message)] {
        for (const auto& participant : participants_) {
            if (participant != sender) participant->deliver(message);
        }
    });
}

void Room::close_all() {
    asio::post(strand_, [this] {
        for (const auto& participant : participants_) participant->close();
    });
}

Session::Session(tcp::socket socket, Room& room)
    : socket_(std::move(socket)),
      room_(room),
      strand_(asio::make_strand(socket_.get_executor())) {
    boost::system::error_code ignored;
    socket_.set_option(tcp::socket::keep_alive(true), ignored);
}

void Session::start() {
    auto self = shared_from_this();
    asio::dispatch(strand_, [self] { self->room_.join(self); self->read_header(); });
}

void Session::deliver(std::shared_ptr<const Message> message) {
    auto self = shared_from_this();
    asio::post(strand_, [self, message = std::move(message)] { self->enqueue_write(message); });
}

void Session::close() {
    auto self = shared_from_this();
    asio::post(strand_, [self] { self->stop(); });
}

void Session::read_header() {
    auto self = shared_from_this();
    asio::async_read(socket_, asio::buffer(incoming_.header_data(), Message::header_size),
        asio::bind_executor(strand_, [self](boost::system::error_code error, std::size_t) {
            if (error || !self->incoming_.decode_header()) { self->stop(); return; }
            self->read_body();
        }));
}

void Session::read_body() {
    auto self = shared_from_this();
    asio::async_read(socket_, asio::buffer(incoming_.body_data(), incoming_.body_size()),
        asio::bind_executor(strand_, [self](boost::system::error_code error, std::size_t) {
            if (error) { self->stop(); return; }
            self->room_.deliver(self, std::make_shared<Message>(std::move(self->incoming_)));
            self->incoming_ = Message{};
            self->read_header();
        }));
}

void Session::enqueue_write(std::shared_ptr<const Message> message) {
    if (stopped_) return;
    if (outgoing_.size() >= max_queued_messages) {
        // A non-reading peer must not be able to consume unlimited server memory.
        stop();
        return;
    }
    const bool write_in_progress = !outgoing_.empty();
    outgoing_.push_back(std::move(message));
    if (!write_in_progress) write_next();
}

void Session::write_next() {
    const auto& message = outgoing_.front();
    auto self = shared_from_this();
    std::array<asio::const_buffer, 2> buffers{
        asio::buffer(message->header_data(), Message::header_size),
        asio::buffer(message->body_data(), message->body_size())};
    asio::async_write(socket_, buffers,
        asio::bind_executor(strand_, [self](boost::system::error_code error, std::size_t) {
            if (error) { self->stop(); return; }
            self->outgoing_.pop_front();
            if (!self->outgoing_.empty()) self->write_next();
        }));
}

void Session::stop() {
    if (stopped_) return;
    stopped_ = true;
    boost::system::error_code ignored;
    socket_.shutdown(tcp::socket::shutdown_both, ignored);
    socket_.close(ignored);
    room_.leave(shared_from_this());
}

class Server {
public:
    Server(asio::io_context& context, unsigned short port)
        : lifecycle_(asio::make_strand(context)),
          acceptor_(context, {tcp::v4(), port}),
          retry_timer_(context),
          signals_(context, SIGINT, SIGTERM),
          room_(context.get_executor()) {
        asio::dispatch(lifecycle_, [this] { accept(); });
        wait_for_shutdown_signal();
    }
    [[nodiscard]] unsigned short port() const {
        return acceptor_.local_endpoint().port();
    }
private:
    void wait_for_shutdown_signal() {
        signals_.async_wait(asio::bind_executor(lifecycle_, [this](boost::system::error_code error, int) {
            if (error) return;
            boost::system::error_code ignored;
            acceptor_.close(ignored);
            retry_timer_.cancel();
            signals_.cancel(ignored);
            room_.close_all();
        }));
    }

    void accept() {
        acceptor_.async_accept(asio::bind_executor(lifecycle_, [this](boost::system::error_code error, tcp::socket socket) {
            if (!error) std::make_shared<Session>(std::move(socket), room_)->start();
            if (error == asio::error::operation_aborted || !acceptor_.is_open()) return;
            if (error) {
                // Back off on descriptor exhaustion or transient OS failures.
                retry_timer_.expires_after(100ms);
                retry_timer_.async_wait(asio::bind_executor(lifecycle_, [this](boost::system::error_code timer_error) {
                    if (!timer_error && acceptor_.is_open()) accept();
                }));
                return;
            }
            accept();
        }));
    }
    asio::strand<asio::any_io_executor> lifecycle_;
    tcp::acceptor acceptor_;
    asio::steady_timer retry_timer_;
    asio::signal_set signals_;
    Room room_;
};

int main(int argc, char* argv[]) {
    if (argc < 2 || argc > 3) { std::cerr << "Usage: chatApp <port> [worker-threads]\n"; return 1; }
    unsigned short port{};
    const auto [port_end, port_error] = std::from_chars(argv[1], argv[1] + std::char_traits<char>::length(argv[1]), port);
    if (port_error != std::errc{} || *port_end != '\0') { std::cerr << "Port must be an integer from 0 to 65535.\n"; return 1; }
    unsigned int workers = std::max(1u, std::thread::hardware_concurrency());
    if (argc == 3) {
        const auto [end, error] = std::from_chars(argv[2], argv[2] + std::char_traits<char>::length(argv[2]), workers);
        if (error != std::errc{} || *end != '\0' || workers == 0) { std::cerr << "Worker count must be positive.\n"; return 1; }
    }
    try {
        asio::io_context context;
        Server server(context, port);
        std::cout << "Listening on port " << server.port() << std::endl;
        std::vector<std::jthread> pool;
        pool.reserve(workers);
        for (unsigned int worker = 0; worker < workers; ++worker) pool.emplace_back([&context] { context.run(); });
    } catch (const std::exception& exception) {
        std::cerr << "Server error: " << exception.what() << '\n'; return 1;
    }
}
