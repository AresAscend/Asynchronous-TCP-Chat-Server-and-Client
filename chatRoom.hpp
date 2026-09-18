#pragma once

#include "message.hpp"

#include <boost/asio.hpp>

#include <deque>
#include <memory>
#include <set>

class Participant {
public:
    virtual ~Participant() = default;
    virtual void deliver(std::shared_ptr<const Message> message) = 0;
    virtual void close() = 0;
};

using ParticipantPointer = std::shared_ptr<Participant>;

// Room state is confined to its strand, making join/leave/broadcast safe when
// io_context is serviced by several worker threads.
class Room {
public:
    explicit Room(boost::asio::any_io_executor executor);
    void join(ParticipantPointer participant);
    void leave(ParticipantPointer participant);
    void deliver(ParticipantPointer sender, std::shared_ptr<const Message> message);
    void close_all();

private:
    boost::asio::strand<boost::asio::any_io_executor> strand_;
    std::set<ParticipantPointer, std::owner_less<ParticipantPointer>> participants_;
};

class Session final : public Participant, public std::enable_shared_from_this<Session> {
public:
    Session(boost::asio::ip::tcp::socket socket, Room& room);
    void start();
    void deliver(std::shared_ptr<const Message> message) override;
    void close() override;

private:
    void read_header();
    void read_body();
    void enqueue_write(std::shared_ptr<const Message> message);
    void write_next();
    void stop();

    static constexpr std::size_t max_queued_messages = 128;
    boost::asio::ip::tcp::socket socket_;
    Room& room_;
    boost::asio::strand<boost::asio::any_io_executor> strand_;
    Message incoming_;
    std::deque<std::shared_ptr<const Message>> outgoing_;
    bool stopped_ = false;
};
