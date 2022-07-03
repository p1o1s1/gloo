/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "gloo/transport/tcp/pair.h"

#include <array>
#include <algorithm>
#include <sstream>
#include <iostream>

#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include "gloo/common/error.h"
#include "gloo/common/logging.h"
#include "gloo/transport/tcp/buffer.h"
#include "gloo/transport/tcp/context.h"
#include "gloo/transport/tcp/unbound_buffer.h"

#define FD_INVALID (-1)
#define MAXEPOLLSIZE (100)
#define MAXBUFFERSIZE (1024)

namespace gloo {
namespace transport {
namespace tcp {

namespace {

// This reflects an approximation of /proc/sys/net/core/{r,w}mem_max.
// It is hard coded because making buffers larger than this would not
// have much impact. Also see socket(7).
constexpr size_t kMaxSendBufferSize = 32 * 1024 * 1024;
constexpr size_t kMaxRecvBufferSize = 32 * 1024 * 1024;

} // namespace

Pair::Pair(
    Context* context,
    Device* device,
    int rank,
    std::chrono::milliseconds timeout)
    : context_(context),
      device_(device),
      rank_(rank),
      state_(INITIALIZING),
      sync_(false),
      timeout_(timeout),
      busyPoll_(false),
      fd_(FD_INVALID),
      sendBufferSize_(2048),
      is_client_(false),
      ex_(nullptr) {
  initialize();
}

// Destructor performs a "soft" close.
Pair::~Pair() {
  // Needs lock so that this doesn't race with read/write of the
  // underlying file descriptor on the device thread.
  std::lock_guard<std::mutex> lock(m_);
  if (state_ != CLOSED) {
    Pair::changeState(CLOSED);
  }
}

// The close function performs a "hard" close.
// It sets SO_LINGER to reset the connection on close,
// in order to avoid sockets hanging around in TIME_WAIT.
void Pair::close() {
  // Needs lock so that this doesn't race with read/write of the
  // underlying file descriptor on the device thread.
  std::cout<< "close????" <<std::endl;
  std::lock_guard<std::mutex> lock(m_);
  if (state_ != CLOSED) {
    if (fd_ != FD_INVALID) {
      struct linger sl;
      sl.l_onoff = 1;
      sl.l_linger = 0;
      setsockopt(fd_, SOL_SOCKET, SO_LINGER, &sl, sizeof(sl));
    }
    changeState(CLOSED);
  }
}

const Address& Pair::address() const {
  return self_;
}

void Pair::connect(const std::vector<char>& bytes) {
  auto peer = Address(bytes);
  connect(peer);
}

static void setSocketBlocking(int fd, bool enable) {
  auto rv = fcntl(fd, F_GETFL);
  GLOO_ENFORCE_NE(rv, -1);
  if (enable) {
    rv &= ~O_NONBLOCK;
  } else {
    rv |= O_NONBLOCK;
  }
  rv = fcntl(fd, F_SETFL, rv);
  GLOO_ENFORCE_NE(rv, -1);
}

void Pair::setSync(bool sync, bool busyPoll) {
  std::unique_lock<std::mutex> lock(m_);

  if (!sync) {
    GLOO_THROW_INVALID_OPERATION_EXCEPTION("Can only switch to sync mode");
  }

  // Wait for pair to be connected. No need to wait for timeout here. If
  // necessary, the connect path will timeout and signal this thread.
  waitUntilConnected(lock, false);
  if (state_ == CLOSED) {
    signalAndThrowException(
        GLOO_ERROR_MSG("Socket unexpectedly closed ", peer_.str()));
  }

  if (!sync_) {
    // If async, unregister from loop and switch socket to blocking mode
    device_->unregisterDescriptor(fd_, this);
    setSocketBlocking(fd_, true);

    // If the pair was still flushing writes, finish them.
    for (auto& op : tx_) {
      auto rv = write(op);
      if (!rv) {
        GLOO_ENFORCE(
            ex_ != nullptr,
            "write() returned false in sync mode; ex_ must be set");
        std::rethrow_exception(ex_);
      }
    }
    tx_.clear();
  }

  sync_ = true;
  busyPoll_ = busyPoll;
}

void Pair::initialize() {
  std::lock_guard<std::mutex> lock(m_);
  int rv;

  const auto& attr = device_->attr_;
  auto fd = socket(attr.ai_family, attr.ai_socktype, attr.ai_protocol);
  if (fd == -1) {
    signalAndThrowException(GLOO_ERROR_MSG("socket: ", strerror(errno)));
  }

  // Set SO_REUSEADDR to signal that reuse of the listening port is OK.
  int on = 1;
  rv = setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));
  if (rv == -1) {
    ::close(fd);
    signalAndThrowException(GLOO_ERROR_MSG("setsockopt: ", strerror(errno)));
  }

  rv = bind(fd, (const sockaddr*)&attr.ai_addr, attr.ai_addrlen);
  if (rv == -1) {
    ::close(fd);
    signalAndThrowException(GLOO_ERROR_MSG("bind: ", strerror(errno)));
  }
  

  fd_ = fd;

  // Make sure socket is non-blocking
  setSocketBlocking(fd_, false);

  // Set timeout
  struct timeval tv = {};
  tv.tv_sec = timeout_.count() / 1000;
  tv.tv_usec = (timeout_.count() % 1000) * 1000;
  rv = setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
  GLOO_ENFORCE_NE(rv, -1);
  rv = setsockopt(fd_, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
  GLOO_ENFORCE_NE(rv, -1);

  // Keep copy of address
  self_ = Address::fromSockName(fd);

  // Register with device so we're called when peer connects
  device_->registerDescriptor(fd_, EPOLLIN, this);
  changeState(LISTENING);

  return;
}

void Pair::connect(const Address& peer) {
  std::unique_lock<std::mutex> lock(m_);
  int rv;
  socklen_t addrlen;
  throwIfException();

  peer_ = peer;

  const auto& selfAddr = self_.getSockaddr();
  const auto& peerAddr = peer_.getSockaddr();

  // Addresses have to have same family
  if (selfAddr.ss_family != peerAddr.ss_family) {
    GLOO_THROW_INVALID_OPERATION_EXCEPTION("address family mismatch");
  }

  if (selfAddr.ss_family == AF_INET) {
    struct sockaddr_in* sa = (struct sockaddr_in*)&selfAddr;
    struct sockaddr_in* sb = (struct sockaddr_in*)&peerAddr;
    addrlen = sizeof(struct sockaddr_in);
    rv = memcmp(&sa->sin_addr, &sb->sin_addr, sizeof(struct in_addr));
    if (rv == 0) {
      rv = sa->sin_port - sb->sin_port;
    }
  } else if (peerAddr.ss_family == AF_INET6) {
    struct sockaddr_in6* sa = (struct sockaddr_in6*)&selfAddr;
    struct sockaddr_in6* sb = (struct sockaddr_in6*)&peerAddr;
    addrlen = sizeof(struct sockaddr_in6);
    rv = memcmp(&sa->sin6_addr, &sb->sin6_addr, sizeof(struct in6_addr));
    if (rv == 0) {
      rv = sa->sin6_port - sb->sin6_port;
    }
  } else {
    GLOO_THROW_INVALID_OPERATION_EXCEPTION("unknown sa_family");
  }

  if (rv == 0) {
    GLOO_THROW_INVALID_OPERATION_EXCEPTION("cannot connect to self");
  }
  
  // Connect to peer
  rv = ::connect(fd_, (struct sockaddr*)&peerAddr, addrlen);
  if (rv == -1 && errno != EINPROGRESS) {
    ::close(fd_);
    fd_ = FD_INVALID;
    signalAndThrowException(GLOO_ERROR_MSG("connect: ", strerror(errno)));
  }

  // Register with device so we're called when connection completes.
  changeState(CONNECTED);
}

ssize_t Pair::prepareWrite(
    Op& op,
    const NonOwningPtr<UnboundBuffer>& buf,
    char*& content
    ) {
  ssize_t len = 0;

  memcpy(content, (char*)&op.preamble, sizeof(op.preamble));
  len += sizeof(op.preamble);

  auto opcode = op.getOpcode();

  // Send data to a remote buffer
  if (opcode == Op::SEND_BUFFER) {
    char* ptr = (char*)op.buf->ptr_;
    size_t offset = op.preamble.offset;
    size_t nbytes = op.preamble.length;
    if(nbytes > MAXBUFFERSIZE - sizeof(op.preamble)){
      nbytes = MAXBUFFERSIZE - sizeof(op.preamble);
      op.preamble.length -= (MAXBUFFERSIZE - sizeof(op.preamble));
    }
    else{
      nbytes = op.preamble.length;
      op.preamble.length = 0;
    }
    memcpy((char*)(content + sizeof(op.preamble)), (char*)ptr + offset, nbytes);
    len += nbytes;
    op.preamble.offset += nbytes;
  }

  // Send data to a remote unbound buffer
  if (opcode == Op::SEND_UNBOUND_BUFFER) {
    char* ptr = (char*)buf->ptr;
    size_t offset = op.offset;
    size_t nbytes = op.nbytes;
    if(nbytes > MAXBUFFERSIZE - sizeof(op.preamble)){
      nbytes = MAXBUFFERSIZE - sizeof(op.preamble);
      op.preamble.length -= (MAXBUFFERSIZE - sizeof(op.preamble));
    }
    else{
      nbytes = op.preamble.length;
      op.preamble.length = 0;
    }
    memcpy((char*)(content + sizeof(op.preamble)), (char*)ptr + (int)offset, (int)nbytes);
    len += nbytes;
    op.preamble.offset += nbytes;
  }
  return len;
}

// write is called from:
// 1) the device thread (the handleEvents function)
// 2) a user thread (the send function)
//
// In either case, the lock is held and the write function
// below inherits it.
//
bool Pair::write(Op& op) {
  if (state_ == CLOSED) {
    return false;
  }
  NonOwningPtr<UnboundBuffer> buf;
  ssize_t rv;

  const auto opcode = op.getOpcode();

  // Acquire pointer to unbound buffer if applicable.
  if (opcode == Op::SEND_UNBOUND_BUFFER) {
    buf = NonOwningPtr<UnboundBuffer>(op.ubuf);
    if (!buf) {
      return false;
    }
  }

  for (;;) {
    char *content = (char *) malloc(MAXBUFFERSIZE * sizeof(char));
    if(!content){
        std::cout << "malloc error" << std::endl;
    }
    memset(content, 0, MAXBUFFERSIZE);
    const auto len = prepareWrite(op, buf, content);

    // Write
    std::cout << "len = " << len << std::endl;
    rv = sendto(fd_, content, len, 0,  (struct sockaddr*)&(peer_.getSockaddr()), sizeof(peer_.getSockaddr()));
    std::cout << self_.str() << "sendto "<< peer_.str() << " : "  << rv << std::endl;
    if (rv == -1) {
      if (errno == EAGAIN) {
        if (sync_) {
          // Sync mode: blocking call returning with EAGAIN indicates timeout.
          signalException(GLOO_ERROR_MSG("Write timeout ", peer_.str()));
        } else {
          // Async mode: can't write more than this.
        }
        return false;
      }

      if (errno == ECONNRESET) {
        if (!sync_) {
          return false;
        }
      }
      if (errno == EPIPE) {
        if (!sync_) {
          return false;
        }
      }

      // Retry on EINTR
      if (errno == EINTR) {
        continue;
      }

      // Unexpected error
      signalException(
          GLOO_ERROR_MSG("sendto ", peer_.str(), ": ", strerror(errno)));
      return false;
    }

    // From write(2) man page (NOTES section):
    //
    //  If a write() is interrupted by a signal handler before any
    //  bytes are written, then the call fails with the error EINTR;
    //  if it is interrupted after at least one byte has been written,
    //  the call succeeds, and returns the number of bytes written.
    //
    // If rv < nbytes we ALWAYS retry, regardless of sync/async mode,
    // since an EINTR may or may not have happened. If this was not
    // the case, and the kernel buffer is full, the next call to
    // write(2) will return EAGAIN, which is handled appropriately.

    if (op.preamble.length != 0 && opcode != Op::NOTIFY_SEND_READY && opcode != Op::NOTIFY_RECV_READY) {
      continue;
    }

    break;
  }

  std::cout << "writeComplete" << std::endl;
  writeComplete(op, buf, opcode);
  return true;
}

void Pair::writeComplete(const Op &op, NonOwningPtr<UnboundBuffer> &buf,
                         const Op::Opcode &opcode) const {
  switch (opcode) {
    case Op::SEND_BUFFER:
      op.buf->handleSendCompletion();
      break;
    case Op::SEND_UNBOUND_BUFFER:
      buf->handleSendCompletion(this->rank_);
      break;
    case Op::NOTIFY_SEND_READY:
      break;
    case Op::NOTIFY_RECV_READY:
      break;
  }
}

bool Pair::prepareRead(){
  if (state_ == CLOSED) {
    return false;
  }

  NonOwningPtr<UnboundBuffer> buf;
  auto start = std::chrono::steady_clock::now();
  const auto& peerAddr = peer_.getSockaddr();
  socklen_t addrlen;

  if (peerAddr.ss_family == AF_INET) {
    addrlen = sizeof(struct sockaddr_in);
  } else if (peerAddr.ss_family == AF_INET6) {
    addrlen = sizeof(struct sockaddr_in6);
  } else {
    GLOO_THROW_INVALID_OPERATION_EXCEPTION("unknown sa_family");
  }

  for (;;) {
    ssize_t rv = 0;
    for (;;) {
      rv = ::recvfrom(fd_, (char*)&rx_.preamble, sizeof(rx_.preamble), MSG_PEEK, (struct sockaddr*)&peerAddr, &addrlen);
      if (rv == -1) { 
        // EAGAIN happens when (1) non-blocking and there are no more bytes left
        // to read or (2) blocking and timeout occurs.
        if (errno == EAGAIN) {
          if (sync_) {
            // Sync mode: EAGAIN indicates nothing to read right now.
            auto hasTimedOut = [&] {
              return (timeout_ != kNoTimeout) &&
                  ((std::chrono::steady_clock::now() - start) >= timeout_);
            };
            if (busyPoll_ && !hasTimedOut()) {
              // Keep looping on EAGAIN if busy-poll flag has been set and the
              // timeout (if set) hasn't been reached
              continue;
            } else {
              // Either timeout on poll or blocking call returning with EAGAIN
              // indicates timeout
              signalException(GLOO_ERROR_MSG("Read timeout ", peer_.str()));
            }
          } else {
            // Async mode: can't read more than this.
          }
          return false;
        }

        // Retry on EINTR
        if (errno == EINTR) {
          continue;
        }

        // Unexpected error
        signalException(
            GLOO_ERROR_MSG("Read error ", peer_.str(), ": ", strerror(errno)));
        return false;
      }
      break;
    }

    // Transition to CLOSED on EOF
    if (rv == 0) {
      signalException(
          GLOO_ERROR_MSG("Connection closed by peer ", peer_.str()));
      return false;
    }
    auto opcode = rx_.getOpcode();

    if (opcode == Op::SEND_BUFFER){
      if (rx_.buf == nullptr) {
        rx_.buf = getBuffer(rx_.preamble.slot);
        // Buffer not (yet) registered, leave it for next loop iteration
        if (rx_.buf == nullptr) {
          return false;
        }
      }
    }
    else if(opcode == Op::SEND_UNBOUND_BUFFER){
      if (!rx_.ubuf) {
        auto it = localPendingRecv_.find(rx_.preamble.slot);
        GLOO_ENFORCE(it != localPendingRecv_.end());
        std::deque<UnboundBufferOp>& queue = it->second;
        GLOO_ENFORCE(!queue.empty());
        std::tie(rx_.ubuf, rx_.offset, rx_.nbytes) = queue.front();
        queue.pop_front();
        if (queue.empty()) {
          localPendingRecv_.erase(it);
        }
      }

      // Acquire short lived pointer to unbound buffer.
      // This is a stack allocated variable in the read function
      // which is destructed upon that function returning.
      buf = NonOwningPtr<UnboundBuffer>(rx_.ubuf);
      if (!buf) {
        return false;
      }
    }
    else if(opcode == Op::NOTIFY_SEND_READY ||opcode == Op::NOTIFY_RECV_READY){
      return true;
    }
    else{
      exit(-1);
    }
    return true;
  }
}

// read is called from:
// 1) the device thread (the handleEvents function).
// 2) a user thread (the recv function) IFF the pair is in sync mode.
//
// In either case, the lock is held and the read function
// below inherits it.
//
bool Pair::read() {
  bool prepareOK = prepareRead();
  if(!prepareOK){
    return false;
  }

  NonOwningPtr<UnboundBuffer> buf;
  auto start = std::chrono::steady_clock::now();
  const auto& peerAddr = peer_.getSockaddr();
  socklen_t addrlen;

  if (peerAddr.ss_family == AF_INET) {
    addrlen = sizeof(struct sockaddr_in);
  } else if (peerAddr.ss_family == AF_INET6) {
    addrlen = sizeof(struct sockaddr_in6);
  } else {
    GLOO_THROW_INVALID_OPERATION_EXCEPTION("unknown sa_family");
  }

  for (;;) {
    ssize_t rv = 0;
    char *content = (char *) malloc(MAXBUFFERSIZE * sizeof(char));
    if(!content){
        std::cout << "malloc error" << std::endl;
        return false;
    }
    memset(content, 0, MAXBUFFERSIZE);
    if(content == NULL){
      exit(-1);
    }
    for (;;) {
      rv = ::recvfrom(fd_, content, MAXBUFFERSIZE, busyPoll_ ? MSG_DONTWAIT : 0, (struct sockaddr*)&peerAddr, &addrlen);
      if (rv == -1) { 
        // EAGAIN happens when (1) non-blocking and there are no more bytes left
        // to read or (2) blocking and timeout occurs.
        if (errno == EAGAIN) {
          if (sync_) {
            // Sync mode: EAGAIN indicates nothing to read right now.
            auto hasTimedOut = [&] {
              return (timeout_ != kNoTimeout) &&
                  ((std::chrono::steady_clock::now() - start) >= timeout_);
            };
            if (busyPoll_ && !hasTimedOut()) {
              // Keep looping on EAGAIN if busy-poll flag has been set and the
              // timeout (if set) hasn't been reached
              continue;
            } else {
              // Either timeout on poll or blocking call returning with EAGAIN
              // indicates timeout
              signalException(GLOO_ERROR_MSG("Read timeout ", peer_.str()));
            }
          } else {
            // Async mode: can't read more than this.
          }
          return false;
        }

        // Retry on EINTR
        if (errno == EINTR) {
          continue;
        }

        // Unexpected error
        signalException(
            GLOO_ERROR_MSG("Read error ", peer_.str(), ": ", strerror(errno)));
        return false;
      }
      else{
        printf("read[%d]\n", rv);
      }
      break;
    }

    // Transition to CLOSED on EOF
    if (rv == 0) {
      signalException(
          GLOO_ERROR_MSG("Connection closed by peer ", peer_.str()));
      return false;
    }

    auto opcode = rx_.getOpcode();

    if (opcode == Op::SEND_BUFFER){
      if (rx_.buf == nullptr) {
        rx_.buf = getBuffer(rx_.preamble.slot);
        // Buffer not (yet) registered, leave it for next loop iteration
        if (rx_.buf == nullptr) {
          std::cout<< "return -1!!!" <<std::endl;
          return false;
        }
      }

      char* dest = ((char*)rx_.buf->ptr_) + rx_.preamble.offset + rx_.preamble.roffset;
      char* src = content + sizeof(rx_.preamble);

      memcpy(dest, src, (int)rv - sizeof(rx_.preamble));
      if(rv == rx_.preamble.length + sizeof(rx_.preamble)){
        break;
      }
    }
    else if(opcode == Op::SEND_UNBOUND_BUFFER){
      if (!rx_.ubuf) {
        auto it = localPendingRecv_.find(rx_.preamble.slot);
        GLOO_ENFORCE(it != localPendingRecv_.end());
        std::deque<UnboundBufferOp>& queue = it->second;
        GLOO_ENFORCE(!queue.empty());
        std::tie(rx_.ubuf, rx_.offset, rx_.nbytes) = queue.front();
        queue.pop_front();
        if (queue.empty()) {
          localPendingRecv_.erase(it);
        }
      }

      // Acquire short lived pointer to unbound buffer.
      // This is a stack allocated variable in the read function
      // which is destructed upon that function returning.
      buf = NonOwningPtr<UnboundBuffer>(rx_.ubuf);
      if (!buf) {
        return -1;
      }

      memcpy((char*)(((char*)buf->ptr) + rx_.preamble.offset + rx_.preamble.roffset), content + sizeof(rx_.preamble), rv - sizeof(rx_.preamble));
      if(rv == rx_.preamble.length + sizeof(rx_.preamble)){
        break;
      }
    }
    else if(opcode == Op::NOTIFY_SEND_READY ||opcode == Op::NOTIFY_RECV_READY){
      break;
    }
    else{
      std::cout<< "exit!!!" <<std::endl;
      exit(-1);
    }
  }

  std::cout<< "going to execute readComplete in read()" <<std::endl;
  readComplete(buf);
  return true;
}

void Pair::readComplete(NonOwningPtr<UnboundBuffer> &buf) {
  const auto opcode = this->rx_.getOpcode();
  std::cout <<"op =" << opcode << std::endl;
  switch (opcode) {
    case Op::SEND_BUFFER:
      // Done sending data to pinned buffer; trigger completion.
      this->rx_.buf->handleRecvCompletion();
      break;
    case Op::SEND_UNBOUND_BUFFER:
      // Remote side is sending data to unbound buffer; trigger completion
      buf->handleRecvCompletion(this->rank_);
      break;
    case Op::NOTIFY_SEND_READY:
      // Remote side has pending send operation
      this->handleRemotePendingSend(this->rx_);
      break;
    case Op::NOTIFY_RECV_READY:
      // Remote side has pending recv operation
      this->handleRemotePendingRecv(this->rx_);
      break;
    }

    // Reset read operation state.
  this->rx_ = Op();
}

// This function is called upon receiving a message from the peer
// indicating it has a pending send operation.
void Pair::handleRemotePendingSend(const Op& op) {
  const auto& slot = op.preamble.slot;

  // Acquire context lock through mutator.
  Context::Mutator mutator(*context_, slot, rank_);

  // If a receive operation was posted without there already being a
  // corresponding send notification, we'll find a pending send
  // notification and don't need to handle this send notification.
  if (mutator.shiftExpectedSendNotification()) {
    return;
  }

  {
    // If we're ready to add it to the context wide pending operation
    // tally, first check if there are any recv-from-any operations
    // that this send operation can fulfill.
    WeakNonOwningPtr<UnboundBuffer> buf;
    size_t offset;
    size_t nbytes;
    if (context_->findRecvFromAny(slot, rank_, &buf, &offset, &nbytes)) {
      localPendingRecv_[slot].push_back(std::make_tuple(buf, offset, nbytes));
      sendNotifyRecvReady(slot, nbytes);
      return;
    }
  }

  // Increase balance of remote pending sends.
  mutator.pushRemotePendingSend();
}

// This function is called upon receiving a message from the peer
// indicating it has a pending receive operation.
void Pair::handleRemotePendingRecv(const Op& op) {
  const auto& slot = op.preamble.slot;

  // Find local pending send and execute it.
  // Nothing to do if there are none.
  auto it = localPendingSend_.find(slot);
  if (it != localPendingSend_.end()) {
    std::deque<UnboundBufferOp>& queue = it->second;
    GLOO_ENFORCE(!queue.empty());
    WeakNonOwningPtr<UnboundBuffer> buf;
    size_t offset;
    size_t nbytes;
    std::tie(buf, offset, nbytes) = queue.front();
    queue.pop_front();
    if (queue.empty()) {
      localPendingSend_.erase(it);
    }
    sendUnboundBuffer(std::move(buf), slot, offset, nbytes);
    return;
  }

  // Increase balance of remote pending recv.
  // Note that the current value CANNOT be negative, as sends
  // cannot execute until the remote side is ready to receive.
  Context::Mutator mutator(*context_, slot, rank_);
  mutator.pushRemotePendingRecv();
}

void Pair::handleEvents(int events) {
  // Try to acquire the pair's lock so the device thread (the thread
  // that ends up calling handleEvents) can mutate the tx and rx op
  // fields of this instance. If the lock cannot be acquired that
  // means some other thread is trying to mutate this pair's state,
  // which in turn might require calling into (and locking) the
  // underlying device (for example, when the pair transitions to the
  // CLOSED state). To avoid deadlocks, attempt to lock the pair and
  // skip handling the events until the next tick if the lock cannot
  // be acquired.
  std::unique_lock<std::mutex> lock(m_, std::try_to_lock);
  if (!lock) {
    return;
  }

  // State must be <= CONNECTED.
  // If state is CLOSED; this function will NOT be called. Refer to
  // Pair::changeState and Device::unregisterDescriptor for more info.
  GLOO_ENFORCE_LE(state_, CONNECTED);

  // Exception must not be set.
  // If exception is set, state must advance to CLOSED state.
  GLOO_ENFORCE(ex_ == nullptr);

  if (state_ == CONNECTED) {
    handleReadWrite(events);
    return;
  }
}

void Pair::handleReadWrite(int events) {
  if (events & EPOLLOUT) {
    GLOO_ENFORCE(
        !tx_.empty(), "tx_ cannot be empty because EPOLLOUT happened");
    while (!tx_.empty()) {
      auto& op = tx_.front();
      if (!write(op)) {
        // Write did not complete; wait for epoll.
        break;
      }
      // Write completed; remove from queue.
      tx_.pop_front();
    }
    // If there is nothing to transmit; remove EPOLLOUT.
    if (tx_.empty()) {
      device_->registerDescriptor(fd_, EPOLLIN, this);
    }
  }
  if (events & EPOLLIN) {
    while (read()) {
      // Keep going
    }
  }
}

// getBuffer must only be called when holding lock.
Buffer* Pair::getBuffer(int slot) {
  for (;;) {
    auto it = buffers_.find(slot);
    if (it == buffers_.end()) {
      // The remote peer already sent some bytes destined for the
      // buffer at this slot, but this side of the pair hasn't
      // registed it yet.
      //
      // The current strategy is to return and let the the device loop
      // repeatedly call us again until the buffer has been
      // registered. This essentially means busy waiting while
      // yielding to other pairs. This is not a problem as this only
      // happens at initialization time.
      //
      return nullptr;
    }

    return it->second;
  }
}

void Pair::registerBuffer(Buffer* buf) {
  std::lock_guard<std::mutex> lock(m_);
  GLOO_ENFORCE(
      buffers_.find(buf->slot_) == buffers_.end(),
      "duplicate buffer for slot ",
      buf->slot_);
  buffers_[buf->slot_] = buf;
  cv_.notify_all();
}

void Pair::unregisterBuffer(Buffer* buf) {
  std::lock_guard<std::mutex> lock(m_);
  buffers_.erase(buf->slot_);
}

// changeState must only be called when holding lock.
void Pair::changeState(state nextState) noexcept {
  if (nextState == CLOSED) {
    switch (state_) {
      case INITIALIZING:
        // This state persists from construction up to the point where
        // Pair::listen sets fd_ and calls listen(2). If this fails,
        // it takes care of cleaning up the socket itself.
        // There is no additional cleanup needed here.
        break;
      case LISTENING:
        break;
      case CONNECTING:
        // The pair may be in the CONNECTING state when it is destructed.
        if (fd_ != FD_INVALID) {
          device_->unregisterDescriptor(fd_, this);
          ::close(fd_);
          fd_ = FD_INVALID;
        }
        break;
      case CONNECTED:
        break;
      case CLOSED:
        // This can't happen, because we ignore no-op state changes above.
        // We handle it regardless to have a case for every enum value.
        break;
    }
  }

  state_ = nextState;
  cv_.notify_all();
}

void Pair::waitUntilConnected(
    std::unique_lock<std::mutex>& lock,
    bool useTimeout) {
  auto pred = [&] {
    throwIfException();
    return state_ >= CONNECTED;
  };
  waitUntil(pred, lock, useTimeout);
}

void Pair::verifyConnected() {
  // This code path should only be called after reaching the connected state
  GLOO_ENFORCE_GE(
      state_,
      CONNECTED,
      "Pair is not connected (",
      self_.str(),
      " <--> ",
      peer_.str(),
      ")");
  // Check if the socket has been closed. We were unable to tell if this was an
  // error or normal tear down, but now throw since we are trying to do IO.
  if (state_ == CLOSED) {
    signalAndThrowException(GLOO_ERROR_MSG("Socket closed ", peer_.str()));
  }
}

// Sends contents of operation to the remote side of the pair.
// The pair's mutex is held when this function is called.
// Only applicable to synchronous mode. May block.
void Pair::sendSyncMode(Op& op) {
  GLOO_ENFORCE(sync_);
  auto rv = write(op);
  if (!rv) {
    GLOO_ENFORCE(ex_ != nullptr);
    std::rethrow_exception(ex_);
  }
}

// Sends contents of operation to the remote side of the pair.
// The pair's mutex is held when this function is called.
// Only applicable to asynchronous mode. Never blocks.
void Pair::sendAsyncMode(Op& op) {
  GLOO_ENFORCE(!sync_);

  // If an earlier operation hasn't finished transmitting,
  // add this operation to the transmit queue.
  if (!tx_.empty()) {
    tx_.push_back(std::move(op));
    return;
  }

  // Write in place without checking socket for writeability.
  // This is the fast path.
  if (write(op)) {
    return;
  }

  // Write may have resulted in an error.
  throwIfException();

  // Write didn't complete; pass to event loop
  tx_.push_back(std::move(op));
  device_->registerDescriptor(fd_, EPOLLOUT, this);
}

void Pair::send(Op& op) {
  std::unique_lock<std::mutex> lock(m_);
  throwIfException();
  verifyConnected();

  // Try to size the send buffer such that the write below completes
  // synchronously and we don't need to finish the write later.
  size_t size = std::min(op.preamble.nbytes, kMaxSendBufferSize);
  if (sendBufferSize_ < size) {
    int rv;
    size_t optval = size;
    socklen_t optlen = sizeof(optval);
    rv = setsockopt(fd_, SOL_SOCKET, SO_SNDBUF, &optval, optlen);
    GLOO_ENFORCE_NE(rv, -1);
    rv = getsockopt(fd_, SOL_SOCKET, SO_SNDBUF, &optval, &optlen);
    GLOO_ENFORCE_NE(rv, -1);
    sendBufferSize_ = optval;
  }

  // Write to socket
  if (sync_) {
    sendSyncMode(op);
  } else {
    sendAsyncMode(op);
  }
}

void Pair::recv() {
  std::cout<<"starting recv" <<std::endl;
  std::unique_lock<std::mutex> lock(m_);
  throwIfException();
  verifyConnected();

  auto rv = read();
  if (!rv) {
    GLOO_ENFORCE(
        ex_ != nullptr, "read() returned false in sync mode; ex_ must be set");
    std::rethrow_exception(ex_);
  }
}

std::unique_ptr<::gloo::transport::Buffer> Pair::createSendBuffer(
    int slot,
    void* ptr,
    size_t size) {
  auto buffer = new Buffer(this, slot, ptr, size);
  return std::unique_ptr<::gloo::transport::Buffer>(buffer);
}

std::unique_ptr<::gloo::transport::Buffer> Pair::createRecvBuffer(
    int slot,
    void* ptr,
    size_t size) {
  auto buffer = new Buffer(this, slot, ptr, size);
  registerBuffer(buffer);
  return std::unique_ptr<::gloo::transport::Buffer>(buffer);
}

// Send from the specified buffer to remote side of pair.
void Pair::send(
    transport::UnboundBuffer* tbuf,
    uint64_t slot,
    size_t offset,
    size_t nbytes) {
  auto buf = static_cast<tcp::UnboundBuffer*>(tbuf)->getWeakNonOwningPtr();

  if (nbytes > 0) {
    GLOO_ENFORCE_LE(offset, tbuf->size);
    GLOO_ENFORCE_LE(nbytes, tbuf->size - offset);
  }

  std::unique_lock<std::mutex> lock(m_);
  throwIfException();

  // Execute this send if there is a remote pending receive.
  Context::Mutator mutator(*context_, slot, rank_);
  if (mutator.shiftRemotePendingRecv()) {
    // We keep a count of remote pending send and receive operations.
    // In this code path the remote side hasn't seen a notification
    // for this send operation yet so we need to take special care
    // their count is updated regardless.
    sendNotifySendReady(slot, nbytes);
    sendUnboundBuffer(std::move(buf), slot, offset, nbytes);
    return;
  }

  // Notify peer of this pending send.
  localPendingSend_[slot].push_back(std::make_tuple(buf, offset, nbytes));
  sendNotifySendReady(slot, nbytes);
}

// Receive into the specified buffer from the remote side of pair.
void Pair::recv(
    transport::UnboundBuffer* tbuf,
    uint64_t slot,
    size_t offset,
    size_t nbytes) {
  auto buf = static_cast<tcp::UnboundBuffer*>(tbuf)->getWeakNonOwningPtr();

  if (nbytes > 0) {
    GLOO_ENFORCE_LE(offset, tbuf->size);
    GLOO_ENFORCE_LE(nbytes, tbuf->size - offset);
  }

  std::unique_lock<std::mutex> lock(m_);
  throwIfException();

  // If this recv happens before the send notification,
  // we are still owed a send notification. Because this recv
  // has already been posted, we have to make sure it doesn't
  // hit the context wide tally.
  Context::Mutator mutator(*context_, slot, rank_);
  if (!mutator.shiftRemotePendingSend()) {
    mutator.pushExpectedSendNotification();
  }

  // Notify peer of this pending recv.
  localPendingRecv_[slot].push_back(std::make_tuple(buf, offset, nbytes));
  sendNotifyRecvReady(slot, nbytes);
}

bool Pair::tryRecv(
    transport::UnboundBuffer* tbuf,
    uint64_t slot,
    size_t offset,
    size_t nbytes) {
  auto buf = static_cast<tcp::UnboundBuffer*>(tbuf)->getWeakNonOwningPtr();

  if (nbytes > 0) {
    GLOO_ENFORCE_LE(offset, tbuf->size);
    GLOO_ENFORCE_LE(nbytes, tbuf->size - offset);
  }

  std::unique_lock<std::mutex> lock(m_);
  throwIfException();

  // Return early if there is no remote pending send.
  Context::Mutator mutator(*context_, slot, rank_);
  if (!mutator.shiftRemotePendingSend()) {
    return false;
  }

  // Notify peer of this pending recv.
  localPendingRecv_[slot].push_back(std::make_tuple(buf, offset, nbytes));
  sendNotifyRecvReady(slot, nbytes);
  return true;
}

void Pair::sendUnboundBuffer(
    WeakNonOwningPtr<UnboundBuffer> buf,
    uint64_t slot,
    size_t offset,
    size_t nbytes) {
  Op op;
  op.preamble.nbytes = sizeof(op.preamble) + nbytes;
  op.preamble.opcode = Op::SEND_UNBOUND_BUFFER;
  op.preamble.slot = slot;
  op.preamble.length = nbytes;
  op.ubuf = std::move(buf);
  op.offset = offset;
  op.nbytes = nbytes;
  sendAsyncMode(op);
}

void Pair::sendNotifyRecvReady(uint64_t slot, size_t nbytes) {
  Op op;
  op.preamble.nbytes = sizeof(op.preamble);
  op.preamble.opcode = Op::NOTIFY_RECV_READY;
  op.preamble.slot = slot;
  op.preamble.length = nbytes;
  sendAsyncMode(op);
}

void Pair::sendNotifySendReady(uint64_t slot, size_t nbytes) {
  Op op;
  op.preamble.nbytes = sizeof(op.preamble);
  op.preamble.opcode = Op::NOTIFY_SEND_READY;
  op.preamble.slot = slot;
  op.preamble.length = nbytes;
  sendAsyncMode(op);
}

void Pair::throwIfException() {
  // If we previously encountered an error, rethrow here.
  if (ex_ != nullptr) {
    std::rethrow_exception(ex_);
  }
}

std::exception_ptr Pair::signalExceptionExternal(const std::string& msg) {
  std::lock_guard<std::mutex> lock(m_);

  // This function may be called by a buffer upon timing out waiting
  // for some operation to complete. It may race with the device
  // thread performing read/write operations, detecting a failure, and
  // setting an exception as well. Therefore, check if an exception is
  // already set, and ignore this call if it is.
  if (ex_ == nullptr) {
    signalException(msg);
  }
  return ex_;
}

void Pair::signalException(const std::string& msg) {
  signalException(std::make_exception_ptr(::gloo::IoException(msg)));
}

void Pair::signalException(std::exception_ptr ex) {
  GLOO_ENFORCE(ex_ == nullptr);

  // Loop through the buffers and signal that an error has occurred.
  for (auto it = buffers_.begin(); it != buffers_.end(); it++) {
    it->second->signalException(ex);
  }

  // Loop through posted send operations.
  for (auto& op : tx_) {
    if (op.buf != nullptr) {
      op.buf->signalException(ex);
    }
  }

  // Loop through pending send operations.
  for (auto& it : localPendingSend_) {
    for (auto& op : it.second) {
      NonOwningPtr<UnboundBuffer> buf(std::get<0>(op));
      if (buf) {
        buf->signalException(ex);
      }
    }
  }

  // Loop through pending recv operations.
  for (auto& it : localPendingRecv_) {
    for (auto& op : it.second) {
      NonOwningPtr<UnboundBuffer> buf(std::get<0>(op));
      if (buf) {
        buf->signalException(ex);
      }
    }
  }

  // Store exception_ptr and signal any threads in the async path.
  ex_ = ex;
  cv_.notify_all();

  // Move to closed state.
  // Either this error is an underlying socket error and the socket
  // must be closed, or this error is an application side timeout, and
  // we are no longer guaranteed that buffer pointers will be valid.
  changeState(CLOSED);
}

void Pair::signalAndThrowException(const std::string& msg) {
  signalAndThrowException(std::make_exception_ptr(::gloo::IoException(msg)));
}

void Pair::signalAndThrowException(std::exception_ptr ex) {
  signalException(ex);
  std::rethrow_exception(ex);
}

} // namespace tcp
} // namespace transport
} // namespace gloo
