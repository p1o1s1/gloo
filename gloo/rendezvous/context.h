/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <memory>
#include <vector>

#include "gloo/common/error.h"
#include "gloo/context.h"
#include "gloo/rendezvous/store.h"
#include "gloo/transport/address.h"
#include "gloo/transport/device.h"

namespace gloo {
namespace rendezvous {

class ContextFactory;

class Context : public ::gloo::Context {
 public:
  Context(int rank, int size, int base = 2);
  virtual ~Context();

  void connectFullMesh(
      Store& store,
      std::shared_ptr<transport::Device>& dev);

 protected:
  std::vector<char> extractAddress(std::vector<char>& allAddrs, int i);

  friend class ContextFactory;
};

class ContextFactory {
 public:
  static constexpr auto kMaxAddressSize =
      ::gloo::transport::Address::kMaxByteSize;

  explicit ContextFactory(std::shared_ptr<::gloo::Context> backingContext);

  std::shared_ptr<::gloo::Context> makeContext(
    std::shared_ptr<transport::Device>& dev);

  std::shared_ptr<::gloo::Context> backingContext_;

protected:
  std::vector<std::vector<char>> recvData_;
  std::vector<std::vector<char>> sendData_;

  std::vector<std::unique_ptr<transport::Buffer>> recvBuffers_;
  std::vector<std::unique_ptr<transport::Buffer>> sendBuffers_;

  std::vector<int> recvNotificationData_;
  std::vector<std::unique_ptr<transport::Buffer>> recvNotificationBuffers_;

  std::vector<int> sendNotificationData_;
  std::vector<std::unique_ptr<transport::Buffer>> sendNotificationBuffers_;
};


} // namespace rendezvous

} // namespace gloo
