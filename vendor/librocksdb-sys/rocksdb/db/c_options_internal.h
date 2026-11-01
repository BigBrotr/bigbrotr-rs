// Copyright (c) Meta Platforms, Inc. and affiliates.
// This source code is licensed under both GPLv2 (COPYING) and Apache 2.0
// (LICENSE.Apache), as in the owning C binding.
#ifndef ROCKSDB_DB_C_OPTIONS_INTERNAL_H_
#define ROCKSDB_DB_C_OPTIONS_INTERNAL_H_

#include "rocksdb/c.h"
#include "rocksdb/options.h"

// Sole implementation definition of the public opaque C options type. Both the
// compiled C binding and its generator template include this owning header.
// Native component controls use this type, never an imitated opaque layout.
struct rocksdb_options_t {
  ROCKSDB_NAMESPACE::Options rep;
};

#endif  // ROCKSDB_DB_C_OPTIONS_INTERNAL_H_
