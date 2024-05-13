// Copyright (C) 2024-2028 Jun Zhang and nats.cpp contributors
// SPDX-License-Identifier: MIT
#define ANKERL_NANOBENCH_IMPLEMENT
#include <nanobench.h>

#include <Nuid.hpp>
#include <icecream.hpp>

#include <print>

namespace nb = ankerl::nanobench;

void nuid() {
  using namespace nats;
  Nuid<nuid::Base62> nuid62;
  // Nuid<nuid::Rfc4648_Base64> nuid64;

  // nb::Bench().run("nuid64::next", [&] { nuid64.next(); });
  nb::Bench().run("nuid62::next", [&] { nuid62.next(); });
}

int main() {
  nuid();
  return 0;
}
