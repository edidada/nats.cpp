// SPDX-License-Identifier: MIT
#pragma once

#include <string>

namespace nats {

namespace ws {

using OpCode = int;

constexpr auto TextMessage = OpCode(1);
constexpr auto BinaryMessage = OpCode(2);
constexpr auto CloseMessage = OpCode(8);
constexpr auto PingMessage = OpCode(9);
constexpr auto PongMessage = OpCode(10);

constexpr auto FinalBit = 1 << 7;
constexpr auto Rsv1Bit =
    1 << 6; // Used for compression, from
            // https://tools.ietf.org/html/rfc7692#section-6
constexpr auto Rsv2Bit = 1 << 5;
constexpr auto Rsv3Bit = 1 << 4;

constexpr auto MaskBit = 1 << 7;

constexpr auto ContinuationFrame = 0;
constexpr auto MaxFrameHeaderSize = 14;
constexpr auto MaxControlPayloadSize = 125;
constexpr auto CloseStatusSize = 2;

// From https://tools.ietf.org/html/rfc6455#section-11.7
constexpr auto CloseStatusNormalClosure = 1000;
constexpr auto CloseStatusNoStatusReceived = 1005;
constexpr auto CloseStatusAbnormalClosure = 1006;
constexpr auto CloseStatusInvalidPayloadData = 1007;

constexpr auto Scheme = "ws";
constexpr auto SchemeTLS = "wss";

constexpr std::string PMCExtension =
    "permessage-deflate"; // per-message compression
constexpr auto PMCSrvNoCtx = "server_no_context_takeover";
constexpr auto PMCCliNoCtx = "client_no_context_takeover";
// constexpr auto PMCReqHeaderValue =
//     PMCExtension + "; " + PMCSrvNoCtx + "; " + PMCCliNoCtx;

} // namespace ws

} // namespace nats
