/*
 *
 * Copyright 2015 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

#include <grpcpp/grpcpp.h>

#include <iostream>
#include <memory>
#include <string>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/log/check.h"

#ifdef BAZEL_BUILD
#include "examples/protos/helloworld.grpc.pb.h"
#else
#include "helloworld.grpc.pb.h"
#endif

ABSL_FLAG(std::string, target, "localhost:50052", "Server address");


constexpr std::string_view kRetryPolicy = R"({
  "methodConfig" : [{
    "name": [{}],
    "retryPolicy": {
      "maxAttempts": 4,
      "initialBackoff": "0.010s",
      "maxBackoff": "0.300s",
      "backoffMultiplier": 2.0,
      "perAttemptRecvTimeout": "0.010s",
      "retryableStatusCodes": ["UNAVAILABLE"]
    }
  }]
})";

using grpc::Channel;
using grpc::ClientAsyncResponseReader;
using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::Status;
using helloworld::Greeter;
using helloworld::HelloReply;
using helloworld::HelloRequest;

class GreeterClient {
 public:
  explicit GreeterClient(std::shared_ptr<Channel> channel)
      : stub_(Greeter::NewStub(channel)) {}

  // Assembles the client's payload, sends it and presents the response back
  // from the server.
  std::string SayHello(const std::string& user) {
    // Data we are sending to the server.
    HelloRequest request;
    request.set_name(user);

    // Container for the data we expect from the server.
    HelloReply reply;

    // Context for the client. It could be used to convey extra information to
    // the server and/or tweak certain RPC behaviors.
    ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds{20});

    // The producer-consumer queue we use to communicate asynchronously with the
    // gRPC runtime.
    CompletionQueue cq;

    // Storage for the status of the RPC upon completion.
    Status status;

    std::unique_ptr<ClientAsyncResponseReader<HelloReply> > rpc(
        stub_->AsyncSayHello(&context, request, &cq));

    // Request that, upon completion of the RPC, "reply" be updated with the
    // server's response; "status" with the indication of whether the operation
    // was successful. Tag the request with the integer 1.
    rpc->Finish(&reply, &status, (void*)1);
    void* got_tag;
    bool ok = false;
    // Block until the next result is available in the completion queue "cq".
    // The return value of Next should always be checked. This return value
    // tells us whether there is any kind of event or the cq_ is shutting down.
    CHECK(cq.Next(&got_tag, &ok));

    // Verify that the result from "cq" corresponds, by its tag, our previous
    // request.
    CHECK_EQ(got_tag, (void*)1);
    // ... and that the request was completed successfully. Note that "ok"
    // corresponds solely to the request for updates introduced by Finish().
    CHECK(ok);

    // Act upon the status of the actual RPC.
    if (status.ok()) {
      return reply.message();
    } else {
      return "RPC failed";
    }
  }

 private:
  // Out of the passed in Channel comes the stub, stored here, our view of the
  // server's exposed services.
  std::unique_ptr<Greeter::Stub> stub_;
};

int main(int argc, char** argv) {
  absl::ParseCommandLine(argc, argv);
  // Instantiate the client. It requires a channel, out of which the actual RPCs
  // are created. This channel models a connection to an endpoint specified by
  // the argument "--target=" which is the only expected argument.
  std::string target_str = absl::GetFlag(FLAGS_target);
  // We indicate that the channel isn't authenticated (use of
  // InsecureChannelCredentials()).
  auto channel_args = grpc::ChannelArguments();
  channel_args.SetServiceConfigJSON(std::string(kRetryPolicy));
  channel_args.SetInt(GRPC_ARG_EXPERIMENTAL_ENABLE_HEDGING, 1);

  GreeterClient greeter(
      grpc::CreateCustomChannel(target_str, grpc::InsecureChannelCredentials(), channel_args));
  std::string user("world");
  // std::string reply = greeter.SayHello(user);  // The actual RPC call!
  // std::cout << "Greeter received: " << reply << std::endl;
  int limit = 1000;
  while (limit--) {
      std::string reply = greeter.SayHello(user);  // The actual RPC call!
      std::cout << limit << ": Greeter received: " << reply << std::endl;
  }

  return 0;
}
