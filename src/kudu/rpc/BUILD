load("@//bazel:protoc-plugin.bzl", "protoc_plugin_library")

protoc_plugin_library(
    name = "rpc_header_cc_proto",
    srcs = ["rpc_header.proto"],
    out = [
        "rpc_header.pb.cc",
        "rpc_header.pb.h",
    ],
    compiler = "@com_google_protobuf//:protoc",
    opts = [
        "--cpp_out",
        "--insertions_out",
    ],
    plugins = [
        "//src/kudu/util:protoc-gen-insertions",
    ],
    visibility = ["//visibility:public"],
)

cc_library(
    name = "rpc_header_proto",
    srcs = [":rpc_header_cc_proto"],
    hdrs = [":rpc_header_cc_proto"],
    include_prefix = "kudu/rpc",
    visibility = ["//visibility:public"],
    deps = [
        "//src/kudu/security:token_proto",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_library(
    name = "rpc_header_proto_exported",
    srcs = [":rpc_header_cc_proto"],
    hdrs = [":rpc_header_cc_proto"],
    copts = [
        "-UKUDU_STATIC_DEFINE",
        "-UTCMALLOC_ENABLED",
        "-fvisibility=hidden",
        "-fvisibility-inlines-hidden",
    ],
    include_prefix = "kudu/rpc",
    visibility = ["//visibility:public"],
    deps = [
        "//src/kudu/security:token_proto_exported",
        "@com_google_protobuf//:protobuf",
    ],
)

protoc_plugin_library(
    name = "rpc_introspection_cc_proto",
    srcs = ["rpc_introspection.proto"],
    out = [
        "rpc_introspection.pb.cc",
        "rpc_introspection.pb.h",
    ],
    compiler = "@com_google_protobuf//:protoc",
    opts = [
        "--cpp_out",
        "--insertions_out",
    ],
    plugins = [
        "//src/kudu/util:protoc-gen-insertions",
    ],
    visibility = ["//visibility:public"],
)

cc_library(
    name = "rpc_introspection_proto",
    srcs = [":rpc_introspection_cc_proto"],
    hdrs = [":rpc_introspection_cc_proto"],
    include_prefix = "kudu/rpc",
    visibility = ["//visibility:public"],
    deps = [
        ":rpc_header_proto",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_library(
    name = "rpc_introspection_proto_exported",
    srcs = [":rpc_introspection_cc_proto"],
    hdrs = [":rpc_introspection_cc_proto"],
    copts = [
        "-UKUDU_STATIC_DEFINE",
        "-UTCMALLOC_ENABLED",
        "-fvisibility=hidden",
        "-fvisibility-inlines-hidden",
    ],
    include_prefix = "kudu/rpc",
    visibility = ["//visibility:public"],
    deps = [
        ":rpc_header_proto_exported",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_library(
    name = "krpc",
    srcs = [
        "acceptor_pool.cc",
        "blocking_ops.cc",
        "client_negotiation.cc",
        "connection.cc",
        "connection_id.cc",
        "constants.cc",
        "inbound_call.cc",
        "messenger.cc",
        "negotiation.cc",
        "outbound_call.cc",
        "periodic.cc",
        "proxy.cc",
        "reactor.cc",
        "remote_method.cc",
        "remote_user.cc",
        "request_tracker.cc",
        "result_tracker.cc",
        "rpc.cc",
        "rpc_compression.cc",
        "rpc_context.cc",
        "rpc_controller.cc",
        "rpc_sidecar.cc",
        "rpc_verification_util.cc",
        "rpcz_store.cc",
        "sasl_common.cc",
        "sasl_helper.cc",
        "serialization.cc",
        "server_negotiation.cc",
        "service_if.cc",
        "service_pool.cc",
        "service_queue.cc",
        "transfer.cc",
        "user_credentials.cc",
    ],
    hdrs = glob(
        ["*.h"],
        exclude = ["*test*.h"],
    ),
    include_prefix = "kudu/rpc",
    visibility = ["//visibility:public"],
    deps = [
        ":rpc_header_proto",
        ":rpc_introspection_proto",
        "//src/kudu/security",
        "//src/kudu/util:headers",
        "//src/kudu/util:kudu_util",
        "//src/kudu/util:kudu_util_compression",
        "@snappy",
    ],
)

cc_library(
    name = "krpc_exported",
    srcs = [
        "acceptor_pool.cc",
        "blocking_ops.cc",
        "client_negotiation.cc",
        "connection.cc",
        "connection_id.cc",
        "constants.cc",
        "inbound_call.cc",
        "messenger.cc",
        "negotiation.cc",
        "outbound_call.cc",
        "periodic.cc",
        "proxy.cc",
        "reactor.cc",
        "remote_method.cc",
        "remote_user.cc",
        "request_tracker.cc",
        "result_tracker.cc",
        "rpc.cc",
        "rpc_compression.cc",
        "rpc_context.cc",
        "rpc_controller.cc",
        "rpc_sidecar.cc",
        "rpc_verification_util.cc",
        "rpcz_store.cc",
        "sasl_common.cc",
        "sasl_helper.cc",
        "serialization.cc",
        "server_negotiation.cc",
        "service_if.cc",
        "service_pool.cc",
        "service_queue.cc",
        "transfer.cc",
        "user_credentials.cc",
    ],
    hdrs = glob(
        ["*.h"],
        exclude = ["*test*.h"],
    ),
    copts = [
        "-UKUDU_STATIC_DEFINE",
        "-UTCMALLOC_ENABLED",
        "-fvisibility=hidden",
        "-fvisibility-inlines-hidden",
    ],
    include_prefix = "kudu/rpc",
    visibility = ["//visibility:public"],
    deps = [
        ":rpc_header_proto_exported",
        ":rpc_introspection_proto_exported",
        "//src/kudu/security:security_exported",
        "//src/kudu/util:headers_exported",
        "//src/kudu/util:kudu_util_exported",
        "//src/kudu/util:kudu_util_compression_exported",
        "@snappy",
    ],
)

cc_library(
    name = "headers",
    hdrs = glob(
        ["*.h"],
        exclude = ["*test*.h"],
    ),
    include_prefix = "kudu/rpc",
    visibility = ["//visibility:public"],
    deps = [
        ":rpc_header_proto",
    ],
)

cc_library(
    name = "headers_exported",
    hdrs = glob(
        ["*.h"],
        exclude = ["*test*.h"],
    ),
    copts = [
        "-UKUDU_STATIC_DEFINE",
        "-UTCMALLOC_ENABLED",
        "-fvisibility=hidden",
        "-fvisibility-inlines-hidden",
    ],
    include_prefix = "kudu/rpc",
    visibility = ["//visibility:public"],
    deps = [
        ":rpc_header_proto_exported",
    ],
)

cc_binary(
    name = "protoc-gen-krpc",
    srcs = ["protoc-gen-krpc.cc"],
    visibility = ["//visibility:public"],
    deps = [
        ":rpc_header_proto",
        "//src/kudu/gutil",
        "@gperf",
        "@libunwind",
        "@boost",
        "@com_google_protobuf//:protobuf",
        "@com_google_protobuf//:protoc_lib",
        "@gflags",
        "@glog",
    ],
)
