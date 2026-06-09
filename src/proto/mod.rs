#![allow(dead_code)]
// This file is kept in sync with replication.proto by hand (no build.rs).
// It contains prost-derived Rust types for every proto message.

// ----- Vector clock ----------------------------------------------------

/// A protobuf-native version vector used in pull requests and piggybacked VV
/// fields of CrdtOp. Keys are node-ids; values are causal counters.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct VectorClock {
    #[prost(map = "string, uint64", tag = "1")]
    pub entries: ::std::collections::HashMap<::prost::alloc::string::String, u64>,
}

// ----- Top-level envelope ----------------------------------------------

/// `Eq` / `Hash` are intentionally omitted because `VectorClock` contains a
/// `HashMap` which does not implement those traits.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Envelope {
    #[prost(oneof = "envelope::Payload", tags = "1, 4, 5")]
    pub payload: ::core::option::Option<envelope::Payload>,
}
/// Nested message and enum types in `Envelope`.
pub mod envelope {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Payload {
        #[prost(message, tag = "1")]
        Handshake(super::Handshake),
        #[prost(message, tag = "4")]
        CrdtOp(super::CrdtOp),
        #[prost(message, tag = "5")]
        CrdtPullRequest(super::CrdtPullRequest),
    }
}

// ----- Handshake -------------------------------------------------------

#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct Handshake {
    /// Globally unique node identifier (UUID recommended).
    #[prost(string, tag = "1")]
    pub node_id: ::prost::alloc::string::String,
    /// Human-readable name (optional).
    #[prost(string, tag = "2")]
    pub node_name: ::prost::alloc::string::String,
    /// Protocol version for forward-compat checks.
    #[prost(uint32, tag = "3")]
    pub version: u32,
    #[prost(bool, tag = "4")]
    pub gc_replica: bool,
}

// ----- CRDT operations -------------------------------------------------

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CrdtOp {
    /// Which CRDT instance this operation targets.
    #[prost(string, tag = "1")]
    pub crdt_id: ::prost::alloc::string::String,
    /// Opaque, serialised operation payload.  The CRDT layer will
    /// deserialise this according to the crdt_id's registered type.
    #[prost(bytes = "vec", tag = "2")]
    pub payload: ::prost::alloc::vec::Vec<u8>,
    /// Originating node so we can break ties / attribute ops.
    #[prost(string, tag = "4")]
    pub origin_node_id: ::prost::alloc::string::String,
    /// Optional piggybacked VV request.  When present, the receiver should
    /// respond with a delta since this knowledge vector, as if it had
    /// received a separate CrdtPullRequest.
    #[prost(message, optional, tag = "5")]
    pub requester_knowledge: ::core::option::Option<VectorClock>,
    /// Optional knowledge matrix: node-id -> VectorClock.
    #[prost(message, optional, tag = "6")]
    pub knowledge_matrix: ::core::option::Option<crdt_op::KnowledgeMatrix>,
}

/// Pull request: asks the remote to send the delta since our knowledge vector.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CrdtPullRequest {
    /// Which CRDT instance this pull targets.
    #[prost(string, tag = "1")]
    pub crdt_id: ::prost::alloc::string::String,
    /// Originating node.
    #[prost(string, tag = "2")]
    pub origin_node_id: ::prost::alloc::string::String,
    /// Protobuf-encoded knowledge map.
    #[prost(message, optional, tag = "3")]
    pub knowledge: ::core::option::Option<VectorClock>,
    /// If true, request that the remote performs replica GC (implementation-defined).
    #[prost(bool, tag = "4")]
    pub gc_replica: bool,
}

/// Nested message and enum types in `CrdtPullRequest`.
pub mod crdt_op {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct KnowledgeMatrix {
        #[prost(map = "string, message", tag = "1")]
        pub entries:
            ::std::collections::HashMap<::prost::alloc::string::String, super::VectorClock>,
    }
}

// ----- CRDT delta / op wire types -------------------------------------

/// A single causal event identifier (node, counter).
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct ProtoDot {
    #[prost(string, tag = "1")]
    pub node_id: ::prost::alloc::string::String,
    #[prost(uint64, tag = "2")]
    pub counter: u64,
}

/// One (element-bytes, dot) entry inside an OR-Set delta.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ProtoElemDot {
    #[prost(bytes = "vec", tag = "1")]
    pub element: ::prost::alloc::vec::Vec<u8>,
    #[prost(message, optional, tag = "2")]
    pub dot: ::core::option::Option<ProtoDot>,
}

/// A tombstone pair: the add-event dot and the remove-event dot.
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct ProtoDotPair {
    #[prost(message, optional, tag = "1")]
    pub add_dot: ::core::option::Option<ProtoDot>,
    #[prost(message, optional, tag = "2")]
    pub remove_dot: ::core::option::Option<ProtoDot>,
}

/// Wire representation of OrSetDelta<E>.
/// Elements are opaque bytes; the concrete CRDT impl encodes/decodes them.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ProtoOrSetDelta {
    #[prost(message, repeated, tag = "1")]
    pub adds: ::prost::alloc::vec::Vec<ProtoElemDot>,
    #[prost(message, repeated, tag = "2")]
    pub tombstones: ::prost::alloc::vec::Vec<ProtoDotPair>,
    #[prost(map = "string, uint64", tag = "3")]
    pub context: ::std::collections::HashMap<::prost::alloc::string::String, u64>,
    #[prost(string, tag = "4")]
    pub dot_node: ::prost::alloc::string::String,
    #[prost(uint64, tag = "5")]
    pub dot_counter: u64,
}

/// Wire representation of OrSetOp<E>.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ProtoOrSetOp {
    #[prost(oneof = "proto_or_set_op::Op", tags = "1, 2")]
    pub op: ::core::option::Option<proto_or_set_op::Op>,
}
pub mod proto_or_set_op {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Op {
        #[prost(bytes, tag = "1")]
        AddElement(::prost::alloc::vec::Vec<u8>),
        #[prost(bytes, tag = "2")]
        RemoveElement(::prost::alloc::vec::Vec<u8>),
    }
}

// ----- Client commands ------------------------------------------------

/// Commands sent from the CLI client to the server over TCP.
/// Framing: 4-byte big-endian length prefix followed by the prost-encoded bytes.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ProtoClientCommand {
    #[prost(oneof = "proto_client_command::Command", tags = "1, 2, 3, 4, 5")]
    pub command: ::core::option::Option<proto_client_command::Command>,
}
pub mod proto_client_command {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Command {
        #[prost(string, tag = "1")]
        Add(::prost::alloc::string::String),
        #[prost(string, tag = "2")]
        Remove(::prost::alloc::string::String),
        #[prost(bool, tag = "3")]
        PrintState(bool),
        #[prost(bool, tag = "4")]
        PrintInternals(bool),
        #[prost(bool, tag = "5")]
        PrintMatrix(bool),
    }
}
// @@protoc_insertion_point(module)
