# -*- coding: utf-8 -*-

# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: google/api/config_change.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder

# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(
    b'\n\x1egoogle/api/config_change.proto\x12\ngoogle.api"\x97\x01\n\x0c\x43onfigChange\x12\x0f\n\x07\x65lement\x18\x01 \x01(\t\x12\x11\n\told_value\x18\x02 \x01(\t\x12\x11\n\tnew_value\x18\x03 \x01(\t\x12+\n\x0b\x63hange_type\x18\x04 \x01(\x0e\x32\x16.google.api.ChangeType\x12#\n\x07\x61\x64vices\x18\x05 \x03(\x0b\x32\x12.google.api.Advice"\x1d\n\x06\x41\x64vice\x12\x13\n\x0b\x64\x65scription\x18\x02 \x01(\t*O\n\nChangeType\x12\x1b\n\x17\x43HANGE_TYPE_UNSPECIFIED\x10\x00\x12\t\n\x05\x41\x44\x44\x45\x44\x10\x01\x12\x0b\n\x07REMOVED\x10\x02\x12\x0c\n\x08MODIFIED\x10\x03\x42q\n\x0e\x63om.google.apiB\x11\x43onfigChangeProtoP\x01ZCgoogle.golang.org/genproto/googleapis/api/configchange;configchange\xa2\x02\x04GAPIb\x06proto3'
)

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(
    DESCRIPTOR, "google.api.config_change_pb2", _globals
)
if _descriptor._USE_C_DESCRIPTORS == False:
    DESCRIPTOR._options = None
    DESCRIPTOR._serialized_options = b"\n\016com.google.apiB\021ConfigChangeProtoP\001ZCgoogle.golang.org/genproto/googleapis/api/configchange;configchange\242\002\004GAPI"
    _globals["_CHANGETYPE"]._serialized_start = 231
    _globals["_CHANGETYPE"]._serialized_end = 310
    _globals["_CONFIGCHANGE"]._serialized_start = 47
    _globals["_CONFIGCHANGE"]._serialized_end = 198
    _globals["_ADVICE"]._serialized_start = 200
    _globals["_ADVICE"]._serialized_end = 229
# @@protoc_insertion_point(module_scope)
