# GurumDDS RAII

A header-only C++11 RAII wrapper for the GurumDDS C API, providing automatic resource management and native API interoperability.

`raii.hpp` adds ownership and lifetime management to the existing C API. Create resources with qualified `raii::dds_*` functions, then pass the resulting handles to native `::dds_*` functions when needed.

## Features

- Shared ownership of DDS entities, with child handles retaining the parents they depend on.
- Move-only resource owners for sequences, samples, TypeSupport, dynamic types, and allocated memory.
- Scoped QoS and descriptor objects with automatic finalization.
- Reader and writer loan adapters, including automatic return of managed sequence loans.
- WaitSet attachments that retain managed conditions until detached or released with the WaitSet.
- Implicit conversion to native C pointers for interoperability with the GurumDDS C API.
- Automatic singleton cleanup at normal program exit.

## Requirements

- A C++11 or later compiler with exception support.
- GurumDDS C API headers and library, installed separately.

The SDK must provide the APIs referenced by `raii.hpp`. Compatibility with every GurumDDS release is not guaranteed. This wrapper does not include the SDK or replace its runtime requirements.

## Installation

Copy `raii.hpp` into your project's include path and link your application against the GurumDDS C library. The wrapper itself requires no separate compilation.

For a Linux installation with SDK headers and libraries in the system search paths:

```sh
c++ -std=c++11 -I/path/to/raii main.cpp -lgurumdds -o example
```

For other SDK layouts, add the appropriate include and library paths and follow the SDK's runtime library configuration instructions.

## Quick start

Save the following as `main.cpp`:

```cpp
#include "raii.hpp"

#include <cstdlib>
#include <exception>
#include <iostream>

int main() {
    try {
        auto* factory = raii::dds_DomainParticipantFactory_get_instance();
        if (!factory) {
            return EXIT_FAILURE;
        }

        raii::dds_DomainParticipantQos participant_qos;
        if (raii::dds_DomainParticipantFactory_get_default_participant_qos(
                factory, participant_qos) != dds_RETCODE_OK) {
            return EXIT_FAILURE;
        }

        auto participant = raii::dds_DomainParticipantFactory_create_participant(
            factory, 0, participant_qos, nullptr, 0);
        if (!participant) {
            return EXIT_FAILURE;
        }

        raii::dds_PublisherQos publisher_qos;
        if (raii::dds_DomainParticipant_get_default_publisher_qos(
                participant, publisher_qos) != dds_RETCODE_OK) {
            return EXIT_FAILURE;
        }

        auto publisher = raii::dds_DomainParticipant_create_publisher(
            participant, publisher_qos, nullptr, 0);
        if (!publisher) {
            return EXIT_FAILURE;
        }

        // Call native C operations through implicit pointer conversion.
        if (::dds_Publisher_enable(publisher) != dds_RETCODE_OK) {
            return EXIT_FAILURE;
        }

        // The publisher is released before the participant at scope exit.
        return EXIT_SUCCESS;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return EXIT_FAILURE;
    }
}
```

## Ownership and interoperability

Use qualified `raii::dds_*` names for resource creation and deletion. Other operations can use the native `::dds_*` API through implicit pointer conversion. Prefer explicit qualification over `using namespace raii` to keep wrapper and native calls clear.

| Wrapper category | Ownership behavior |
| --- | --- |
| Entity handles, such as `raii::dds_DomainParticipant` | Copyable handles sharing the same managed entity. Children retain required parent handles. |
| `raii::Resource<T, Deleter>` aliases | Move-only owners. Use `get()` for a borrowed pointer, `reset()` to release ownership, and `close()` for an explicit cleanup result. |
| QoS wrappers | Scoped objects that finalize their native values. They are neither copyable nor movable; use the provided copy functions. |

QoS wrappers start with zero-initialized storage. Obtain valid default QoS values using the corresponding `raii::dds_*_get_default_*_qos` function before creating an entity, as shown above.

A native pointer obtained from a handle is borrowed. Keep the owning handle alive for as long as that pointer is used. Resetting one entity handle does not destroy the entity while other handles or dependent resources still retain it.

For explicit entity deletion, use the matching `raii::dds_*_delete_*` function and check its return code. Deletion may return `dds_RETCODE_PRECONDITION_NOT_MET` while the entity is retained by another handle or dependent resource.

## IDL-generated samples

Include the generated TypeSupport header and invoke `RAII_TYPESUPPORT_DEFINE`
once per type in a C++ adapter header, at global scope outside any `extern "C"`
block. The generated C and header files do not need changes:

```cpp
#include "FooTypeSupport.h"
#include "raii.hpp"

RAII_TYPESUPPORT_DEFINE(FooTypeSupport, Foo)
```

This defines the move-only sample owner `raii::Foo` and exactly three wrappers:

| Wrapper | Behavior |
| --- | --- |
| `raii::FooTypeSupport_alloc()` | Calls generated `::FooTypeSupport_alloc()` and owns the returned sample. |
| `raii::FooTypeSupport_free(sample)` | Releases the sample owner; cleanup calls generated `::FooTypeSupport_free()`. |
| `raii::FooTypeSupport_deserialize(input, size)` | Calls generated deserialization and owns the returned sample. |

```cpp
auto sample = raii::FooTypeSupport_alloc();
if (!sample)
    return EXIT_FAILURE;

// Existing operations accept the owner's implicit Foo* conversion.
::FooDataWriter_write(writer, sample, dds_HANDLE_NIL);

// Optional explicit release; otherwise cleanup happens at scope exit.
raii::FooTypeSupport_free(sample);

auto decoded = raii::FooTypeSupport_deserialize(buffer, size);
```

Allocation and deserialization failures produce empty owners. Moving an owner
transfers its cleanup responsibility. Initialize allocated data according to the
generated API before passing it to a writer. The macro preserves the generated
allocator/deserializer behavior.

Registration, type-name lookup, serialization, and DataWriter/DataReader operations
continue to use their existing APIs. Resources returned by those APIs must still
be managed according to their own ownership rules. Do not call the native sample
free function on a managed sample unless ownership was transferred with
`sample.release()`.

The macro manages samples only. The generated TypeSupport singleton remains
borrowed and is never adopted or deleted by these wrappers. No metadata access,
new TypeSupport instances, or Participant lifetime changes are introduced.

## Factory lifetime

Use the wrapper accessor to obtain the Participant Factory singleton:

```cpp
auto* factory = raii::dds_DomainParticipantFactory_get_instance();
```

The first successful call registers automatic cleanup at normal program exit. Repeated calls return the same native singleton without registering additional cleanup. No explicit `shutdown()` call is needed when using this accessor.

Release DDS resources and stop DDS worker threads before program exit. The wrapper then cleans up the DynamicData and DynamicTypeBuilder factories before shutting down the Participant Factory. Global or static DDS resources must also be destroyed before this automatic cleanup runs.

## Loans and resource release

Managed reader sequence loans are returned when their participating sequence owners are destroyed or reused. The active loan retains its DataReader. Use the matching wrapper return-loan function when you need to return a loan explicitly and inspect the result.

`Resource::release()` transfers a native pointer to the caller and disables automatic cleanup for that resource. It throws if a sequence loan is still active or another owner retains the resource state. After a successful transfer, the caller is responsible for the native resource's lifetime.

When transferring a writer loan to a native `raw_write` API, explicitly use `sample.release()` and follow that API's ownership rules. The wrapper does not infer loan ownership from a failed write's return code.

## Important behavior

- **Do not destroy managed resources through native delete functions or `delete_contained_entities`.** Native deletion cannot update outstanding C++ handles and may cause dangling pointers or double deletion.
- **Do not adopt borrowed pointers.** Native get, lookup, and find results must not be passed to owning resource constructors unless ownership is explicitly transferred by the API.
- **Listeners and context pointers remain borrowed.** Keep them alive while callbacks can access them. The wrapper does not own listener objects or synchronize in-flight callbacks.
- **Provide external synchronization.** Shared ownership does not make handle operations or native DDS resources thread-safe.
- **Destructor cleanup failures terminate the process.** For cleanup paths that check native return codes, a failure calls `std::terminate()`. Use `close()` or the corresponding explicit wrapper deletion function when you need to inspect cleanup errors before destruction.
- **Allocation can throw.** C++ allocation failures propagate as exceptions. Native create functions that return null produce empty handles. Converting an empty or closed entity handle to a native pointer throws `std::logic_error`; check the handle first or use `get()` to inspect it.
- **Some objects remain native handles.** DynamicTypeBuilder objects and singleton factory pointers are not independently owned by this wrapper. The Participant Factory singleton is cleaned up automatically when accessed through the wrapper.

## License

Copyright 2026 GurumNetworks, Inc.

This wrapper is licensed under the [Apache License, Version 2.0](LICENSE).

This license applies only to this wrapper. The GurumDDS SDK is subject to its own license terms and is not covered by this license.

