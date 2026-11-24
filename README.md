# GurumDDS RAII

[한국어](README.kr.md)

`raii.hpp` is a header-only C++11 wrapper that manages resources from the GurumDDS C API. Use qualified `raii::dds_*` functions to create, release, or replace managed resources. Pass the handles directly to native `::dds_*` functions that only borrow them.

## Installation

Add `raii.hpp` to your include path. You need a C++11 or later compiler with exceptions enabled and a GurumDDS SDK that provides the C APIs used by the header. Link your application against the GurumDDS C library.

If the SDK headers and library are in the system search paths, build the example with the following command. For a custom SDK location, add the corresponding include and library paths.

```sh
c++ -std=c++11 -I/path/to/raii main.cpp -lgurumdds -o example
```

## Quick start

Save this example as `main.cpp`. It creates a managed participant and passes it to a native C operation.

```cpp
#include "raii.hpp"

#include <cstdlib>

int main() {
	auto* factory{raii::dds_DomainParticipantFactory_get_instance()};
	if(!factory)
		return EXIT_FAILURE;

	raii::dds_DomainParticipantQos qos{};
	const dds_ReturnCode_t result{raii::dds_DomainParticipantFactory_get_default_participant_qos(factory, qos)};
	if(result != dds_RETCODE_OK)
		return EXIT_FAILURE;

	const dds_DomainId_t domainId{0};
	auto participant{raii::dds_DomainParticipantFactory_create_participant(factory, domainId, qos, nullptr, 0)};
	if(!participant)
		return EXIT_FAILURE;

	const dds_DomainId_t actualDomainId{::dds_DomainParticipant_get_domain_id(participant)};
	if(actualDomainId != domainId)
		return EXIT_FAILURE;

	return EXIT_SUCCESS;
}
```

## Ownership and lifetime

| Handle | Ownership |
| --- | --- |
| DDS entities, such as Participant, Topic, and DataReader | Copyable handles that share ownership. Child entities retain the managed objects they depend on. |
| Resources, such as sequences, samples, and DynamicType | Move-only owners. Use `get()` to borrow a pointer and `close()` to check the result of explicit cleanup. |
| QoS and descriptors | Move-only wrappers that finalize their native values. Use the wrapper copy functions for a deep copy and initialize QoS with the appropriate default-QoS accessor. |

Keep an owner alive while using a borrowed pointer. Use the matching `raii::dds_*_delete_*` function for explicit entity deletion and check the return code. Other owners can prevent deletion with `dds_RETCODE_PRECONDITION_NOT_MET`. Native delete functions and `delete_contained_entities` bypass wrapper ownership. Only pass owning pointers to resource constructors, and use `release()` only for independently transferable resources.

Listener context pointers and objects accessed through them are borrowed. Keep them valid until callbacks have finished. The wrapper does not wait for in-flight callbacks. Synchronize shared access to handles and native resources as required.

Native creation failures that return null yield empty handles. C++ allocation failures can throw. Check entity handles before implicit native pointer conversion, which throws for an empty or closed entity. A checked native cleanup failure in a destructor calls `std::terminate()`.

Obtaining the Participant Factory through `raii::dds_DomainParticipantFactory_get_instance()` registers normal-exit cleanup for the DDS factories. Release DDS resources and stop threads that use DDS before factory cleanup runs.

## Loans and WaitSets

- A managed Reader sequence loan retains its Reader and is returned when a participating sequence is destroyed or reused. Use the matching wrapper return-loan function to check the result of an explicit return.
- Acquire a Writer loan with `raii::dds_DataWriter_get_loan(writer, &sample, &capacity)` using a `raii::dds_WriterLoan` sample. Return it with `raii::dds_DataWriter_return_loaned_sample()` or pass it to `raii::dds_DataWriter_raw_write()`. The write wrapper returns the original loan before the native write, so a native write failure leaves the loan returned.
- A WaitSet retains conditions attached through the wrapper until they are detached or the WaitSet is destroyed.

## IDL-generated samples

Include the generated TypeSupport header and invoke `RAII_TYPESUPPORT_DEFINE` once per type at global scope outside any `extern "C"` block. The macro defines a move-only sample owner and wrapper functions for allocation, deserialization, and cleanup. Use the generated APIs for type registration and read or write operations.

```cpp
#include "FooTypeSupport.h"
#include "raii.hpp"

RAII_TYPESUPPORT_DEFINE(FooTypeSupport, Foo)
```

For example, `raii::FooTypeSupport_alloc()` returns a `raii::Foo` handle that releases the sample at scope exit. Initialize the sample according to the generated API before writing it. The generated TypeSupport singleton remains borrowed.

## License

Copyright 2026 GurumNetworks, Inc.

This wrapper is licensed under the [Apache License, Version 2.0](LICENSE). The GurumDDS SDK is subject to its own license terms.
