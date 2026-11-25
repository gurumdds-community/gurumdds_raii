# GurumDDS RAII

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

Native creation failures that return null yield empty handles. C++ allocation failures can throw. Check entity handles before implicit native pointer conversion, which throws for an empty or closed entity. A checked native cleanup failure in a destructor calls `std::terminate()`.

Obtaining the Participant Factory through `raii::dds_DomainParticipantFactory_get_instance()` registers normal-exit cleanup for the DDS factories. Release DDS resources and stop threads that use DDS before factory cleanup runs.

## Managed listeners

After creating a DataReader, DataWriter, Topic, Subscriber, Publisher, or DomainParticipant, use its `raii::dds_*Listener` and `raii::dds_*_set_listener()` to register a listener. The fields use the SDK callback names and argument types. Pass another listener to replace the current one, or `nullptr, 0` to remove it. Check the return code. A failed SDK call preserves the previous managed registration.

In this example, the application owns a `foo` that holds the Reader, and `get_foo()` returns a shared reference to it. The listener captures a `weak_ptr` to avoid an ownership cycle. Each successful `lock()` keeps the `foo` alive for that invocation.

```cpp
struct foo {
	raii::dds_DataReader reader;
	void on_data(const dds_DataReader* source);
};

std::shared_ptr<foo> get_foo();

void f() {
	const auto bar{get_foo()};
	if(!bar)
		return;

	const std::weak_ptr<foo> weak_foo{bar};
	raii::dds_DataReaderListener listener{};
	listener.on_data_available = [weak_foo](const dds_DataReader* source) {
		auto locked_foo{weak_foo.lock()};
		if(!locked_foo)
			return;

		locked_foo->on_data(source);
	};

	const dds_ReturnCode_t result{
	    raii::dds_DataReader_set_listener(bar->reader, &listener, dds_DATA_AVAILABLE_STATUS)};
	if(result != dds_RETCODE_OK)
		throw std::runtime_error("failed to set Reader listener");
}
```

The wrapper copies the listener and retains that copy and its value captures for each invocation that acquires it, even if the listener is subsequently replaced, removed, or the entity is deleted. The SDK selects the listener according to its status mask and propagation rules. An event queued by the SDK can reach the trampoline after a registration change and use the registration present at lookup time. Callbacks run outside the registration mutex. Synchronize access to shared callback data and keep exceptions inside the callback.

The wrapper does not wait for callbacks or extend the lifetime of their native DDS pointers. For example, the native Participant delete can join its callback thread, and holding the managed listener alive does not allow continued use of a Reader after native deletion. Use the managed setter consistently after installing a managed listener. Existing C listeners passed at creation and their native context pointers remain borrowed. Managed registration leaves the SDK context pointer untouched.

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
