#include "raii.hpp"
#include <cassert>
#include <type_traits>

struct Message { int value; };
struct OtherMessage { int value; };
static int allocations = 0;
static int frees = 0;
static int deserializations = 0;
static bool fail_allocation = false;

Message* MessageTypeSupport_alloc() {
    if(fail_allocation)
        return nullptr;
    ++allocations;
    return new Message{42};
}
void MessageTypeSupport_free(Message* sample) {
    ++frees;
    delete sample;
}
Message* MessageTypeSupport_deserialize(void* input, size_t size) {
    ++deserializations;
    return input && size == sizeof(Message) ? MessageTypeSupport_alloc() : nullptr;
}
OtherMessage* OtherSupport_alloc() { return new OtherMessage{7}; }
void OtherSupport_free(OtherMessage* sample) { delete sample; }
OtherMessage* OtherSupport_deserialize(void*, size_t) { return OtherSupport_alloc(); }

RAII_TYPESUPPORT_DEFINE(MessageTypeSupport, Message)
RAII_TYPESUPPORT_DEFINE(OtherSupport, OtherMessage)

static void native_write(const Message* sample) {
    assert(sample && sample->value == 42);
}

int main() {
    static_assert(!std::is_copy_constructible<raii::Message>::value, "move-only owner");
    static_assert(std::is_move_constructible<raii::Message>::value, "movable owner");
    {
        auto sample = raii::MessageTypeSupport_alloc();
        assert(sample && sample->value == 42);
        native_write(sample);
        auto moved = std::move(sample);
        assert(!sample && moved);
    }
    assert(allocations == 1 && frees == 1);
    {
        auto sample = raii::MessageTypeSupport_alloc();
        raii::MessageTypeSupport_free(sample);
        assert(!sample && frees == 2);
        raii::MessageTypeSupport_free(sample);
        assert(frees == 2);
    }
    assert(frees == 2);
    {
        Message input{42};
        auto sample = raii::MessageTypeSupport_deserialize(&input, sizeof(input));
        assert(sample && deserializations == 1);
        native_write(sample);
        // release() transfers ownership to the original API.
        auto raw = sample.release();
        assert(!sample && frees == 2);
        ::MessageTypeSupport_free(raw);
    }
    assert(allocations == 3 && frees == 3);
    assert(!raii::MessageTypeSupport_deserialize(nullptr, 0));
    fail_allocation = true;
    assert(!raii::MessageTypeSupport_alloc());
    Message input{42};
    assert(!raii::MessageTypeSupport_deserialize(&input, sizeof(input)));
    assert(frees == 3);
    auto other = raii::OtherSupport_alloc();
    assert(other && other->value == 7);
}
