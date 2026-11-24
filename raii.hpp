// Copyright 2026 GurumNetworks, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// This wrapper is licensed under the Apache License, Version 2.0.
// See the LICENSE file or https://www.apache.org/licenses/LICENSE-2.0.
//
// This license applies only to this wrapper. The GurumDDS SDK is subject
// to its own license terms and is not covered by this license.

#ifndef RAII_HPP
#define RAII_HPP

#include <gurumdds/dds.h>
// Header-only C++11 resource adapters for the supplied GurumDDS C API.
// Use qualified raii::dds_* names for allocation/deallocation. Other operations
// use the original ::dds_* functions through implicit native-pointer conversion.
// Do not delete managed pointers through ::dds_* or delete_contained_entities:
// the C API cannot notify outstanding C++ handles about external destruction.
// Native listeners/context pointers remain borrowed, as in the C API.
// Handles and their native resources require external synchronization.
#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <exception>
#include <iostream>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

namespace raii {

class DomainParticipant;
class Publisher;
class Subscriber;
class Topic;
class ContentFilteredTopic;
class MultiTopic;
class DataWriter;
class DataReader;
class ReadCondition;
class QueryCondition;
class WaitSet;
class GuardCondition;

template <typename TImpl>
class DDSEntity;

namespace detail {

template <typename TImpl>
std::shared_ptr<TImpl>& SharedImpl(DDSEntity<TImpl>& h) noexcept;

template <typename TImpl>
const std::shared_ptr<TImpl>& SharedImpl(const DDSEntity<TImpl>& h) noexcept;

// Factory operations only coordinate participant creation, lookup and deletion.
// All child-entity lifetime operations use their owning participant's state.
inline std::recursive_mutex& GetFactoryMutex() {
	static std::recursive_mutex s_mutex;
	return s_mutex;
}

struct LifecycleState {
	std::recursive_mutex m_mutex;
	// Allocate only when find first waits, preserving participant construction.
	std::unique_ptr<std::condition_variable_any> m_changed;
	std::unordered_multimap<std::string, std::weak_ptr<Topic>> m_topics;
};

struct LifecycleLock;

inline std::shared_ptr<DomainParticipant> GetParticipant(
    const std::shared_ptr<DomainParticipant>& participant) noexcept;

template <typename TImpl>
std::shared_ptr<DomainParticipant> GetParticipant(const std::shared_ptr<TImpl>& impl) noexcept {
	if(!impl)
		return {};
	return GetParticipant(impl->m_parent);
}

inline void TerminateOnError(::dds_ReturnCode_t ret, const char* func) noexcept {
	if(ret != dds_RETCODE_OK) {
		std::cerr << "fail " << func << "() ret=" << ret << '\n';
		std::terminate();
	}
}

inline ::dds_ReturnCode_t BadParameter() noexcept {
	return dds_RETCODE_BAD_PARAMETER;
}

inline void ClearListener(::dds_DomainParticipant* p) noexcept {
	if(p == nullptr) {
		return;
	}
	(void)::dds_DomainParticipant_set_listener(p, nullptr, 0);
	(void)::dds_DomainParticipant_set_listener_context(p, nullptr);
}

inline void ClearListener(::dds_Topic* p) noexcept {
	if(p == nullptr) {
		return;
	}
	(void)::dds_Topic_set_listener(p, nullptr, 0);
	(void)::dds_Topic_set_listener_context(p, nullptr);
}

inline void ClearListener(::dds_Publisher* p) noexcept {
	if(p == nullptr) {
		return;
	}
	(void)::dds_Publisher_set_listener(p, nullptr, 0);
	(void)::dds_Publisher_set_listener_context(p, nullptr);
}

inline void ClearListener(::dds_Subscriber* p) noexcept {
	if(p == nullptr) {
		return;
	}
	(void)::dds_Subscriber_set_listener(p, nullptr, 0);
	(void)::dds_Subscriber_set_listener_context(p, nullptr);
}

inline void ClearListener(::dds_DataWriter* p) noexcept {
	if(p == nullptr) {
		return;
	}
	(void)::dds_DataWriter_set_listener(p, nullptr, 0);
	(void)::dds_DataWriter_set_listener_context(p, nullptr);
}

inline void ClearListener(::dds_DataReader* p) noexcept {
	if(p == nullptr) {
		return;
	}
	(void)::dds_DataReader_set_listener(p, nullptr, 0);
	(void)::dds_DataReader_set_listener_context(p, nullptr);
}

template <typename TValue>
inline TValue* ConditionCast(TValue* p) noexcept {
	return p;
}

inline ::dds_Condition* ConditionCast(::dds_GuardCondition* p) noexcept {
	return reinterpret_cast<::dds_Condition*>(p);
}

inline ::dds_Condition* ConditionCast(::dds_ReadCondition* p) noexcept {
	return reinterpret_cast<::dds_Condition*>(p);
}

inline ::dds_Condition* ConditionCast(::dds_QueryCondition* p) noexcept {
	return reinterpret_cast<::dds_Condition*>(p);
}

inline ::dds_Condition* ConditionCast(::dds_StatusCondition* p) noexcept {
	return reinterpret_cast<::dds_Condition*>(p);
}

inline const ::dds_TopicDescription* TopicDescriptionCast(const ::dds_Topic* p) noexcept {
	return reinterpret_cast<const ::dds_TopicDescription*>(p);
}

inline const ::dds_TopicDescription* TopicDescriptionCast(const ::dds_ContentFilteredTopic* p) noexcept {
	return reinterpret_cast<const ::dds_TopicDescription*>(p);
}

inline const ::dds_TopicDescription* TopicDescriptionCast(const ::dds_MultiTopic* p) noexcept {
	return reinterpret_cast<const ::dds_TopicDescription*>(p);
}

} // namespace detail

template <typename TImpl>
class DDSEntity {
public:
	using DDSType = typename TImpl::DDSType;
	using native_type = DDSType;

	DDSEntity() = default;

	DDSEntity(std::nullptr_t) noexcept {
	}

	explicit DDSEntity(std::shared_ptr<TImpl> impl)
	    : m_sharedptr(std::move(impl)) {
	}

	explicit operator bool() const noexcept {
		return m_sharedptr != nullptr && **m_sharedptr != nullptr;
	}

	bool operator==(std::nullptr_t) const noexcept {
		return !*this;
	}

	bool operator!=(std::nullptr_t) const noexcept {
		return static_cast<bool>(*this);
	}

	DDSType* get() const noexcept {
		if(!m_sharedptr)
			return nullptr;
		return **m_sharedptr;
	}

	bool operator==(const DDSEntity& other) const noexcept {
		return m_sharedptr == other.m_sharedptr;
	}

	bool operator!=(const DDSEntity& other) const noexcept {
		return !(*this == other);
	}

	DDSEntity& operator=(std::nullptr_t) noexcept {
		m_sharedptr.reset();
		return *this;
	}

	operator DDSType*() const {
		RequireValid();
		return **m_sharedptr;
	}

	DDSType* operator*() {
		RequireValid();
		return **m_sharedptr;
	}

	const DDSType* operator*() const {
		RequireValid();
		return **m_sharedptr;
	}

private:
	void RequireValid() const {
		if(m_sharedptr == nullptr) {
			throw std::logic_error("DDS entity handle is null");
		}
		if(**m_sharedptr == nullptr) {
			throw std::logic_error("DDS entity is already closed");
		}
	}

private:
	std::shared_ptr<TImpl> m_sharedptr;

	friend std::shared_ptr<TImpl>& detail::SharedImpl<TImpl>(DDSEntity<TImpl>& h) noexcept;
	friend const std::shared_ptr<TImpl>& detail::SharedImpl<TImpl>(const DDSEntity<TImpl>& h) noexcept;
};

namespace detail {

template <typename TImpl>
std::shared_ptr<TImpl>& SharedImpl(DDSEntity<TImpl>& h) noexcept {
	return h.m_sharedptr;
}

template <typename TImpl>
const std::shared_ptr<TImpl>& SharedImpl(const DDSEntity<TImpl>& h) noexcept {
	return h.m_sharedptr;
}

template <typename TImpl, typename TRaw, typename... TArgs>
std::shared_ptr<TImpl> MakeOwned(TRaw* raw, const TArgs&... args) {
	try {
		return std::make_shared<TImpl>(raw, args...);
	} catch(...) {
		TImpl cleanup(raw, args...);
		throw;
	}
}

// Weak entries never keep entities alive. Lifecycle operations acquire the
// lifecycle mutex before a registry mutex. Raw C API calls remain externally synchronized.
template <typename TImpl>
struct OwnerRegistry {
	std::mutex m_mutex;
	std::unordered_map<const void*, std::weak_ptr<TImpl>> m_owners;
};

template <typename TImpl>
OwnerRegistry<TImpl>& GetOwnerRegistry() {
	static OwnerRegistry<TImpl> s_registry;
	return s_registry;
}

template <typename TImpl, typename TRaw>
std::shared_ptr<TImpl> RegisteredOwner(TRaw* raw) {
	if(!raw)
		return {};
	auto& registry = GetOwnerRegistry<TImpl>();
	std::lock_guard<std::mutex> lock(registry.m_mutex);
	auto it = registry.m_owners.find(raw);
	if(it == registry.m_owners.end())
		return {};
	auto owner = it->second.lock();
	if(!owner || **owner != raw) {
		registry.m_owners.erase(it);
		return {};
	}
	return owner;
}

template <typename TImpl, typename TRaw>
void RegisterOwner(TRaw* raw, const std::shared_ptr<TImpl>& owner) {
	auto& registry = GetOwnerRegistry<TImpl>();
	std::lock_guard<std::mutex> lock(registry.m_mutex);
	// Remove expired control blocks so repeated create/delete does not grow it.
	for(auto it = registry.m_owners.begin(); it != registry.m_owners.end();) {
		if(it->second.expired())
			it = registry.m_owners.erase(it);
		else
			++it;
	}
	registry.m_owners[raw] = owner;
}

template <typename TImpl, typename TRaw, typename... TArgs>
std::shared_ptr<TImpl> MakeRegisteredOwned(TRaw* raw, const TArgs&... args) {
	auto owner = MakeOwned<TImpl>(raw, args...);
	RegisterOwner(raw, owner);
	return owner;
}

} // namespace detail

using dds_DomainParticipant = DDSEntity<DomainParticipant>;
using dds_Publisher = DDSEntity<Publisher>;
using dds_Subscriber = DDSEntity<Subscriber>;
using dds_Topic = DDSEntity<Topic>;
using dds_ContentFilteredTopic = DDSEntity<ContentFilteredTopic>;
using dds_MultiTopic = DDSEntity<MultiTopic>;
using dds_DataWriter = DDSEntity<DataWriter>;
using dds_DataReader = DDSEntity<DataReader>;
using dds_ReadCondition = DDSEntity<ReadCondition>;
using dds_QueryCondition = DDSEntity<QueryCondition>;
using dds_WaitSet = DDSEntity<WaitSet>;
using dds_GuardCondition = DDSEntity<GuardCondition>;

#define RAII_DDS_IMPL_OPERATORS(ImplType, RawType)                                                                     \
public:                                                                                                                \
	ImplType(const ImplType&) = delete;                                                                                \
	ImplType& operator=(const ImplType&) = delete;                                                                     \
	using DDSType = RawType;                                                                                           \
	ImplType& operator=(std::nullptr_t) noexcept {                                                                     \
		m_entity = nullptr;                                                                                            \
		return *this;                                                                                                  \
	}                                                                                                                  \
	RawType* operator*() noexcept {                                                                                    \
		return m_entity;                                                                                               \
	}                                                                                                                  \
	const RawType* operator*() const noexcept {                                                                        \
		return m_entity;                                                                                               \
	}                                                                                                                  \
                                                                                                                       \
private:                                                                                                               \
	template <typename TImpl>                                                                                          \
	friend std::shared_ptr<DomainParticipant> detail::GetParticipant(const std::shared_ptr<TImpl>&) noexcept;          \
	template <typename>                                                                                                \
	friend class ::raii::DDSEntity

class DomainParticipant {
	RAII_DDS_IMPL_OPERATORS(DomainParticipant, ::dds_DomainParticipant);

public:
	explicit DomainParticipant(::dds_DomainParticipant* entity) noexcept
	    : m_entity(entity) {
	}

	~DomainParticipant() noexcept {
		std::lock_guard<std::recursive_mutex> lifecycleLock(m_lifecycle.m_mutex);
		std::lock_guard<std::recursive_mutex> factoryLock(detail::GetFactoryMutex());
		if(m_entity == nullptr) {
			return;
		}
		detail::ClearListener(m_entity);
		::dds_DomainParticipantFactory* dpf = ::dds_DomainParticipantFactory_get_instance();
		::dds_ReturnCode_t ret = ::dds_DomainParticipantFactory_delete_participant(dpf, m_entity);
		detail::TerminateOnError(ret, "dds_DomainParticipantFactory_delete_participant");
		m_entity = nullptr;
	}

private:
	detail::LifecycleState m_lifecycle;
	friend struct detail::LifecycleLock;
	::dds_DomainParticipant* m_entity{nullptr};
};

namespace detail {
inline std::shared_ptr<DomainParticipant> GetParticipant(
    const std::shared_ptr<DomainParticipant>& participant) noexcept {
	return participant;
}

// The guard pins the participant so its mutex survives handle reset/deletion.
// Member order releases the mutex before releasing the participant pin.
struct LifecycleLock {
	std::shared_ptr<DomainParticipant> m_participant;
	std::unique_lock<std::recursive_mutex> m_lock;

	explicit LifecycleLock(std::shared_ptr<DomainParticipant> participant)
	    : m_participant(std::move(participant)) {
		if(m_participant)
			m_lock = std::unique_lock<std::recursive_mutex>(m_participant->m_lifecycle.m_mutex);
	}

	void RegisterTopic(const char* name, const std::shared_ptr<Topic>& topic) {
		auto& lifecycle = m_participant->m_lifecycle;
		for(auto it = lifecycle.m_topics.begin(); it != lifecycle.m_topics.end();) {
			if(it->second.expired())
				it = lifecycle.m_topics.erase(it);
			else
				++it;
		}
		lifecycle.m_topics.emplace(name, topic);
		if(lifecycle.m_changed)
			lifecycle.m_changed->notify_all();
	}

	std::shared_ptr<Topic> FindTopic(const std::string& name) const {
		const auto candidates{m_participant->m_lifecycle.m_topics.equal_range(name)};
		for(auto it = candidates.first; it != candidates.second; ++it) {
			if(auto topic = it->second.lock())
				return topic;
		}
		return {};
	}

	void Wait() {
		GetChanged().wait(m_lock);
	}

	void WaitUntil(const std::chrono::steady_clock::time_point& deadline) {
		GetChanged().wait_until(m_lock, deadline);
	}

private:
	std::condition_variable_any& GetChanged() {
		auto& changed = m_participant->m_lifecycle.m_changed;
		if(!changed)
			changed.reset(new std::condition_variable_any);
		return *changed;
	}
};

template <typename TImpl>
LifecycleLock LockLifecycle(const std::shared_ptr<TImpl>& impl) {
	return LifecycleLock(GetParticipant(impl));
}

} // namespace detail

class Topic {
	RAII_DDS_IMPL_OPERATORS(Topic, ::dds_Topic);

public:
	Topic(::dds_Topic* entity, std::shared_ptr<DomainParticipant> parent) noexcept
	    : m_parent(std::move(parent))
	    , m_entity(entity) {
	}

	~Topic() noexcept {
		auto lifecycleLock = detail::LockLifecycle(m_parent);
		if(m_entity == nullptr) {
			return;
		}
		detail::ClearListener(m_entity);
		if(m_parent && **m_parent != nullptr) {
			::dds_ReturnCode_t ret = ::dds_DomainParticipant_delete_topic(**m_parent, m_entity);
			detail::TerminateOnError(ret, "dds_DomainParticipant_delete_topic");
		}
		m_entity = nullptr;
	}

private:
	std::shared_ptr<DomainParticipant> m_parent;
	::dds_Topic* m_entity{nullptr};
};

class ContentFilteredTopic {
	RAII_DDS_IMPL_OPERATORS(ContentFilteredTopic, ::dds_ContentFilteredTopic);

public:
	ContentFilteredTopic(::dds_ContentFilteredTopic* entity,
	    std::shared_ptr<DomainParticipant> parent,
	    std::shared_ptr<Topic> relatedTopic) noexcept
	    : m_parent(std::move(parent))
	    , m_relatedTopic(std::move(relatedTopic))
	    , m_entity(entity) {
	}

	~ContentFilteredTopic() noexcept {
		auto lifecycleLock = detail::LockLifecycle(m_parent);
		if(m_entity == nullptr) {
			return;
		}
		if(m_parent && **m_parent != nullptr) {
			::dds_ReturnCode_t ret = ::dds_DomainParticipant_delete_contentfilteredtopic(**m_parent, m_entity);
			detail::TerminateOnError(ret, "dds_DomainParticipant_delete_contentfilteredtopic");
		}
		m_entity = nullptr;
	}

private:
	std::shared_ptr<DomainParticipant> m_parent;
	std::shared_ptr<Topic> m_relatedTopic;
	::dds_ContentFilteredTopic* m_entity{nullptr};
};

class MultiTopic {
	RAII_DDS_IMPL_OPERATORS(MultiTopic, ::dds_MultiTopic);

public:
	MultiTopic(::dds_MultiTopic* entity, std::shared_ptr<DomainParticipant> parent) noexcept
	    : m_parent(std::move(parent))
	    , m_entity(entity) {
	}

	~MultiTopic() noexcept {
		auto lifecycleLock = detail::LockLifecycle(m_parent);
		if(m_entity == nullptr) {
			return;
		}
		if(m_parent && **m_parent != nullptr) {
			::dds_ReturnCode_t ret = ::dds_DomainParticipant_delete_multitopic(**m_parent, m_entity);
			detail::TerminateOnError(ret, "dds_DomainParticipant_delete_multitopic");
		}
		m_entity = nullptr;
	}

private:
	std::shared_ptr<DomainParticipant> m_parent;
	::dds_MultiTopic* m_entity{nullptr};
};

// A TopicDescription is an interface to one of these three topic types.
// This handle shares the original owner's control block, never a new deleter.
class dds_TopicDescription {
public:
	dds_TopicDescription() noexcept = default;

	dds_TopicDescription(std::nullptr_t) noexcept {
	}

	dds_TopicDescription(const dds_TopicDescription&) noexcept = default;
	dds_TopicDescription& operator=(const dds_TopicDescription&) noexcept = default;

	dds_TopicDescription(dds_TopicDescription&& other) noexcept
	    : m_owner(std::move(other.m_owner))
	    , m_raw(other.m_raw)
	    , m_kind(other.m_kind) {
		other.m_raw = nullptr;
		other.m_kind = Kind::None;
	}

	dds_TopicDescription& operator=(dds_TopicDescription&& other) noexcept {
		if(this != &other) {
			m_owner = std::move(other.m_owner);
			m_raw = other.m_raw;
			m_kind = other.m_kind;
			other.m_raw = nullptr;
			other.m_kind = Kind::None;
		}
		return *this;
	}

	dds_TopicDescription(const dds_Topic& topic) noexcept
	    : m_owner(detail::SharedImpl(topic))
	    , m_raw(topic.get())
	    , m_kind(Kind::Topic) {
	}

	dds_TopicDescription(const dds_ContentFilteredTopic& topic) noexcept
	    : m_owner(detail::SharedImpl(topic))
	    , m_raw(topic.get())
	    , m_kind(Kind::Filtered) {
	}

	dds_TopicDescription(const dds_MultiTopic& topic) noexcept
	    : m_owner(detail::SharedImpl(topic))
	    , m_raw(topic.get())
	    , m_kind(Kind::Multi) {
	}

	::dds_TopicDescription* get() const noexcept {
		return reinterpret_cast<::dds_TopicDescription*>(m_raw);
	}

	operator ::dds_TopicDescription*() const noexcept {
		return get();
	}

	explicit operator bool() const noexcept {
		return m_raw != nullptr;
	}

	bool operator==(std::nullptr_t) const noexcept {
		return !*this;
	}

	bool operator!=(std::nullptr_t) const noexcept {
		return static_cast<bool>(*this);
	}

	dds_TopicDescription& operator=(std::nullptr_t) noexcept {
		m_owner.reset();
		m_raw = nullptr;
		m_kind = Kind::None;
		return *this;
	}

	dds_Topic as_topic() const noexcept {
		if(m_kind != Kind::Topic)
			return {};
		return dds_Topic(std::static_pointer_cast<Topic>(m_owner));
	}

	dds_ContentFilteredTopic as_contentfilteredtopic() const noexcept {
		if(m_kind != Kind::Filtered)
			return {};
		return dds_ContentFilteredTopic(std::static_pointer_cast<ContentFilteredTopic>(m_owner));
	}

	dds_MultiTopic as_multitopic() const noexcept {
		if(m_kind != Kind::Multi)
			return {};
		return dds_MultiTopic(std::static_pointer_cast<MultiTopic>(m_owner));
	}

	const std::shared_ptr<void>& GetOwner() const noexcept {
		return m_owner;
	}

private:
	enum class Kind {
		None,
		Topic,
		Filtered,
		Multi
	};
	std::shared_ptr<void> m_owner;
	void* m_raw = nullptr;
	Kind m_kind = Kind::None;
};

namespace detail {
inline dds_TopicDescription RegisteredDescription(const ::dds_TopicDescription* raw) {
	if(auto owner = RegisteredOwner<Topic>(reinterpret_cast<const ::dds_Topic*>(raw)))
		return dds_TopicDescription(dds_Topic(owner));
	if(auto owner = RegisteredOwner<ContentFilteredTopic>(reinterpret_cast<const ::dds_ContentFilteredTopic*>(raw)))
		return dds_TopicDescription(dds_ContentFilteredTopic(owner));
	if(auto owner = RegisteredOwner<MultiTopic>(reinterpret_cast<const ::dds_MultiTopic*>(raw)))
		return dds_TopicDescription(dds_MultiTopic(owner));
	return {};
}
} // namespace detail

class Publisher {
	RAII_DDS_IMPL_OPERATORS(Publisher, ::dds_Publisher);

public:
	Publisher(::dds_Publisher* entity, std::shared_ptr<DomainParticipant> parent) noexcept
	    : m_parent(std::move(parent))
	    , m_entity(entity) {
	}

	~Publisher() noexcept {
		auto lifecycleLock = detail::LockLifecycle(m_parent);
		if(m_entity == nullptr) {
			return;
		}
		detail::ClearListener(m_entity);
		if(m_parent && **m_parent != nullptr) {
			::dds_ReturnCode_t ret = ::dds_DomainParticipant_delete_publisher(**m_parent, m_entity);
			detail::TerminateOnError(ret, "dds_DomainParticipant_delete_publisher");
		}
		m_entity = nullptr;
	}

private:
	std::shared_ptr<DomainParticipant> m_parent;
	::dds_Publisher* m_entity{nullptr};
};

class Subscriber {
	RAII_DDS_IMPL_OPERATORS(Subscriber, ::dds_Subscriber);

public:
	Subscriber(::dds_Subscriber* entity, std::shared_ptr<DomainParticipant> parent) noexcept
	    : m_parent(std::move(parent))
	    , m_entity(entity) {
	}

	~Subscriber() noexcept {
		auto lifecycleLock = detail::LockLifecycle(m_parent);
		if(m_entity == nullptr) {
			return;
		}
		detail::ClearListener(m_entity);
		if(m_parent && **m_parent != nullptr) {
			::dds_ReturnCode_t ret = ::dds_DomainParticipant_delete_subscriber(**m_parent, m_entity);
			detail::TerminateOnError(ret, "dds_DomainParticipant_delete_subscriber");
		}
		m_entity = nullptr;
	}

private:
	std::shared_ptr<DomainParticipant> m_parent;
	::dds_Subscriber* m_entity{nullptr};
};

class DataWriter {
	RAII_DDS_IMPL_OPERATORS(DataWriter, ::dds_DataWriter);

public:
	DataWriter(::dds_DataWriter* entity, std::shared_ptr<Publisher> parent, std::shared_ptr<Topic> topic) noexcept
	    : m_parent(std::move(parent))
	    , m_topic(std::move(topic))
	    , m_entity(entity) {
	}

	~DataWriter() noexcept {
		auto lifecycleLock = detail::LockLifecycle(m_parent);
		if(m_entity == nullptr) {
			return;
		}
		detail::ClearListener(m_entity);
		if(m_parent && **m_parent != nullptr) {
			::dds_ReturnCode_t ret = ::dds_Publisher_delete_datawriter(**m_parent, m_entity);
			detail::TerminateOnError(ret, "dds_Publisher_delete_datawriter");
		}
		m_entity = nullptr;
	}

private:
	std::shared_ptr<Publisher> m_parent;
	std::shared_ptr<Topic> m_topic;
	::dds_DataWriter* m_entity{nullptr};
};

class DataReader {
	RAII_DDS_IMPL_OPERATORS(DataReader, ::dds_DataReader);

public:
	DataReader(::dds_DataReader* entity,
	    std::shared_ptr<Subscriber> parent,
	    std::shared_ptr<void> topicDescriptionOwner) noexcept
	    : m_parent(std::move(parent))
	    , m_topicDescriptionOwner(std::move(topicDescriptionOwner))
	    , m_entity(entity) {
	}

	~DataReader() noexcept {
		auto lifecycleLock = detail::LockLifecycle(m_parent);
		if(m_entity == nullptr) {
			return;
		}
		detail::ClearListener(m_entity);
		if(m_parent && **m_parent != nullptr) {
			::dds_ReturnCode_t ret = ::dds_Subscriber_delete_datareader(**m_parent, m_entity);
			detail::TerminateOnError(ret, "dds_Subscriber_delete_datareader");
		}
		m_entity = nullptr;
	}

private:
	std::shared_ptr<Subscriber> m_parent;
	std::shared_ptr<void> m_topicDescriptionOwner;
	::dds_DataReader* m_entity{nullptr};
};

class ReadCondition {
	RAII_DDS_IMPL_OPERATORS(ReadCondition, ::dds_ReadCondition);

public:
	ReadCondition(::dds_ReadCondition* entity, std::shared_ptr<DataReader> parent) noexcept
	    : m_parent(std::move(parent))
	    , m_entity(entity) {
	}

	~ReadCondition() noexcept {
		if(m_entity == nullptr) {
			return;
		}
		if(m_parent && **m_parent != nullptr) {
			::dds_ReturnCode_t ret = ::dds_DataReader_delete_readcondition(**m_parent, m_entity);
			detail::TerminateOnError(ret, "dds_DataReader_delete_readcondition");
		}
		m_entity = nullptr;
	}

private:
	std::shared_ptr<DataReader> m_parent;
	::dds_ReadCondition* m_entity{nullptr};
};

class QueryCondition {
	RAII_DDS_IMPL_OPERATORS(QueryCondition, ::dds_QueryCondition);

public:
	QueryCondition(::dds_QueryCondition* entity, std::shared_ptr<DataReader> parent) noexcept
	    : m_parent(std::move(parent))
	    , m_entity(entity) {
	}

	~QueryCondition() noexcept {
		if(m_entity == nullptr) {
			return;
		}
		if(m_parent && **m_parent != nullptr) {
			::dds_ReturnCode_t ret =
			    ::dds_DataReader_delete_readcondition(**m_parent, reinterpret_cast<::dds_ReadCondition*>(m_entity));
			detail::TerminateOnError(ret, "dds_DataReader_delete_readcondition");
		}
		m_entity = nullptr;
	}

private:
	std::shared_ptr<DataReader> m_parent;
	::dds_QueryCondition* m_entity{nullptr};
};

class WaitSet {
	RAII_DDS_IMPL_OPERATORS(WaitSet, ::dds_WaitSet);

public:
	explicit WaitSet(::dds_WaitSet* entity) noexcept
	    : m_entity(entity) {
	}

	~WaitSet() noexcept {
		if(m_entity == nullptr) {
			return;
		}
		::dds_ReturnCode_t ret = ::dds_WaitSet_delete(m_entity);
		detail::TerminateOnError(ret, "dds_WaitSet_delete");
		m_entity = nullptr;
	}

	struct Attachment {
		::dds_Condition* raw;
		std::shared_ptr<void> owner;
	};

	template <typename TC>
	::dds_ReturnCode_t Attach(const TC& condition) {
		auto raw = detail::ConditionCast(condition.get());
		for(const auto& item : m_attached) {
			if(item.raw == raw)
				return ::dds_WaitSet_attach_condition(m_entity, raw);
		}
		m_attached.push_back({raw, detail::SharedImpl(condition)});
		const auto ret = ::dds_WaitSet_attach_condition(m_entity, raw);
		if(ret != dds_RETCODE_OK)
			m_attached.pop_back();
		return ret;
	}

	template <typename TC>
	::dds_ReturnCode_t Detach(const TC& condition) {
		auto raw = detail::ConditionCast(condition.get());
		const auto ret = ::dds_WaitSet_detach_condition(m_entity, raw);
		if(ret == dds_RETCODE_OK) {
			m_attached.erase(
			    std::remove_if(
			        m_attached.begin(), m_attached.end(), [raw](const Attachment& item) { return item.raw == raw; }),
			    m_attached.end());
		}
		return ret;
	}

	void ClearAttachments() noexcept {
		m_attached.clear();
	}

private:
	std::vector<Attachment> m_attached;
	::dds_WaitSet* m_entity{nullptr};
};

class GuardCondition {
	RAII_DDS_IMPL_OPERATORS(GuardCondition, ::dds_GuardCondition);

public:
	explicit GuardCondition(::dds_GuardCondition* entity) noexcept
	    : m_entity(entity) {
	}

	~GuardCondition() noexcept {
		if(m_entity == nullptr) {
			return;
		}
		::dds_GuardCondition_delete(m_entity);
		m_entity = nullptr;
	}

private:
	::dds_GuardCondition* m_entity{nullptr};
};

#undef RAII_DDS_IMPL_OPERATORS

// -----------------------------------------------------------------------------
// DomainParticipantFactory wrappers
// -----------------------------------------------------------------------------
// Use this accessor before creating DDS resources to register normal-exit cleanup.
// All DDS users must finish before the function-local shutdown guard is destroyed.
inline ::dds_DomainParticipantFactory* dds_DomainParticipantFactory_get_instance() {
	struct ShutdownGuard {
		~ShutdownGuard() noexcept {
			detail::TerminateOnError(
			    ::dds_DynamicDataFactory_delete_instance(), "dds_DynamicDataFactory_delete_instance");
			detail::TerminateOnError(
			    ::dds_DynamicTypeBuilderFactory_delete_instance(), "dds_DynamicTypeBuilderFactory_delete_instance");
			::dds_DomainParticipantFactory_shutdown();
		}
	};

	auto* factory = ::dds_DomainParticipantFactory_get_instance();
	if(factory != nullptr) {
		static ShutdownGuard s_guard;
		(void)s_guard;
	}
	return factory;
}

inline dds_DomainParticipant dds_DomainParticipantFactory_create_participant(::dds_DomainParticipantFactory* self,
    ::dds_DomainId_t domainId,
    const ::dds_DomainParticipantQos* qos,
    const ::dds_DomainParticipantListener* aListener,
    ::dds_StatusMask mask) {
	std::lock_guard<std::recursive_mutex> factoryLock(detail::GetFactoryMutex());
	::dds_DomainParticipant* raw =
	    ::dds_DomainParticipantFactory_create_participant(self, domainId, qos, aListener, mask);
	if(raw == nullptr) {
		return {};
	}
	return dds_DomainParticipant(detail::MakeRegisteredOwned<DomainParticipant>(raw));
}

inline ::dds_ReturnCode_t dds_DomainParticipantFactory_delete_participant(
    ::dds_DomainParticipantFactory* self, dds_DomainParticipant& aParticipant) {
	auto lifecycleLock = detail::LockLifecycle(detail::SharedImpl(aParticipant));
	std::lock_guard<std::recursive_mutex> factoryLock(detail::GetFactoryMutex());
	// Exclude the one participant pin held by lifecycleLock.
	if(detail::SharedImpl(aParticipant).use_count() > 2)
		return dds_RETCODE_PRECONDITION_NOT_MET;
	if(!aParticipant) {
		return detail::BadParameter();
	}
	::dds_ReturnCode_t ret = ::dds_DomainParticipantFactory_delete_participant(self, aParticipant);
	if(ret == dds_RETCODE_OK) {
		*detail::SharedImpl(aParticipant) = nullptr;
		aParticipant = nullptr;
	}
	return ret;
}

inline dds_DomainParticipant dds_DomainParticipantFactory_create_participant_w_id(::dds_DomainParticipantFactory* self,
    ::dds_DomainId_t domainId,
    uint16_t participantId,
    const ::dds_DomainParticipantQos* qos,
    const ::dds_DomainParticipantListener* aListener,
    ::dds_StatusMask mask) {
	std::lock_guard<std::recursive_mutex> factoryLock(detail::GetFactoryMutex());
	::dds_DomainParticipant* raw =
	    ::dds_DomainParticipantFactory_create_participant_w_id(self, domainId, participantId, qos, aListener, mask);
	if(raw == nullptr) {
		return {};
	}
	return dds_DomainParticipant(detail::MakeRegisteredOwned<DomainParticipant>(raw));
}

inline dds_DomainParticipant dds_DomainParticipantFactory_create_participant_w_props(
    ::dds_DomainParticipantFactory* self,
    ::dds_DomainId_t domainId,
    const ::dds_DomainParticipantQos* qos,
    const ::dds_DomainParticipantListener* aListener,
    ::dds_StatusMask mask,
    ::dds_StringProperty* props) {
	std::lock_guard<std::recursive_mutex> factoryLock(detail::GetFactoryMutex());
	::dds_DomainParticipant* raw =
	    ::dds_DomainParticipantFactory_create_participant_w_props(self, domainId, qos, aListener, mask, props);
	if(raw == nullptr) {
		return {};
	}
	return dds_DomainParticipant(detail::MakeRegisteredOwned<DomainParticipant>(raw));
}

inline dds_DomainParticipant dds_DomainParticipantFactory_create_participant_w_profile(
    ::dds_DomainParticipantFactory* self,
    ::dds_DomainId_t domainId,
    const char* profileName,
    const ::dds_DomainParticipantListener* aListener,
    ::dds_StatusMask mask) {
	std::lock_guard<std::recursive_mutex> factoryLock(detail::GetFactoryMutex());
	::dds_DomainParticipant* raw =
	    ::dds_DomainParticipantFactory_create_participant_w_profile(self, domainId, profileName, aListener, mask);
	if(raw == nullptr) {
		return {};
	}
	return dds_DomainParticipant(detail::MakeRegisteredOwned<DomainParticipant>(raw));
}

// -----------------------------------------------------------------------------
// DomainParticipant-owned entities
// -----------------------------------------------------------------------------
inline dds_Publisher dds_DomainParticipant_create_publisher(const dds_DomainParticipant& self,
    const ::dds_PublisherQos* qos,
    const ::dds_PublisherListener* aListener,
    ::dds_StatusMask mask) {
	auto lifecycleLock = detail::LockLifecycle(detail::SharedImpl(self));
	::dds_Publisher* raw =
	    ::dds_DomainParticipant_create_publisher(static_cast<::dds_DomainParticipant*>(self), qos, aListener, mask);
	if(raw == nullptr) {
		return {};
	}
	return dds_Publisher(detail::MakeOwned<Publisher>(raw, detail::SharedImpl(self)));
}

inline ::dds_ReturnCode_t dds_DomainParticipant_delete_publisher(
    const dds_DomainParticipant& self, dds_Publisher& publisher) {
	auto lifecycleLock = detail::LockLifecycle(detail::SharedImpl(self));
	if(!self || !publisher) {
		return detail::BadParameter();
	}
	if(detail::SharedImpl(publisher).use_count() > 1)
		return dds_RETCODE_PRECONDITION_NOT_MET;
	::dds_ReturnCode_t ret =
	    ::dds_DomainParticipant_delete_publisher(static_cast<::dds_DomainParticipant*>(self), publisher);
	if(ret == dds_RETCODE_OK) {
		*detail::SharedImpl(publisher) = nullptr;
		publisher = nullptr;
	}
	return ret;
}

inline dds_Subscriber dds_DomainParticipant_create_subscriber(const dds_DomainParticipant& self,
    const ::dds_SubscriberQos* qos,
    const ::dds_SubscriberListener* aListener,
    ::dds_StatusMask mask) {
	auto lifecycleLock = detail::LockLifecycle(detail::SharedImpl(self));
	::dds_Subscriber* raw =
	    ::dds_DomainParticipant_create_subscriber(static_cast<::dds_DomainParticipant*>(self), qos, aListener, mask);
	if(raw == nullptr) {
		return {};
	}
	return dds_Subscriber(detail::MakeOwned<Subscriber>(raw, detail::SharedImpl(self)));
}

inline ::dds_ReturnCode_t dds_DomainParticipant_delete_subscriber(
    const dds_DomainParticipant& self, dds_Subscriber& subscriber) {
	auto lifecycleLock = detail::LockLifecycle(detail::SharedImpl(self));
	if(!self || !subscriber) {
		return detail::BadParameter();
	}
	if(detail::SharedImpl(subscriber).use_count() > 1)
		return dds_RETCODE_PRECONDITION_NOT_MET;
	::dds_ReturnCode_t ret =
	    ::dds_DomainParticipant_delete_subscriber(static_cast<::dds_DomainParticipant*>(self), subscriber);
	if(ret == dds_RETCODE_OK) {
		*detail::SharedImpl(subscriber) = nullptr;
		subscriber = nullptr;
	}
	return ret;
}

inline dds_Topic dds_DomainParticipant_create_topic(const dds_DomainParticipant& self,
    const char* topicName,
    const char* typeName,
    const ::dds_TopicQos* qos,
    const ::dds_TopicListener* aListener,
    ::dds_StatusMask mask) {
	auto lifecycleLock = detail::LockLifecycle(detail::SharedImpl(self));
	::dds_Topic* raw = ::dds_DomainParticipant_create_topic(
	    static_cast<::dds_DomainParticipant*>(self), topicName, typeName, qos, aListener, mask);
	if(raw == nullptr) {
		return {};
	}
	auto owner{detail::MakeRegisteredOwned<Topic>(raw, detail::SharedImpl(self))};
	lifecycleLock.RegisterTopic(topicName, owner);
	return dds_Topic(std::move(owner));
}

inline ::dds_ReturnCode_t dds_DomainParticipant_delete_topic(const dds_DomainParticipant& self, dds_Topic& topic) {
	auto lifecycleLock = detail::LockLifecycle(detail::SharedImpl(self));
	if(!self || !topic) {
		return detail::BadParameter();
	}
	if(detail::SharedImpl(topic).use_count() > 1)
		return dds_RETCODE_PRECONDITION_NOT_MET;
	::dds_ReturnCode_t ret = ::dds_DomainParticipant_delete_topic(static_cast<::dds_DomainParticipant*>(self), topic);
	if(ret == dds_RETCODE_OK) {
		*detail::SharedImpl(topic) = nullptr;
		topic = nullptr;
	}
	return ret;
}

inline dds_ContentFilteredTopic dds_DomainParticipant_create_contentfilteredtopic(const dds_DomainParticipant& self,
    const char* name,
    const dds_Topic& relatedTopic,
    const char* filterExpression,
    const ::dds_StringSeq* expressionParameters) {
	auto lifecycleLock = detail::LockLifecycle(detail::SharedImpl(self));
	::dds_ContentFilteredTopic* raw =
	    ::dds_DomainParticipant_create_contentfilteredtopic(static_cast<::dds_DomainParticipant*>(self),
	        name,
	        static_cast<::dds_Topic*>(relatedTopic),
	        filterExpression,
	        expressionParameters);
	if(raw == nullptr) {
		return {};
	}
	return dds_ContentFilteredTopic(detail::MakeRegisteredOwned<ContentFilteredTopic>(
	    raw, detail::SharedImpl(self), detail::SharedImpl(relatedTopic)));
}

inline ::dds_ReturnCode_t dds_DomainParticipant_delete_contentfilteredtopic(
    const dds_DomainParticipant& self, dds_ContentFilteredTopic& topic) {
	auto lifecycleLock = detail::LockLifecycle(detail::SharedImpl(self));
	if(!self || !topic) {
		return detail::BadParameter();
	}
	if(detail::SharedImpl(topic).use_count() > 1)
		return dds_RETCODE_PRECONDITION_NOT_MET;
	::dds_ReturnCode_t ret =
	    ::dds_DomainParticipant_delete_contentfilteredtopic(static_cast<::dds_DomainParticipant*>(self), topic);
	if(ret == dds_RETCODE_OK) {
		*detail::SharedImpl(topic) = nullptr;
		topic = nullptr;
	}
	return ret;
}

inline dds_MultiTopic dds_DomainParticipant_create_multitopic(const dds_DomainParticipant& self,
    const char* name,
    const char* typeName,
    const char* subscriptionExpression,
    const ::dds_StringSeq* expressionParameters) {
	auto lifecycleLock = detail::LockLifecycle(detail::SharedImpl(self));
	::dds_MultiTopic* raw = ::dds_DomainParticipant_create_multitopic(
	    static_cast<::dds_DomainParticipant*>(self), name, typeName, subscriptionExpression, expressionParameters);
	if(raw == nullptr) {
		return {};
	}
	return dds_MultiTopic(detail::MakeRegisteredOwned<MultiTopic>(raw, detail::SharedImpl(self)));
}

inline ::dds_ReturnCode_t dds_DomainParticipant_delete_multitopic(
    const dds_DomainParticipant& self, dds_MultiTopic& topic) {
	auto lifecycleLock = detail::LockLifecycle(detail::SharedImpl(self));
	if(!self || !topic) {
		return detail::BadParameter();
	}
	if(detail::SharedImpl(topic).use_count() > 1)
		return dds_RETCODE_PRECONDITION_NOT_MET;
	::dds_ReturnCode_t ret =
	    ::dds_DomainParticipant_delete_multitopic(static_cast<::dds_DomainParticipant*>(self), topic);
	if(ret == dds_RETCODE_OK) {
		*detail::SharedImpl(topic) = nullptr;
		topic = nullptr;
	}
	return ret;
}

// Lookup wrappers only share entities created through this RAII adapter.
// A native entity with no registered owner yields an empty handle; it is never adopted.
inline dds_DomainParticipant dds_DomainParticipantFactory_lookup_participant(
    ::dds_DomainParticipantFactory* self, ::dds_DomainId_t domainId) {
	std::lock_guard<std::recursive_mutex> factoryLock(detail::GetFactoryMutex());
	if(!self)
		return {};
	return dds_DomainParticipant(
	    detail::RegisteredOwner<DomainParticipant>(::dds_DomainParticipantFactory_lookup_participant(self, domainId)));
}

inline dds_Topic dds_DomainParticipant_find_topic(
    const dds_DomainParticipant& self, const char* topicName, const ::dds_Duration_t* timeout) {
	auto lifecycleLock = detail::LockLifecycle(detail::SharedImpl(self));
	if(!self || !topicName || !timeout || timeout->sec < 0)
		return {};
	// Preserve the SDK's zero-timeout behavior without entering a blocking SDK find.
	if(timeout->sec == 0 && timeout->nanosec == 0)
		return {};
	const bool infinite = timeout->sec == dds_DURATION_INFINITE_SEC && timeout->nanosec == dds_DURATION_INFINITE_NSEC;
	const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(timeout->sec) +
	                      std::chrono::nanoseconds(timeout->nanosec);
	const std::string name{topicName};
	for(;;) {
		auto owner{lifecycleLock.FindTopic(name)};
		if(owner)
			return dds_Topic(std::move(owner));
		if(infinite) {
			lifecycleLock.Wait();
		} else if(std::chrono::steady_clock::now() >= deadline) {
			return {};
		} else {
			// Waiting releases the lifecycle mutex, allowing creation and deletion.
			lifecycleLock.WaitUntil(deadline);
		}
	}
}

inline dds_TopicDescription dds_DomainParticipant_lookup_topicdescription(
    const dds_DomainParticipant& self, const char* name) {
	auto lifecycleLock = detail::LockLifecycle(detail::SharedImpl(self));
	if(!self || !name)
		return {};
	return detail::RegisteredDescription(::dds_DomainParticipant_lookup_topicdescription(self, name));
}

inline dds_TopicDescription dds_DomainParticipant_lookup_topicdescription_w_index(
    const dds_DomainParticipant& self, const char* name, ::dds_UnsignedLong index) {
	auto lifecycleLock = detail::LockLifecycle(detail::SharedImpl(self));
	if(!self || !name)
		return {};
	return detail::RegisteredDescription(::dds_DomainParticipant_lookup_topicdescription_w_index(self, name, index));
}

inline dds_DataWriter dds_Publisher_lookup_datawriter(const dds_Publisher& self, const char* topicName) {
	auto lifecycleLock = detail::LockLifecycle(detail::SharedImpl(self));
	if(!self || !topicName)
		return {};
	return dds_DataWriter(detail::RegisteredOwner<DataWriter>(::dds_Publisher_lookup_datawriter(self, topicName)));
}

inline dds_DataReader dds_Subscriber_lookup_datareader(const dds_Subscriber& self, const char* topicName) {
	auto lifecycleLock = detail::LockLifecycle(detail::SharedImpl(self));
	if(!self || !topicName)
		return {};
	return dds_DataReader(detail::RegisteredOwner<DataReader>(::dds_Subscriber_lookup_datareader(self, topicName)));
}

inline dds_DataWriter dds_Publisher_get_contained_datawriter_by_name(const dds_Publisher& self, char* name) {
	auto lifecycleLock = detail::LockLifecycle(detail::SharedImpl(self));
	if(!self || !name)
		return {};
	return dds_DataWriter(
	    detail::RegisteredOwner<DataWriter>(::dds_Publisher_get_contained_datawriter_by_name(self, name)));
}

inline dds_DataReader dds_Subscriber_get_contained_datareader_by_name(const dds_Subscriber& self, char* name) {
	auto lifecycleLock = detail::LockLifecycle(detail::SharedImpl(self));
	if(!self || !name)
		return {};
	return dds_DataReader(
	    detail::RegisteredOwner<DataReader>(::dds_Subscriber_get_contained_datareader_by_name(self, name)));
}

// -----------------------------------------------------------------------------
// Publisher-owned entities
// -----------------------------------------------------------------------------
inline dds_DataWriter dds_Publisher_create_datawriter(const dds_Publisher& self,
    const dds_Topic& aTopic,
    const ::dds_DataWriterQos* qos,
    const ::dds_DataWriterListener* aListener,
    ::dds_StatusMask mask) {
	auto lifecycleLock = detail::LockLifecycle(detail::SharedImpl(self));
	::dds_DataWriter* raw = ::dds_Publisher_create_datawriter(
	    static_cast<::dds_Publisher*>(self), static_cast<::dds_Topic*>(aTopic), qos, aListener, mask);
	if(raw == nullptr) {
		return {};
	}
	return dds_DataWriter(
	    detail::MakeRegisteredOwned<DataWriter>(raw, detail::SharedImpl(self), detail::SharedImpl(aTopic)));
}

inline dds_DataWriter dds_Publisher_create_datawriter_w_props(const dds_Publisher& self,
    const dds_Topic& aTopic,
    const ::dds_DataWriterQos* qos,
    const ::dds_DataWriterListener* aListener,
    ::dds_StatusMask mask,
    ::dds_StringProperty* props) {
	auto lifecycleLock = detail::LockLifecycle(detail::SharedImpl(self));
	::dds_DataWriter* raw = ::dds_Publisher_create_datawriter_w_props(
	    static_cast<::dds_Publisher*>(self), static_cast<::dds_Topic*>(aTopic), qos, aListener, mask, props);
	if(raw == nullptr) {
		return {};
	}
	return dds_DataWriter(
	    detail::MakeRegisteredOwned<DataWriter>(raw, detail::SharedImpl(self), detail::SharedImpl(aTopic)));
}

inline dds_DataWriter dds_Publisher_create_datawriter_w_profile(const dds_Publisher& self,
    const dds_Topic& aTopic,
    const char* profileName,
    const ::dds_DataWriterListener* aListener,
    ::dds_StatusMask mask) {
	auto lifecycleLock = detail::LockLifecycle(detail::SharedImpl(self));
	::dds_DataWriter* raw = ::dds_Publisher_create_datawriter_w_profile(
	    static_cast<::dds_Publisher*>(self), static_cast<::dds_Topic*>(aTopic), profileName, aListener, mask);
	if(raw == nullptr) {
		return {};
	}
	return dds_DataWriter(
	    detail::MakeRegisteredOwned<DataWriter>(raw, detail::SharedImpl(self), detail::SharedImpl(aTopic)));
}

// Endpoints retain managed topics. Callers keep externally owned topics alive.
inline dds_DataWriter dds_Publisher_create_datawriter(const dds_Publisher& self,
    const ::dds_Topic* aTopic,
    const ::dds_DataWriterQos* qos,
    const ::dds_DataWriterListener* aListener,
    ::dds_StatusMask mask) {
	auto lifecycleLock = detail::LockLifecycle(detail::SharedImpl(self));
	auto topicOwner{detail::RegisteredOwner<Topic>(aTopic)};
	::dds_DataWriter* raw =
	    ::dds_Publisher_create_datawriter(static_cast<::dds_Publisher*>(self), aTopic, qos, aListener, mask);
	if(raw == nullptr) {
		return {};
	}
	return dds_DataWriter(detail::MakeRegisteredOwned<DataWriter>(raw, detail::SharedImpl(self), topicOwner));
}

inline dds_DataWriter dds_Publisher_create_datawriter_w_props(const dds_Publisher& self,
    const ::dds_Topic* aTopic,
    const ::dds_DataWriterQos* qos,
    const ::dds_DataWriterListener* aListener,
    ::dds_StatusMask mask,
    ::dds_StringProperty* props) {
	auto lifecycleLock = detail::LockLifecycle(detail::SharedImpl(self));
	auto topicOwner{detail::RegisteredOwner<Topic>(aTopic)};
	::dds_DataWriter* raw = ::dds_Publisher_create_datawriter_w_props(
	    static_cast<::dds_Publisher*>(self), aTopic, qos, aListener, mask, props);
	if(raw == nullptr) {
		return {};
	}
	return dds_DataWriter(detail::MakeRegisteredOwned<DataWriter>(raw, detail::SharedImpl(self), topicOwner));
}

inline dds_DataWriter dds_Publisher_create_datawriter_w_profile(const dds_Publisher& self,
    const ::dds_Topic* aTopic,
    const char* profileName,
    const ::dds_DataWriterListener* aListener,
    ::dds_StatusMask mask) {
	auto lifecycleLock = detail::LockLifecycle(detail::SharedImpl(self));
	auto topicOwner{detail::RegisteredOwner<Topic>(aTopic)};
	::dds_DataWriter* raw = ::dds_Publisher_create_datawriter_w_profile(
	    static_cast<::dds_Publisher*>(self), aTopic, profileName, aListener, mask);
	if(raw == nullptr) {
		return {};
	}
	return dds_DataWriter(detail::MakeRegisteredOwned<DataWriter>(raw, detail::SharedImpl(self), topicOwner));
}

inline ::dds_ReturnCode_t dds_Publisher_delete_datawriter(const dds_Publisher& self, dds_DataWriter& aDatawriter) {
	auto lifecycleLock = detail::LockLifecycle(detail::SharedImpl(self));
	if(!self || !aDatawriter) {
		return detail::BadParameter();
	}
	if(detail::SharedImpl(aDatawriter).use_count() > 1)
		return dds_RETCODE_PRECONDITION_NOT_MET;
	::dds_ReturnCode_t ret = ::dds_Publisher_delete_datawriter(static_cast<::dds_Publisher*>(self), aDatawriter);
	if(ret == dds_RETCODE_OK) {
		*detail::SharedImpl(aDatawriter) = nullptr;
		aDatawriter = nullptr;
	}
	return ret;
}

// -----------------------------------------------------------------------------
// Subscriber-owned entities
// -----------------------------------------------------------------------------
inline dds_DataReader dds_Subscriber_create_datareader(const dds_Subscriber& self,
    const dds_Topic& aTopic,
    const ::dds_DataReaderQos* qos,
    const ::dds_DataReaderListener* aListener,
    ::dds_StatusMask mask) {
	auto lifecycleLock = detail::LockLifecycle(detail::SharedImpl(self));
	::dds_DataReader* raw = ::dds_Subscriber_create_datareader(static_cast<::dds_Subscriber*>(self),
	    detail::TopicDescriptionCast(static_cast<::dds_Topic*>(aTopic)),
	    qos,
	    aListener,
	    mask);
	if(raw == nullptr) {
		return {};
	}
	return dds_DataReader(
	    detail::MakeRegisteredOwned<DataReader>(raw, detail::SharedImpl(self), detail::SharedImpl(aTopic)));
}

inline dds_DataReader dds_Subscriber_create_datareader(const dds_Subscriber& self,
    const dds_ContentFilteredTopic& aTopic,
    const ::dds_DataReaderQos* qos,
    const ::dds_DataReaderListener* aListener,
    ::dds_StatusMask mask) {
	auto lifecycleLock = detail::LockLifecycle(detail::SharedImpl(self));
	::dds_DataReader* raw = ::dds_Subscriber_create_datareader(static_cast<::dds_Subscriber*>(self),
	    detail::TopicDescriptionCast(static_cast<::dds_ContentFilteredTopic*>(aTopic)),
	    qos,
	    aListener,
	    mask);
	if(raw == nullptr) {
		return {};
	}
	return dds_DataReader(
	    detail::MakeRegisteredOwned<DataReader>(raw, detail::SharedImpl(self), detail::SharedImpl(aTopic)));
}

inline dds_DataReader dds_Subscriber_create_datareader(const dds_Subscriber& self,
    const dds_MultiTopic& aTopic,
    const ::dds_DataReaderQos* qos,
    const ::dds_DataReaderListener* aListener,
    ::dds_StatusMask mask) {
	auto lifecycleLock = detail::LockLifecycle(detail::SharedImpl(self));
	::dds_DataReader* raw = ::dds_Subscriber_create_datareader(static_cast<::dds_Subscriber*>(self),
	    detail::TopicDescriptionCast(static_cast<::dds_MultiTopic*>(aTopic)),
	    qos,
	    aListener,
	    mask);
	if(raw == nullptr) {
		return {};
	}
	return dds_DataReader(
	    detail::MakeRegisteredOwned<DataReader>(raw, detail::SharedImpl(self), detail::SharedImpl(aTopic)));
}

inline dds_DataReader dds_Subscriber_create_datareader_w_profile(const dds_Subscriber& self,
    const dds_Topic& aTopic,
    const char* profileName,
    const ::dds_DataReaderListener* aListener,
    ::dds_StatusMask mask) {
	auto lifecycleLock = detail::LockLifecycle(detail::SharedImpl(self));
	::dds_DataReader* raw = ::dds_Subscriber_create_datareader_w_profile(
	    static_cast<::dds_Subscriber*>(self), static_cast<::dds_Topic*>(aTopic), profileName, aListener, mask);
	if(raw == nullptr) {
		return {};
	}
	return dds_DataReader(
	    detail::MakeRegisteredOwned<DataReader>(raw, detail::SharedImpl(self), detail::SharedImpl(aTopic)));
}

inline dds_DataReader dds_Subscriber_create_datareader(const dds_Subscriber& self,
    const dds_TopicDescription& aTopic,
    const ::dds_DataReaderQos* qos,
    const ::dds_DataReaderListener* aListener,
    ::dds_StatusMask mask) {
	auto lifecycleLock = detail::LockLifecycle(detail::SharedImpl(self));
	::dds_DataReader* raw = ::dds_Subscriber_create_datareader(self, aTopic.get(), qos, aListener, mask);
	if(!raw)
		return {};
	return dds_DataReader(detail::MakeRegisteredOwned<DataReader>(raw, detail::SharedImpl(self), aTopic.GetOwner()));
}

// Retain managed topics even when the caller passes a native topic description pointer.
inline dds_DataReader dds_Subscriber_create_datareader(const dds_Subscriber& self,
    const ::dds_TopicDescription* aTopic,
    const ::dds_DataReaderQos* qos,
    const ::dds_DataReaderListener* aListener,
    ::dds_StatusMask mask) {
	auto lifecycleLock = detail::LockLifecycle(detail::SharedImpl(self));
	auto topicOwner{detail::RegisteredDescription(aTopic).GetOwner()};
	::dds_DataReader* raw =
	    ::dds_Subscriber_create_datareader(static_cast<::dds_Subscriber*>(self), aTopic, qos, aListener, mask);
	if(raw == nullptr) {
		return {};
	}
	return dds_DataReader(detail::MakeRegisteredOwned<DataReader>(raw, detail::SharedImpl(self), topicOwner));
}

// Accept the native topic types as well as the common TopicDescription pointer.
// Restrict deduction to native types so existing RAII overloads keep ownership.
template <typename TNativeTopic>
inline typename std::enable_if<std::is_same<TNativeTopic, ::dds_Topic>::value ||
                                   std::is_same<TNativeTopic, ::dds_ContentFilteredTopic>::value ||
                                   std::is_same<TNativeTopic, ::dds_MultiTopic>::value,
    dds_DataReader>::type
dds_Subscriber_create_datareader(const dds_Subscriber& self,
    const TNativeTopic* aTopic,
    const ::dds_DataReaderQos* qos,
    const ::dds_DataReaderListener* aListener,
    ::dds_StatusMask mask) {
	return raii::dds_Subscriber_create_datareader(self, detail::TopicDescriptionCast(aTopic), qos, aListener, mask);
}

inline dds_DataReader dds_Subscriber_create_datareader_w_profile(const dds_Subscriber& self,
    const ::dds_Topic* aTopic,
    const char* profileName,
    const ::dds_DataReaderListener* aListener,
    ::dds_StatusMask mask) {
	auto lifecycleLock = detail::LockLifecycle(detail::SharedImpl(self));
	auto topicOwner{detail::RegisteredOwner<Topic>(aTopic)};
	::dds_DataReader* raw = ::dds_Subscriber_create_datareader_w_profile(
	    static_cast<::dds_Subscriber*>(self), aTopic, profileName, aListener, mask);
	if(raw == nullptr) {
		return {};
	}
	return dds_DataReader(detail::MakeRegisteredOwned<DataReader>(raw, detail::SharedImpl(self), topicOwner));
}

inline ::dds_ReturnCode_t dds_Subscriber_delete_datareader(const dds_Subscriber& self, dds_DataReader& aDatareader) {
	auto lifecycleLock = detail::LockLifecycle(detail::SharedImpl(self));
	if(!self || !aDatareader) {
		return detail::BadParameter();
	}
	if(detail::SharedImpl(aDatareader).use_count() > 1)
		return dds_RETCODE_PRECONDITION_NOT_MET;
	::dds_ReturnCode_t ret = ::dds_Subscriber_delete_datareader(static_cast<::dds_Subscriber*>(self), aDatareader);
	if(ret == dds_RETCODE_OK) {
		*detail::SharedImpl(aDatareader) = nullptr;
		aDatareader = nullptr;
	}
	return ret;
}

// -----------------------------------------------------------------------------
// DataReader-owned conditions
// -----------------------------------------------------------------------------
inline dds_ReadCondition dds_DataReader_create_readcondition(const dds_DataReader& self,
    ::dds_SampleStateMask sampleStates,
    ::dds_ViewStateMask viewStates,
    ::dds_InstanceStateMask instanceStates) {
	::dds_ReadCondition* raw = ::dds_DataReader_create_readcondition(
	    static_cast<::dds_DataReader*>(self), sampleStates, viewStates, instanceStates);
	if(raw == nullptr) {
		return {};
	}
	return dds_ReadCondition(detail::MakeOwned<ReadCondition>(raw, detail::SharedImpl(self)));
}

inline dds_QueryCondition dds_DataReader_create_querycondition(const dds_DataReader& self,
    ::dds_SampleStateMask sampleStates,
    ::dds_ViewStateMask viewStates,
    ::dds_InstanceStateMask instanceStates,
    const char* queryExpression,
    const ::dds_StringSeq* queryParameters) {
	::dds_QueryCondition* raw = ::dds_DataReader_create_querycondition(static_cast<::dds_DataReader*>(self),
	    sampleStates,
	    viewStates,
	    instanceStates,
	    queryExpression,
	    queryParameters);
	if(raw == nullptr) {
		return {};
	}
	return dds_QueryCondition(detail::MakeOwned<QueryCondition>(raw, detail::SharedImpl(self)));
}

inline ::dds_ReturnCode_t dds_DataReader_delete_readcondition(
    const dds_DataReader& self, dds_ReadCondition& aCondition) {
	if(!self || !aCondition) {
		return detail::BadParameter();
	}
	if(detail::SharedImpl(aCondition).use_count() > 1)
		return dds_RETCODE_PRECONDITION_NOT_MET;
	::dds_ReturnCode_t ret = ::dds_DataReader_delete_readcondition(static_cast<::dds_DataReader*>(self), aCondition);
	if(ret == dds_RETCODE_OK) {
		*detail::SharedImpl(aCondition) = nullptr;
		aCondition = nullptr;
	}
	return ret;
}

inline ::dds_ReturnCode_t dds_DataReader_delete_readcondition(
    const dds_DataReader& self, dds_QueryCondition& aCondition) {
	if(!self || !aCondition) {
		return detail::BadParameter();
	}
	if(detail::SharedImpl(aCondition).use_count() > 1)
		return dds_RETCODE_PRECONDITION_NOT_MET;
	::dds_ReturnCode_t ret = ::dds_DataReader_delete_readcondition(static_cast<::dds_DataReader*>(self),
	    reinterpret_cast<::dds_ReadCondition*>(static_cast<::dds_QueryCondition*>(aCondition)));
	if(ret == dds_RETCODE_OK) {
		*detail::SharedImpl(aCondition) = nullptr;
		aCondition = nullptr;
	}
	return ret;
}

// -----------------------------------------------------------------------------
// Standalone WaitSet / GuardCondition
// -----------------------------------------------------------------------------
inline dds_WaitSet dds_WaitSet_create() {
	::dds_WaitSet* raw = ::dds_WaitSet_create();
	if(raw == nullptr) {
		return {};
	}
	return dds_WaitSet(detail::MakeOwned<WaitSet>(raw));
}

inline ::dds_ReturnCode_t dds_WaitSet_delete(dds_WaitSet& self) {
	if(!self) {
		return detail::BadParameter();
	}
	if(detail::SharedImpl(self).use_count() > 1)
		return dds_RETCODE_PRECONDITION_NOT_MET;
	::dds_ReturnCode_t ret = ::dds_WaitSet_delete(self);
	if(ret == dds_RETCODE_OK) {
		*detail::SharedImpl(self) = nullptr;
		detail::SharedImpl(self)->ClearAttachments();
		self = nullptr;
	}
	return ret;
}

inline dds_GuardCondition dds_GuardCondition_create() {
	::dds_GuardCondition* raw = ::dds_GuardCondition_create();
	if(raw == nullptr) {
		return {};
	}
	return dds_GuardCondition(detail::MakeOwned<GuardCondition>(raw));
}

inline void dds_GuardCondition_delete(dds_GuardCondition& self) noexcept {
	if(!self) {
		return;
	}
	if(detail::SharedImpl(self).use_count() > 1) {
		self = nullptr;
		return;
	}
	::dds_GuardCondition_delete(self);
	*detail::SharedImpl(self) = nullptr;
	self = nullptr;
}

inline ::dds_ReturnCode_t dds_WaitSet_attach_condition(const dds_WaitSet& self, const dds_GuardCondition& cond) {
	if(!self || !cond) {
		return detail::BadParameter();
	}
	return detail::SharedImpl(self)->Attach(cond);
}

inline ::dds_ReturnCode_t dds_WaitSet_attach_condition(const dds_WaitSet& self, const dds_ReadCondition& cond) {
	if(!self || !cond) {
		return detail::BadParameter();
	}
	return detail::SharedImpl(self)->Attach(cond);
}

inline ::dds_ReturnCode_t dds_WaitSet_attach_condition(const dds_WaitSet& self, const dds_QueryCondition& cond) {
	if(!self || !cond) {
		return detail::BadParameter();
	}
	return detail::SharedImpl(self)->Attach(cond);
}

inline ::dds_ReturnCode_t dds_WaitSet_detach_condition(const dds_WaitSet& self, const dds_GuardCondition& cond) {
	if(!self || !cond) {
		return detail::BadParameter();
	}
	return detail::SharedImpl(self)->Detach(cond);
}

inline ::dds_ReturnCode_t dds_WaitSet_detach_condition(const dds_WaitSet& self, const dds_ReadCondition& cond) {
	if(!self || !cond) {
		return detail::BadParameter();
	}
	return detail::SharedImpl(self)->Detach(cond);
}

inline ::dds_ReturnCode_t dds_WaitSet_detach_condition(const dds_WaitSet& self, const dds_QueryCondition& cond) {
	if(!self || !cond) {
		return detail::BadParameter();
	}
	return detail::SharedImpl(self)->Detach(cond);
}

namespace detail {
struct DynamicDataAccess;
struct ResourceAccess;
struct SequenceItemAccess;
struct SequenceLoanAccess;
struct TypeSupportDataAccess;
struct WriterLoanAccess;

enum class NativeOwnership {
	Transferable,
	Dependent,
	Borrowed
};

template <typename TDeleter>
auto GetNativeOwnership(const TDeleter& deleter, int) noexcept -> decltype(deleter.GetNativeOwnership()) {
	return deleter.GetNativeOwnership();
}

template <typename TDeleter>
NativeOwnership GetNativeOwnership(const TDeleter&, ...) noexcept {
	return NativeOwnership::Transferable;
}

// Several sequences can share one loan. Returning any of them ends the entire loan.
struct SequenceLoan {
	std::shared_ptr<DataReader> m_reader;
	::dds_DataSeq* m_data{};
	::dds_DynamicDataSeq* m_dynamicData{};
	::dds_SampleInfoSeq* m_infos{};
	::dds_UnsignedLongSeq* m_sizes{};
	bool m_active{false};

	::dds_ReturnCode_t Finish() noexcept {
		if(!m_active)
			return dds_RETCODE_OK;

		::dds_ReturnCode_t ret{};
		if(m_sizes)
			ret = ::dds_DataReader_raw_return_loan(**m_reader, m_data, m_infos, m_sizes);
		else if(m_dynamicData)
			ret = ::dds_DynamicDataReader_return_loan(**m_reader, m_dynamicData, m_infos);
		else
			ret = ::dds_DataReader_return_loan(**m_reader, m_data, m_infos);

		if(ret == dds_RETCODE_OK) {
			m_active = false;
			m_reader.reset();
		}
		return ret;
	}
};
}

// Move-only owners for sequences, TypeSupport, samples, and dynamic types.
// Deleters return the native return code. Destructor failures terminate, matching
// the entity wrappers; use close()/the explicit delete wrapper to inspect errors.
template <typename TValue, typename TDeleter>
class Resource {
	friend struct detail::DynamicDataAccess;
	friend struct detail::TypeSupportDataAccess;
	friend struct detail::ResourceAccess;
	friend struct detail::SequenceLoanAccess;
	friend struct detail::SequenceItemAccess;
	friend struct detail::WriterLoanAccess;

	struct State {
		TValue* m_raw = nullptr;
		TDeleter m_deleter;
		std::shared_ptr<detail::SequenceLoan> m_loan;
		detail::OwnerRegistry<State>* m_registry{};
		const void* m_registryKey{};
		bool m_retainsSequenceItems{false};

		explicit State(TDeleter d)
		    : m_deleter(std::move(d)) {
		}

		~State() noexcept {
			if(m_raw) {
				if(m_loan)
					detail::TerminateOnError(m_loan->Finish(), "return sequence loan");
				detail::TerminateOnError(m_deleter(m_raw), "RAII resource cleanup");
			}
		}

		TValue* operator*() const noexcept {
			return m_raw;
		}
	};

	std::shared_ptr<State> m_state;

	std::unique_lock<std::mutex> LockOwnerRegistry() const {
		if(!m_state->m_registry)
			return {};
		return std::unique_lock<std::mutex>{m_state->m_registry->m_mutex};
	}

	void ClearRaw() noexcept {
		if(m_state->m_registry)
			m_state->m_registry->m_owners.erase(m_state->m_registryKey);
		m_state->m_raw = nullptr;
	}

	template <typename TCleanup>
	::dds_ReturnCode_t CloseWith(TCleanup cleanup) noexcept {
		if(!get())
			return dds_RETCODE_OK;

		{
			// Lock the registry so lookup cannot acquire a pointer during explicit cleanup.
			auto lock{LockOwnerRegistry()};
			if(m_state.use_count() != 1)
				return dds_RETCODE_PRECONDITION_NOT_MET;
			::dds_ReturnCode_t loanRet{dds_RETCODE_OK};
			if(m_state->m_loan)
				loanRet = m_state->m_loan->Finish();
			if(loanRet != dds_RETCODE_OK)
				return loanRet;
			const auto ret{cleanup(m_state->m_raw)};
			if(ret != dds_RETCODE_OK)
				return ret;
			ClearRaw();
		}
		m_state.reset();
		return dds_RETCODE_OK;
	}

public:
	using native_type = TValue;
	using deleter_type = TDeleter;
	Resource() noexcept = default;
	Resource(const Resource&) = delete;
	Resource& operator=(const Resource&) = delete;
	Resource(Resource&&) noexcept = default;
	Resource& operator=(Resource&&) noexcept = default;

	// Adopts an owning C pointer, never a borrowed get/lookup result.
	explicit Resource(TValue* raw, TDeleter deleter = TDeleter()) {
		if(!raw)
			return;
		try {
			m_state = std::make_shared<State>(deleter);
		} catch(...) {
			detail::TerminateOnError(deleter(raw), "adoption cleanup");
			throw;
		}
		m_state->m_raw = raw;
	}

	Resource& operator=(TValue* raw) {
		if(get() != raw) {
			Resource next(raw);
			*this = std::move(next);
		}
		return *this;
	}

	Resource& operator=(std::nullptr_t) noexcept {
		m_state.reset();
		return *this;
	}

	TValue* get() const noexcept {
		if(!m_state)
			return nullptr;
		return m_state->m_raw;
	}

	operator TValue*() const noexcept {
		return get();
	}

	TValue* operator->() const noexcept {
		return get();
	}

	TValue* operator*() const noexcept {
		return get();
	}

	explicit operator bool() const noexcept {
		return get() != nullptr;
	}

	bool operator==(std::nullptr_t) const noexcept {
		return !get();
	}

	bool operator!=(std::nullptr_t) const noexcept {
		return get() != nullptr;
	}

	std::shared_ptr<void> GetOwner() const noexcept {
		return m_state;
	}

	const TDeleter* GetDeleter() const noexcept {
		if(!m_state)
			return nullptr;
		return &m_state->m_deleter;
	}

	void reset() noexcept {
		m_state.reset();
	}

	TValue* release() {
		if(!m_state)
			return nullptr;
		const auto ownership{detail::GetNativeOwnership(m_state->m_deleter, 0)};
		if(ownership == detail::NativeOwnership::Borrowed)
			throw std::logic_error("Cannot release a borrowed DDS resource");
		if(ownership == detail::NativeOwnership::Dependent)
			throw std::logic_error("Use the matching raii::dds_* function for a resource with dependencies");
		if(m_state->m_loan && m_state->m_loan->m_active)
			throw std::logic_error("Return the sequence loan before release()");
		if(m_state->m_retainsSequenceItems)
			throw std::logic_error("DDS sequence retains its items");
		TValue* raw{};
		{
			auto lock{LockOwnerRegistry()};
			if(m_state.use_count() != 1)
				throw std::logic_error("DDS resource is still in use");
			raw = m_state->m_raw;
			ClearRaw();
		}
		m_state.reset();
		return raw;
	}

	::dds_ReturnCode_t close() noexcept {
		return CloseWith([this](TValue* raw) { return m_state->m_deleter(raw); });
	}

	template <typename TAllocate>
	static Resource Create(TAllocate allocate, TDeleter deleter = TDeleter()) {
		Resource result;
		result.m_state = std::make_shared<State>(std::move(deleter));
		result.m_state->m_raw = allocate(); // No throwing C++ work after allocation.
		if(!result.m_state->m_raw)
			result.m_state.reset();
		return result;
	}
};

namespace detail {
struct ResourceAccess {
	template <typename TResource, typename TAllocate, typename TDeleter = typename TResource::deleter_type>
	static TResource Create(TAllocate allocate, TDeleter cleanup = TDeleter()) {
		auto value{TResource::Create(allocate, std::move(cleanup))};
		Register(value, value.get());
		return value;
	}

	template <typename TResource>
	static void Register(TResource& value, const void* key) {
		if(!value || !key)
			return;

		auto& registry{GetOwnerRegistry<typename TResource::State>()};
		value.m_state->m_registryKey = key;
		RegisterOwner(key, value.m_state);
		value.m_state->m_registry = &registry;
	}

	template <typename TResource>
	static TResource Lookup(const void* key) {
		TResource value;
		if(!key)
			return value;

		auto& registry{GetOwnerRegistry<typename TResource::State>()};
		std::lock_guard<std::mutex> lock(registry.m_mutex);
		auto it{registry.m_owners.find(key)};
		if(it == registry.m_owners.end())
			return value;

		value.m_state = it->second.lock();
		if(!value)
			registry.m_owners.erase(it);
		return value;
	}
};

struct SequenceLoanAccess {
	template <typename TSequence>
	static std::shared_ptr<SequenceLoan> Active(const TSequence& sequence) noexcept {
		if(!sequence.m_state || !sequence.m_state->m_loan || !sequence.m_state->m_loan->m_active)
			return {};
		return sequence.m_state->m_loan;
	}

	template <typename TSequence>
	static std::shared_ptr<SequenceLoan> Active(typename TSequence::native_type* raw) {
		auto sequence{ResourceAccess::Lookup<TSequence>(raw)};
		return Active(sequence);
	}

	template <typename TSequence>
	static ::dds_ReturnCode_t Prepare(TSequence& sequence) {
		if(sequence.m_state->m_retainsSequenceItems)
			return dds_RETCODE_PRECONDITION_NOT_MET;
		if(!sequence.m_state->m_registry)
			ResourceAccess::Register(sequence, sequence.get());
		return dds_RETCODE_OK;
	}

	template <typename TSequence>
	static ::dds_ReturnCode_t Finish(TSequence& sequence) noexcept {
		auto loan{Active(sequence)};
		if(!loan)
			return dds_RETCODE_OK;
		return loan->Finish();
	}

	template <typename TSequence>
	static void Commit(TSequence& sequence, const std::shared_ptr<SequenceLoan>& loan) noexcept {
		sequence.m_state->m_loan = loan;
	}
};
} // namespace detail

// Expand at global scope after including the generated TypeSupport header.
// Only sample allocation, deserialization and release are wrapped. The generated
// TypeSupport singleton remains borrowed and the generated C files stay intact.
#define RAII_TYPESUPPORT_DEFINE(TypeSupport, Type)                                                                     \
	namespace raii {                                                                                                   \
	struct TypeSupport##_sample_deleter {                                                                              \
		::dds_ReturnCode_t operator()(::Type* raw) const noexcept {                                                    \
			::TypeSupport##_free(raw);                                                                                 \
			return dds_RETCODE_OK;                                                                                     \
		}                                                                                                              \
	};                                                                                                                 \
	using Type = ::raii::Resource<::Type, TypeSupport##_sample_deleter>;                                               \
	inline Type TypeSupport##_alloc() {                                                                                \
		return Type::Create([]() { return ::TypeSupport##_alloc(); });                                                 \
	}                                                                                                                  \
	inline void TypeSupport##_free(Type& sample) noexcept {                                                            \
		sample.reset();                                                                                                \
	}                                                                                                                  \
	inline Type TypeSupport##_deserialize(void* input, size_t input_size) {                                            \
		return Type::Create([&]() { return ::TypeSupport##_deserialize(input, input_size); });                         \
	}                                                                                                                  \
	}

#define RAII_SEQUENCE_DEFINE(seq_type, check_loan)                                                                     \
	struct seq_type##_deleter {                                                                                        \
		::dds_ReturnCode_t operator()(::seq_type* raw) const noexcept {                                                \
			::seq_type##_delete(raw);                                                                                  \
			return dds_RETCODE_OK;                                                                                     \
		}                                                                                                              \
	};                                                                                                                 \
	using seq_type = Resource<::seq_type, seq_type##_deleter>;                                                         \
	inline seq_type seq_type##_create(uint32_t capacity) {                                                             \
		return seq_type::Create([=]() { return ::seq_type##_create(capacity); });                                      \
	}                                                                                                                  \
	inline seq_type seq_type##_clone(::seq_type* sequence) {                                                           \
		if(check_loan && detail::SequenceLoanAccess::Active<seq_type>(sequence))                                       \
			return {};                                                                                                 \
		return seq_type::Create([=]() { return ::seq_type##_clone(sequence); });                                       \
	}                                                                                                                  \
	inline void seq_type##_delete(seq_type& sequence) noexcept {                                                       \
		sequence.reset();                                                                                              \
	}

RAII_SEQUENCE_DEFINE(dds_InstanceHandleSeq, false)
RAII_SEQUENCE_DEFINE(dds_QosPolicyCountSeq, false)
RAII_SEQUENCE_DEFINE(dds_DataReaderSeq, false)
RAII_SEQUENCE_DEFINE(dds_DataWriterSeq, false)
RAII_SEQUENCE_DEFINE(dds_ConditionSeq, false)
RAII_SEQUENCE_DEFINE(dds_SampleInfoSeq, true)
RAII_SEQUENCE_DEFINE(dds_DataSeq, true)
RAII_SEQUENCE_DEFINE(dds_DataRepresentationIdSeq, false)
RAII_SEQUENCE_DEFINE(dds_PropertySeq, false)
RAII_SEQUENCE_DEFINE(dds_TagSeq, false)
RAII_SEQUENCE_DEFINE(dds_BinaryPropertySeq, false)
RAII_SEQUENCE_DEFINE(dds_CharSeq, false)
RAII_SEQUENCE_DEFINE(dds_StringSeq, false)
RAII_SEQUENCE_DEFINE(dds_WcharSeq, false)
RAII_SEQUENCE_DEFINE(dds_WstringSeq, false)
RAII_SEQUENCE_DEFINE(dds_OctetSeq, false)
RAII_SEQUENCE_DEFINE(dds_ShortSeq, false)
RAII_SEQUENCE_DEFINE(dds_UnsignedShortSeq, false)
RAII_SEQUENCE_DEFINE(dds_LongSeq, false)
RAII_SEQUENCE_DEFINE(dds_UnsignedLongSeq, false)
RAII_SEQUENCE_DEFINE(dds_LongLongSeq, false)
RAII_SEQUENCE_DEFINE(dds_UnsignedLongLongSeq, false)
RAII_SEQUENCE_DEFINE(dds_FloatSeq, false)
RAII_SEQUENCE_DEFINE(dds_DoubleSeq, false)
RAII_SEQUENCE_DEFINE(dds_BooleanSeq, false)
RAII_SEQUENCE_DEFINE(dds_KeyedStringSeq, false)
RAII_SEQUENCE_DEFINE(dds_BytesSeq, false)
RAII_SEQUENCE_DEFINE(dds_KeyedBytesSeq, false)
#undef RAII_SEQUENCE_DEFINE

struct dds_DynamicDataSeq_deleter {
	std::shared_ptr<void> m_itemsOwner;

	::dds_ReturnCode_t operator()(::dds_DynamicDataSeq* sequence) const noexcept {
		::dds_DynamicDataSeq_delete(sequence);
		return dds_RETCODE_OK;
	}
};

using dds_DynamicDataSeq = Resource<::dds_DynamicDataSeq, dds_DynamicDataSeq_deleter>;

inline dds_DynamicDataSeq dds_DynamicDataSeq_create(uint32_t capacity) {
	return dds_DynamicDataSeq::Create([capacity]() { return ::dds_DynamicDataSeq_create(capacity); });
}

inline void dds_DynamicDataSeq_delete(dds_DynamicDataSeq& sequence) noexcept {
	sequence.reset();
}

namespace detail {
template <typename TValue>
::dds_ReturnCode_t CheckScopedOutput(TValue*, ::dds_ReturnCode_t ret) noexcept {
	return ret;
}

inline ::dds_ReturnCode_t CheckScopedOutput(::dds_DomainParticipantQos* qos, ::dds_ReturnCode_t ret) noexcept {
	// If the native copy fails on the first field, later fields can still point into the source.
	if(!qos->property.value)
		qos->property.binary_value = nullptr;
	if(ret == dds_RETCODE_OK && !qos->property.binary_value)
		return dds_RETCODE_ERROR;
	return ret;
}

template <typename TQos>
::dds_ReturnCode_t CheckEndpointQos(TQos* qos, ::dds_ReturnCode_t ret) noexcept {
	if(!qos->data_representation.value)
		qos->property.value = nullptr;
	if(!qos->property.value)
		qos->property.binary_value = nullptr;
	if(!qos->property.binary_value)
		qos->data_tags.tags = nullptr;
	if(ret == dds_RETCODE_OK && !qos->data_tags.tags)
		return dds_RETCODE_ERROR;
	return ret;
}

inline ::dds_ReturnCode_t CheckScopedOutput(::dds_DataWriterQos* qos, ::dds_ReturnCode_t ret) noexcept {
	return CheckEndpointQos(qos, ret);
}

inline ::dds_ReturnCode_t CheckScopedOutput(::dds_DataReaderQos* qos, ::dds_ReturnCode_t ret) noexcept {
	return CheckEndpointQos(qos, ret);
}

inline ::dds_ReturnCode_t CheckScopedOutput(::dds_TypeDescriptor* descriptor, ::dds_ReturnCode_t ret) noexcept {
	// On failure, the native TypeDescriptor copy frees internal fields and leaves their pointers set.
	if(ret != dds_RETCODE_OK)
		*descriptor = {};
	return ret;
}

template <typename TValue>
const typename TValue::native_type* GetScopedInput(const TValue* value) noexcept {
	if(!value)
		return nullptr;
	return *value;
}

template <typename TValue>
const typename TValue::native_type* GetScopedInput(const typename TValue::native_type* value) noexcept {
	return value;
}

template <typename TValue>
const typename TValue::native_type* GetScopedInput(std::nullptr_t) noexcept {
	return nullptr;
}

template <typename TValue>
const typename TValue::native_type* GetScopedInput(const TValue& value) noexcept {
	return value;
}

template <typename TValue, typename TPopulate>
::dds_ReturnCode_t ReplaceScopedValue(TValue& destination, TPopulate populate) noexcept {
	TValue next;
	auto* raw{static_cast<typename TValue::native_type*>(next)};
	const auto nativeRet{populate(raw)};
	const auto ret{CheckScopedOutput(raw, nativeRet)};
	if(ret == dds_RETCODE_OK)
		destination = std::move(next);
	return ret;
}
} // namespace detail

#define RAII_QOS_DEFINE(qos_type)                                                                                      \
	class qos_type {                                                                                                   \
	public:                                                                                                            \
		using native_type = ::qos_type;                                                                                \
                                                                                                                       \
		qos_type() noexcept = default;                                                                                 \
		qos_type(const qos_type&) = delete;                                                                            \
		qos_type& operator=(const qos_type&) = delete;                                                                 \
		/* Transfer ownership; leave the source safe to finalize or reuse. */                                          \
		qos_type(qos_type&& other) noexcept                                                                            \
		    : m_qosEntity(other.m_qosEntity) {                                                                         \
			other.m_qosEntity = native_type{};                                                                         \
		}                                                                                                              \
		qos_type& operator=(qos_type&& other) noexcept {                                                               \
			if(this != &other) {                                                                                       \
				detail::TerminateOnError(::qos_type##_finalize(&m_qosEntity), "move assignment cleanup");              \
				m_qosEntity = other.m_qosEntity;                                                                       \
				other.m_qosEntity = native_type{};                                                                     \
			}                                                                                                          \
			return *this;                                                                                              \
		}                                                                                                              \
                                                                                                                       \
		operator native_type*() noexcept {                                                                             \
			return &m_qosEntity;                                                                                       \
		}                                                                                                              \
                                                                                                                       \
		operator const native_type*() const noexcept {                                                                 \
			return &m_qosEntity;                                                                                       \
		}                                                                                                              \
                                                                                                                       \
		native_type* operator->() noexcept {                                                                           \
			return &m_qosEntity;                                                                                       \
		}                                                                                                              \
                                                                                                                       \
		const native_type* operator->() const noexcept {                                                               \
			return &m_qosEntity;                                                                                       \
		}                                                                                                              \
                                                                                                                       \
		~qos_type() noexcept {                                                                                         \
			(void)::qos_type##_finalize(&m_qosEntity);                                                                 \
		}                                                                                                              \
                                                                                                                       \
	private:                                                                                                           \
		native_type m_qosEntity{};                                                                                     \
	};                                                                                                                 \
                                                                                                                       \
	inline ::dds_ReturnCode_t qos_type##_finalize(qos_type& qos) noexcept {                                            \
		auto ret = ::qos_type##_finalize(qos);                                                                         \
		if(ret == dds_RETCODE_OK)                                                                                      \
			*static_cast<::qos_type*>(qos) = {};                                                                       \
		return ret;                                                                                                    \
	}                                                                                                                  \
                                                                                                                       \
	inline ::dds_ReturnCode_t qos_type##_copy(qos_type& dst, const ::qos_type* src) noexcept {                         \
		if(!src)                                                                                                       \
			return dds_RETCODE_BAD_PARAMETER;                                                                          \
		if(static_cast<const ::qos_type*>(dst) == src)                                                                 \
			return dds_RETCODE_OK;                                                                                     \
		return detail::ReplaceScopedValue(dst, [src](::qos_type* next) { return ::qos_type##_copy(next, src); });      \
	}                                                                                                                  \
	inline ::dds_ReturnCode_t qos_type##_copy(qos_type& dst, const qos_type& src) noexcept {                           \
		return raii::qos_type##_copy(dst, static_cast<const ::qos_type*>(src));                                        \
	}                                                                                                                  \
	inline ::dds_ReturnCode_t qos_type##_finalize(qos_type* value) noexcept {                                          \
		if(!value)                                                                                                     \
			return dds_RETCODE_BAD_PARAMETER;                                                                          \
		return raii::qos_type##_finalize(*value);                                                                      \
	}                                                                                                                  \
	template <typename TSource>                                                                                        \
	inline ::dds_ReturnCode_t qos_type##_copy(qos_type* dst, const TSource& src) noexcept {                            \
		if(!dst)                                                                                                       \
			return dds_RETCODE_BAD_PARAMETER;                                                                          \
		return raii::qos_type##_copy(*dst, detail::GetScopedInput<qos_type>(src));                                     \
	}

// Expand the RAII QoS macros.
RAII_QOS_DEFINE(dds_DomainParticipantFactoryQos)
RAII_QOS_DEFINE(dds_DomainParticipantQos)
RAII_QOS_DEFINE(dds_TopicQos)
RAII_QOS_DEFINE(dds_PublisherQos)
RAII_QOS_DEFINE(dds_SubscriberQos)
RAII_QOS_DEFINE(dds_DataWriterQos)
RAII_QOS_DEFINE(dds_DataReaderQos)

#undef RAII_QOS_DEFINE

#define RAII_QOS_OUT_API_DEFINE(c_api_name, entity_type, qos_type)                                                     \
	inline ::dds_ReturnCode_t c_api_name(::entity_type* self, qos_type& qos) noexcept {                                \
		return detail::ReplaceScopedValue(qos, [self](::qos_type* next) { return ::c_api_name(self, next); });         \
	}                                                                                                                  \
	inline ::dds_ReturnCode_t c_api_name(::entity_type* self, qos_type* qos) noexcept {                                \
		if(!qos)                                                                                                       \
			return dds_RETCODE_BAD_PARAMETER;                                                                          \
		return raii::c_api_name(self, *qos);                                                                           \
	}

RAII_QOS_OUT_API_DEFINE(dds_DomainParticipant_get_qos, dds_DomainParticipant, dds_DomainParticipantQos)

RAII_QOS_OUT_API_DEFINE(dds_DomainParticipant_get_default_publisher_qos, dds_DomainParticipant, dds_PublisherQos)

RAII_QOS_OUT_API_DEFINE(dds_DomainParticipant_get_default_subscriber_qos, dds_DomainParticipant, dds_SubscriberQos)

RAII_QOS_OUT_API_DEFINE(dds_DomainParticipant_get_default_topic_qos, dds_DomainParticipant, dds_TopicQos)

RAII_QOS_OUT_API_DEFINE(
    dds_DomainParticipantFactory_get_default_participant_qos, dds_DomainParticipantFactory, dds_DomainParticipantQos)

RAII_QOS_OUT_API_DEFINE(
    dds_DomainParticipantFactory_get_qos, dds_DomainParticipantFactory, dds_DomainParticipantFactoryQos)

RAII_QOS_OUT_API_DEFINE(dds_Topic_get_qos, dds_Topic, dds_TopicQos)

RAII_QOS_OUT_API_DEFINE(dds_Publisher_get_qos, dds_Publisher, dds_PublisherQos)

RAII_QOS_OUT_API_DEFINE(dds_Publisher_get_default_datawriter_qos, dds_Publisher, dds_DataWriterQos)

RAII_QOS_OUT_API_DEFINE(dds_DataWriter_get_qos, dds_DataWriter, dds_DataWriterQos)

RAII_QOS_OUT_API_DEFINE(dds_Subscriber_get_qos, dds_Subscriber, dds_SubscriberQos)

RAII_QOS_OUT_API_DEFINE(dds_Subscriber_get_default_datareader_qos, dds_Subscriber, dds_DataReaderQos)

RAII_QOS_OUT_API_DEFINE(dds_DataReader_get_qos, dds_DataReader, dds_DataReaderQos)

#undef RAII_QOS_OUT_API_DEFINE

namespace detail {
template <typename TEntity, typename TQos, typename TCopy>
::dds_ReturnCode_t CopyFromTopicQos(TEntity* self, TQos& qos, const ::dds_TopicQos* topic, TCopy copy) noexcept {
	if(!topic)
		return dds_RETCODE_BAD_PARAMETER;

	// Preserve borrowed endpoint-specific policies and isolate the data representation the native copy replaces.
	using TRaw = typename TQos::native_type;
	auto* destination{static_cast<TRaw*>(qos)};
	TRaw next{*destination};
	next.data_representation.value = nullptr;
	const auto ret{copy(self, &next, topic)};
	if(ret != dds_RETCODE_OK) {
		::dds_DataRepresentationIdSeq_delete(next.data_representation.value);
		return ret;
	}

	::dds_DataRepresentationIdSeq_delete(destination->data_representation.value);
	*destination = next;
	return ret;
}
} // namespace detail

inline ::dds_ReturnCode_t dds_Publisher_copy_from_topic_qos(
    ::dds_Publisher* self, dds_DataWriterQos& datawriterQos, const ::dds_TopicQos* topicQos) noexcept {
	return detail::CopyFromTopicQos(self, datawriterQos, topicQos, ::dds_Publisher_copy_from_topic_qos);
}

inline ::dds_ReturnCode_t dds_Publisher_copy_from_topic_qos(
    ::dds_Publisher* self, dds_DataWriterQos& datawriterQos, const dds_TopicQos& topicQos) noexcept {
	return raii::dds_Publisher_copy_from_topic_qos(self, datawriterQos, static_cast<const ::dds_TopicQos*>(topicQos));
}

template <typename TTopicQos>
inline ::dds_ReturnCode_t dds_Publisher_copy_from_topic_qos(
    ::dds_Publisher* self, dds_DataWriterQos* qos, const TTopicQos& topicQos) noexcept {
	if(!qos)
		return dds_RETCODE_BAD_PARAMETER;
	return raii::dds_Publisher_copy_from_topic_qos(self, *qos, detail::GetScopedInput<dds_TopicQos>(topicQos));
}

inline ::dds_ReturnCode_t dds_Subscriber_copy_from_topic_qos(
    ::dds_Subscriber* self, dds_DataReaderQos& datareaderQos, const ::dds_TopicQos* topicQos) noexcept {
	return detail::CopyFromTopicQos(self, datareaderQos, topicQos, ::dds_Subscriber_copy_from_topic_qos);
}

inline ::dds_ReturnCode_t dds_Subscriber_copy_from_topic_qos(
    ::dds_Subscriber* self, dds_DataReaderQos& datareaderQos, const dds_TopicQos& topicQos) noexcept {
	return raii::dds_Subscriber_copy_from_topic_qos(self, datareaderQos, static_cast<const ::dds_TopicQos*>(topicQos));
}

template <typename TTopicQos>
inline ::dds_ReturnCode_t dds_Subscriber_copy_from_topic_qos(
    ::dds_Subscriber* self, dds_DataReaderQos* qos, const TTopicQos& topicQos) noexcept {
	if(!qos)
		return dds_RETCODE_BAD_PARAMETER;
	return raii::dds_Subscriber_copy_from_topic_qos(self, *qos, detail::GetScopedInput<dds_TopicQos>(topicQos));
}

inline ::dds_ReturnCode_t dds_DomainParticipantFactory_get_participant_qos_from_profile(
    const char* profileName, dds_DomainParticipantQos& qos) noexcept {
	if(!profileName)
		return dds_RETCODE_BAD_PARAMETER;
	return detail::ReplaceScopedValue(qos, [profileName](::dds_DomainParticipantQos* next) {
		return ::dds_DomainParticipantFactory_get_participant_qos_from_profile(profileName, next);
	});
}

inline ::dds_ReturnCode_t dds_DomainParticipantFactory_get_participant_qos_from_profile(
    const char* profileName, dds_DomainParticipantQos* qos) noexcept {
	if(!qos)
		return dds_RETCODE_BAD_PARAMETER;
	return raii::dds_DomainParticipantFactory_get_participant_qos_from_profile(profileName, *qos);
}

inline ::dds_ReturnCode_t dds_DomainParticipantFactory_get_datawriter_qos_from_profile(
    const char* profileName, dds_DataWriterQos& qos) noexcept {
	if(!profileName)
		return dds_RETCODE_BAD_PARAMETER;
	return detail::ReplaceScopedValue(qos, [profileName](::dds_DataWriterQos* next) {
		return ::dds_DomainParticipantFactory_get_datawriter_qos_from_profile(profileName, next);
	});
}

inline ::dds_ReturnCode_t dds_DomainParticipantFactory_get_datawriter_qos_from_profile(
    const char* profileName, dds_DataWriterQos* qos) noexcept {
	if(!qos)
		return dds_RETCODE_BAD_PARAMETER;
	return raii::dds_DomainParticipantFactory_get_datawriter_qos_from_profile(profileName, *qos);
}

inline ::dds_ReturnCode_t dds_DomainParticipantFactory_get_datareader_qos_from_profile(
    const char* profileName, dds_DataReaderQos& qos) noexcept {
	if(!profileName)
		return dds_RETCODE_BAD_PARAMETER;
	return detail::ReplaceScopedValue(qos, [profileName](::dds_DataReaderQos* next) {
		return ::dds_DomainParticipantFactory_get_datareader_qos_from_profile(profileName, next);
	});
}

inline ::dds_ReturnCode_t dds_DomainParticipantFactory_get_datareader_qos_from_profile(
    const char* profileName, dds_DataReaderQos* qos) noexcept {
	if(!qos)
		return dds_RETCODE_BAD_PARAMETER;
	return raii::dds_DomainParticipantFactory_get_datareader_qos_from_profile(profileName, *qos);
}

#define RAII_DESCRIPTOR_DEFINE(type)                                                                                   \
	class type {                                                                                                       \
		::type m_value{};                                                                                              \
                                                                                                                       \
	public:                                                                                                            \
		using native_type = ::type;                                                                                    \
		type() noexcept = default;                                                                                     \
		type(const type&) = delete;                                                                                    \
		type& operator=(const type&) = delete;                                                                         \
		/* Transfer ownership; leave the source safe to finalize or reuse. */                                          \
		type(type&& other) noexcept                                                                                    \
		    : m_value(other.m_value) {                                                                                 \
			other.m_value = native_type{};                                                                             \
		}                                                                                                              \
		type& operator=(type&& other) noexcept {                                                                       \
			if(this != &other) {                                                                                       \
				detail::TerminateOnError(::type##_finalize(&m_value), "move assignment cleanup");                      \
				m_value = other.m_value;                                                                               \
				other.m_value = native_type{};                                                                         \
			}                                                                                                          \
			return *this;                                                                                              \
		}                                                                                                              \
		operator ::type*() noexcept {                                                                                  \
			return &m_value;                                                                                           \
		}                                                                                                              \
		operator const ::type*() const noexcept {                                                                      \
			return &m_value;                                                                                           \
		}                                                                                                              \
		::type* operator->() noexcept {                                                                                \
			return &m_value;                                                                                           \
		}                                                                                                              \
		const ::type* operator->() const noexcept {                                                                    \
			return &m_value;                                                                                           \
		}                                                                                                              \
		~type() noexcept {                                                                                             \
			(void)::type##_finalize(&m_value);                                                                         \
		}                                                                                                              \
	};                                                                                                                 \
	inline ::dds_ReturnCode_t type##_finalize(type& value) noexcept {                                                  \
		return ::type##_finalize(value);                                                                               \
	}                                                                                                                  \
	inline ::dds_ReturnCode_t type##_copy_from(type& dst, const ::type* src) noexcept {                                \
		if(!src)                                                                                                       \
			return dds_RETCODE_BAD_PARAMETER;                                                                          \
		if(static_cast<const ::type*>(dst) == src)                                                                     \
			return dds_RETCODE_OK;                                                                                     \
		return detail::ReplaceScopedValue(dst, [src](::type* next) { return ::type##_copy_from(next, src); });         \
	}                                                                                                                  \
	inline ::dds_ReturnCode_t type##_copy_from(type& dst, const type& src) noexcept {                                  \
		return raii::type##_copy_from(dst, static_cast<const ::type*>(src));                                           \
	}                                                                                                                  \
	inline ::dds_ReturnCode_t type##_finalize(type* value) noexcept {                                                  \
		if(!value)                                                                                                     \
			return dds_RETCODE_BAD_PARAMETER;                                                                          \
		return raii::type##_finalize(*value);                                                                          \
	}                                                                                                                  \
	template <typename TSource>                                                                                        \
	inline ::dds_ReturnCode_t type##_copy_from(type* dst, const TSource& src) noexcept {                               \
		if(!dst)                                                                                                       \
			return dds_RETCODE_BAD_PARAMETER;                                                                          \
		return raii::type##_copy_from(*dst, detail::GetScopedInput<type>(src));                                        \
	}
RAII_DESCRIPTOR_DEFINE(dds_TypeDescriptor)
RAII_DESCRIPTOR_DEFINE(dds_MemberDescriptor)
#undef RAII_DESCRIPTOR_DEFINE

#define RAII_VOID_RESOURCE(type, free_api)                                                                             \
	struct type##_deleter {                                                                                            \
		::dds_ReturnCode_t operator()(::type* raw) const noexcept {                                                    \
			::free_api(raw);                                                                                           \
			return dds_RETCODE_OK;                                                                                     \
		}                                                                                                              \
	};                                                                                                                 \
	using type = Resource<::type, type##_deleter>;                                                                     \
	inline void free_api(type& value) noexcept {                                                                       \
		value.reset();                                                                                                 \
	}
RAII_VOID_RESOURCE(dds_TypeSupport, dds_TypeSupport_delete)
RAII_VOID_RESOURCE(dds_DynamicTypeMember, dds_DynamicTypeMember_free)
RAII_VOID_RESOURCE(dds_KeyedString, dds_KeyedStringTypeSupport_free)
RAII_VOID_RESOURCE(dds_Bytes, dds_BytesTypeSupport_free)
RAII_VOID_RESOURCE(dds_KeyedBytes, dds_KeyedBytesTypeSupport_free)
#undef RAII_VOID_RESOURCE

struct dds_DynamicTypeSupport_deleter {
	::dds_ReturnCode_t operator()(::dds_DynamicTypeSupport* raw) const noexcept {
		return ::dds_DynamicTypeSupport_delete_type_support(raw);
	}
};

using dds_DynamicTypeSupport = Resource<::dds_DynamicTypeSupport, dds_DynamicTypeSupport_deleter>;

inline ::dds_ReturnCode_t dds_DynamicTypeSupport_delete_type_support(dds_DynamicTypeSupport& value) noexcept {
	return value.close();
}

struct dds_DynamicType_deleter {
	::dds_DynamicTypeBuilderFactory* m_factory{};
	std::shared_ptr<void> m_supportOwner;

	explicit dds_DynamicType_deleter(::dds_DynamicTypeBuilderFactory* factory = nullptr) noexcept
	    : m_factory(factory) {
	}

	detail::NativeOwnership GetNativeOwnership() const noexcept {
		if(m_supportOwner)
			return detail::NativeOwnership::Borrowed;
		return detail::NativeOwnership::Transferable;
	}

	::dds_ReturnCode_t operator()(::dds_DynamicType* raw) const noexcept {
		if(m_supportOwner)
			return dds_RETCODE_OK;
		auto* factory{m_factory};
		if(!factory)
			factory = ::dds_DynamicTypeBuilderFactory_get_instance();
		return ::dds_DynamicTypeBuilderFactory_delete_type(factory, raw);
	}
};

using dds_DynamicType = Resource<::dds_DynamicType, dds_DynamicType_deleter>;

struct dds_DynamicData_deleter {
	::dds_DynamicDataFactory* m_factory{};
	::dds_DynamicData* m_parent{};
	std::shared_ptr<void> m_parentOwner;
	std::shared_ptr<void> m_typeOwner;

	explicit dds_DynamicData_deleter(::dds_DynamicDataFactory* factory = nullptr) noexcept
	    : m_factory(factory) {
	}

	detail::NativeOwnership GetNativeOwnership() const noexcept {
		if(m_parent || m_typeOwner)
			return detail::NativeOwnership::Dependent;
		return detail::NativeOwnership::Transferable;
	}

	::dds_ReturnCode_t operator()(::dds_DynamicData* raw) const noexcept {
		if(m_parent)
			return ::dds_DynamicData_return_loaned_value(m_parent, raw);
		auto* factory{m_factory};
		if(!factory)
			factory = ::dds_DynamicDataFactory_get_instance();
		return ::dds_DynamicDataFactory_delete_data(factory, raw);
	}
};

using dds_DynamicData = Resource<::dds_DynamicData, dds_DynamicData_deleter>;

namespace detail {
struct DynamicDataAccess {
	template <typename TAllocate>
	static dds_DynamicTypeSupport CreateSupport(TAllocate allocate) {
		auto support{dds_DynamicTypeSupport::Create(allocate)};
		if(support)
			ResourceAccess::Register(support, ::dds_DynamicTypeSupport_get_dynamictype(support.get()));
		return support;
	}

	static std::shared_ptr<void> GetTypeOwner(const ::dds_DynamicType* type) {
		auto owner{ResourceAccess::Lookup<dds_DynamicType>(type)};
		if(owner)
			return owner.GetOwner();
		return ResourceAccess::Lookup<dds_DynamicTypeSupport>(type).GetOwner();
	}

	static std::shared_ptr<void> GetTypeOwner(const dds_DynamicType& type) noexcept {
		const auto* cleanup{type.GetDeleter()};
		if(cleanup && cleanup->m_supportOwner)
			return cleanup->m_supportOwner;
		return type.GetOwner();
	}

	static std::shared_ptr<void> GetTypeOwner(const dds_DynamicData& data) noexcept {
		auto* cleanup{data.GetDeleter()};
		if(!cleanup)
			return {};
		return cleanup->m_typeOwner;
	}

	template <typename TAllocate>
	static dds_DynamicData Loan(const dds_DynamicData& owner, ::dds_DynamicData* parent, TAllocate allocate) {
		if(!parent)
			return {};

		dds_DynamicData_deleter cleanup;
		cleanup.m_parent = parent;
		cleanup.m_parentOwner = owner.GetOwner();
		cleanup.m_typeOwner = GetTypeOwner(owner);
		return ResourceAccess::Create<dds_DynamicData>(allocate, std::move(cleanup));
	}

	static dds_DynamicData Clone(::dds_DynamicData* raw, std::shared_ptr<void> typeOwner) {
		if(!raw)
			return {};
		dds_DynamicData_deleter cleanup;
		cleanup.m_typeOwner = std::move(typeOwner);
		return ResourceAccess::Create<dds_DynamicData>(
		    [raw]() { return ::dds_DynamicData_clone(raw); }, std::move(cleanup));
	}

	static dds_DynamicDataSeq CloneSequence(::dds_DynamicDataSeq* raw, const std::shared_ptr<DataReader>& reader) {
		if(!raw)
			return {};

		const uint32_t count{::dds_DynamicDataSeq_length(raw)};
		auto items{std::make_shared<std::vector<dds_DynamicData>>()};
		items->reserve(count);
		dds_DynamicDataSeq_deleter cleanup{items};
		auto result{dds_DynamicDataSeq::Create(
		    [raw]() { return ::dds_DynamicDataSeq_create(::dds_DynamicDataSeq_capacity(raw)); }, std::move(cleanup))};
		if(!result)
			return {};

		for(uint32_t index{}; index < count; ++index) {
			auto* item{static_cast<::dds_DynamicData*>(::dds_DynamicDataSeq_get(raw, index))};
			dds_DynamicData value;
			// Keep the reader alive because cloned samples still use its type after the loan is returned.
			if(reader)
				value = Clone(item, reader);
			else
				value = ResourceAccess::Lookup<dds_DynamicData>(item);
			if(item && !value)
				return {};
			if(!::dds_DynamicDataSeq_add(result.get(), value.get()))
				return {};
			if(value)
				items->push_back(std::move(value));
		}
		result.m_state->m_retainsSequenceItems = !items->empty();
		return result;
	}

	static ::dds_ReturnCode_t Delete(::dds_DynamicTypeBuilderFactory* factory, dds_DynamicType& type) noexcept {
		if(auto* cleanup = type.GetDeleter()) {
			if(cleanup->m_supportOwner || (cleanup->m_factory && cleanup->m_factory != factory))
				return dds_RETCODE_PRECONDITION_NOT_MET;
		}
		return type.CloseWith([factory](::dds_DynamicType* raw) {
			return ::dds_DynamicTypeBuilderFactory_delete_type(factory, raw);
		});
	}

	static ::dds_ReturnCode_t Delete(::dds_DynamicDataFactory* factory, dds_DynamicData& data) noexcept {
		const auto* cleanup{data.GetDeleter()};
		if(!cleanup)
			return dds_RETCODE_BAD_PARAMETER;
		if(cleanup->m_parent || (cleanup->m_factory && cleanup->m_factory != factory))
			return dds_RETCODE_PRECONDITION_NOT_MET;
		return data.CloseWith(
		    [factory](::dds_DynamicData* raw) { return ::dds_DynamicDataFactory_delete_data(factory, raw); });
	}
};
} // namespace detail

inline dds_DynamicData dds_DynamicData_lookup(::dds_DynamicData* raw) {
	return detail::ResourceAccess::Lookup<dds_DynamicData>(raw);
}

inline dds_DynamicDataSeq dds_DynamicDataSeq_clone(::dds_DynamicDataSeq* sequence) {
	auto loan{detail::SequenceLoanAccess::Active<dds_DynamicDataSeq>(sequence)};
	if(loan)
		return detail::DynamicDataAccess::CloneSequence(sequence, loan->m_reader);
	return detail::DynamicDataAccess::CloneSequence(sequence, {});
}

inline ::dds_ReturnCode_t dds_DynamicTypeBuilderFactory_delete_type(
    ::dds_DynamicTypeBuilderFactory* self, dds_DynamicType& type) noexcept {
	return detail::DynamicDataAccess::Delete(self, type);
}

inline ::dds_ReturnCode_t dds_DynamicDataFactory_delete_data(
    ::dds_DynamicDataFactory* self, dds_DynamicData& data) noexcept {
	return detail::DynamicDataAccess::Delete(self, data);
}

inline dds_DynamicData dds_DynamicData_loan_value(const dds_DynamicData& self, ::dds_MemberId_t id) {
	return detail::DynamicDataAccess::Loan(
	    self, self.get(), [&]() { return ::dds_DynamicData_loan_value(self.get(), id); });
}

inline dds_DynamicData dds_DynamicData_loan_value(::dds_DynamicData* self, ::dds_MemberId_t id) {
	auto parent{raii::dds_DynamicData_lookup(self)};
	return detail::DynamicDataAccess::Loan(parent, self, [=]() { return ::dds_DynamicData_loan_value(self, id); });
}

namespace detail {
inline ::dds_ReturnCode_t GetComplexValue(
    ::dds_DynamicData* self, const dds_DynamicData& owner, dds_DynamicData& value, ::dds_MemberId_t id) {
	if(!self)
		return dds_RETCODE_BAD_PARAMETER;
	if(owner && owner.GetOwner() == value.GetOwner())
		return dds_RETCODE_PRECONDITION_NOT_MET;

	::dds_ReturnCode_t ret{};
	auto next{DynamicDataAccess::Loan(owner, self, [&]() -> ::dds_DynamicData* {
		::dds_DynamicData* raw{};
		ret = ::dds_DynamicData_get_complex_value(self, &raw, id);
		return raw;
	})};
	if(ret != dds_RETCODE_OK)
		return ret;
	if(!next)
		return dds_RETCODE_OUT_OF_RESOURCES;

	ret = value.close();
	if(ret == dds_RETCODE_OK)
		value = std::move(next);
	return ret;
}
} // namespace detail

inline ::dds_ReturnCode_t dds_DynamicData_get_complex_value(
    const dds_DynamicData& self, dds_DynamicData& value, ::dds_MemberId_t id) {
	return detail::GetComplexValue(self.get(), self, value, id);
}

inline ::dds_ReturnCode_t dds_DynamicData_get_complex_value(
    ::dds_DynamicData* self, dds_DynamicData& value, ::dds_MemberId_t id) {
	return detail::GetComplexValue(self, raii::dds_DynamicData_lookup(self), value, id);
}

using ::dds_DynamicData_get_complex_value;

template <typename TParent>
inline ::dds_ReturnCode_t dds_DynamicData_get_complex_value(
    const TParent& self, dds_DynamicData* value, ::dds_MemberId_t id) {
	if(!value)
		return dds_RETCODE_BAD_PARAMETER;
	return raii::dds_DynamicData_get_complex_value(self, *value, id);
}

inline ::dds_ReturnCode_t dds_DynamicData_return_loaned_value(
    ::dds_DynamicData* self, dds_DynamicData& value) noexcept {
	if(!self || !value)
		return dds_RETCODE_BAD_PARAMETER;
	if(value.GetDeleter()->m_parent != self)
		return dds_RETCODE_PRECONDITION_NOT_MET;
	return value.close();
}

inline ::dds_ReturnCode_t dds_DynamicData_return_loaned_value(
    const dds_DynamicData& self, dds_DynamicData& value) noexcept {
	return raii::dds_DynamicData_return_loaned_value(self.get(), value);
}

struct dds_Memory_deleter {
	::dds_ReturnCode_t operator()(void* raw) const noexcept {
		::dds_free(raw);
		return dds_RETCODE_OK;
	}
};

using dds_Memory = Resource<void, dds_Memory_deleter>;
using dds_String = Resource<char, dds_Memory_deleter>;

namespace detail {
template <typename TItem>
struct NamedStringItemDeleter {
	::dds_ReturnCode_t operator()(TItem* item) const noexcept {
		::dds_free(item->name);
		::dds_free(item->value);
		::dds_free(item);
		return dds_RETCODE_OK;
	}
};

struct BinaryPropertyDeleter {
	::dds_ReturnCode_t operator()(::dds_BinaryProperty_t* item) const noexcept {
		::dds_free(item->name);
		::dds_OctetSeq_delete(item->value);
		::dds_free(item);
		return dds_RETCODE_OK;
	}
};
} // namespace detail

using dds_Property_t = Resource<::dds_Property_t, detail::NamedStringItemDeleter<::dds_Property_t>>;
using dds_Tag = Resource<::dds_Tag, detail::NamedStringItemDeleter<::dds_Tag>>;
using dds_BinaryProperty_t = Resource<::dds_BinaryProperty_t, detail::BinaryPropertyDeleter>;

inline void dds_free(dds_Memory& value) noexcept {
	value.reset();
}

inline void dds_free(dds_String& value) noexcept {
	value.reset();
}

inline dds_Memory dds_malloc(size_t size) {
	return dds_Memory::Create([=]() { return ::dds_malloc(size); });
}

inline dds_Memory dds_calloc(size_t count, size_t size) {
	return dds_Memory::Create([=]() { return ::dds_calloc(count, size); });
}

inline dds_String dds_strdup(const char* str) {
	return dds_String::Create([=]() { return ::dds_strdup(str); });
}

using dds_Wstring = Resource<uint16_t, dds_Memory_deleter>;
using dds_WideString = Resource<wchar_t, dds_Memory_deleter>;

inline dds_Wstring dds_wcsdup(const uint16_t* value) {
	return dds_Wstring::Create([value]() { return ::dds_wcsdup(value); });
}

namespace detail {
struct SequenceItemAccess {
	template <typename TSequence, typename TItem, typename TAdd>
	static bool Add(TSequence& sequence, TItem& value, TAdd add) noexcept {
		if(!sequence || !value || !CanTransfer(value))
			return false;
		if(!add(sequence.get(), value.get()))
			return false;
		Transfer(value);
		return true;
	}

	template <typename TSequence, typename TItem, typename TAdd, typename TRemove, typename TLength>
	static bool AddArray(TSequence& sequence, std::vector<TItem>& values, TAdd add, TRemove remove, TLength length) {
		if(!sequence)
			return false;
		for(const auto& value : values) {
			if(value && !CanTransfer(value))
				return false;
		}

		const uint32_t originalLength{length(sequence.get())};
		for(const auto& value : values) {
			if(!add(sequence.get(), value.get())) {
				while(length(sequence.get()) > originalLength) {
					(void)remove(sequence.get(), length(sequence.get()) - 1);
				}
				return false;
			}
		}
		for(auto& value : values) {
			if(value)
				Transfer(value);
		}
		return true;
	}

	template <typename TItem>
	static TItem Copy(const typename TItem::native_type* source) {
		using TRaw = typename TItem::native_type;
		if(!source)
			return {};
		auto value{TItem::Create([]() { return static_cast<TRaw*>(::dds_calloc(1, sizeof(TRaw))); })};
		if(!value || !CopyFields(*source, *value.get()))
			return {};
		return value;
	}

	template <typename TItem, typename TSequence, typename TRawItem, typename TAdd, typename TRemove, typename TLength>
	static bool AddArrayCopy(
	    TSequence& sequence, TRawItem* const* values, uint32_t size, TAdd add, TRemove remove, TLength length) {
		if(!sequence || (!values && size != 0))
			return false;

		std::vector<TItem> copies;
		copies.reserve(size);
		for(uint32_t index{}; index < size; ++index) {
			auto* raw{static_cast<const typename TItem::native_type*>(values[index])};
			auto value{Copy<TItem>(raw)};
			if(values[index] && !value)
				return false;
			copies.push_back(std::move(value));
		}
		return AddArray(sequence, copies, add, remove, length);
	}

private:
	template <typename TItem>
	static bool CanTransfer(const TItem& value) noexcept {
		// Use this helper only for items that the sequence can free directly.
		return value.m_state.use_count() == 1 && !value.m_state->m_registry && !value.m_state->m_loan;
	}

	template <typename TItem>
	static void Transfer(TItem& value) noexcept {
		value.ClearRaw();
		value.reset();
	}

	static bool CopyField(const char* source, char*& destination) noexcept {
		if(!source)
			return true;
		destination = ::dds_strdup(source);
		return destination != nullptr;
	}

	static bool CopyField(::dds_OctetSeq* source, ::dds_OctetSeq*& destination) noexcept {
		if(!source)
			return true;
		destination = ::dds_OctetSeq_clone(source);
		return destination != nullptr;
	}

	template <typename TItem>
	static bool CopyProperty(const TItem& source, TItem& destination) noexcept {
		destination.propagate = source.propagate;
		return CopyField(source.name, destination.name) && CopyField(source.value, destination.value);
	}

	template <typename TItem>
	static bool CopyKeyed(const TItem& source, TItem& destination) noexcept {
		return CopyField(source.key, destination.key) && CopyField(source.value, destination.value);
	}

	static bool CopyFields(const ::dds_Property_t& source, ::dds_Property_t& destination) noexcept {
		return CopyProperty(source, destination);
	}

	static bool CopyFields(const ::dds_BinaryProperty_t& source, ::dds_BinaryProperty_t& destination) noexcept {
		return CopyProperty(source, destination);
	}

	static bool CopyFields(const ::dds_Tag& source, ::dds_Tag& destination) noexcept {
		return CopyField(source.name, destination.name) && CopyField(source.value, destination.value);
	}

	static bool CopyFields(const ::dds_KeyedString& source, ::dds_KeyedString& destination) noexcept {
		return CopyKeyed(source, destination);
	}

	static bool CopyFields(const ::dds_Bytes& source, ::dds_Bytes& destination) noexcept {
		return CopyField(source.value, destination.value);
	}

	static bool CopyFields(const ::dds_KeyedBytes& source, ::dds_KeyedBytes& destination) noexcept {
		return CopyKeyed(source, destination);
	}
};

template <>
inline dds_String SequenceItemAccess::Copy<dds_String>(const char* source) {
	if(!source)
		return {};
	return raii::dds_strdup(source);
}

template <>
inline dds_Wstring SequenceItemAccess::Copy<dds_Wstring>(const uint16_t* source) {
	if(!source)
		return {};
	return raii::dds_wcsdup(source);
}
} // namespace detail

// Copy items passed by pointer. Transfer ownership of rvalues only after successful insertion.
#define RAII_SEQUENCE_ITEM_DEFINE(sequence_type, item_type)                                                            \
	inline bool sequence_type##_add(sequence_type& sequence, item_type&& value) noexcept {                             \
		return detail::SequenceItemAccess::Add(sequence, value, ::sequence_type##_add);                                \
	}                                                                                                                  \
	inline bool sequence_type##_add(sequence_type& sequence, const item_type::native_type* value) {                    \
		if(!sequence)                                                                                                  \
			return false;                                                                                              \
		return raii::sequence_type##_add(sequence, detail::SequenceItemAccess::Copy<item_type>(value));                \
	}                                                                                                                  \
	inline bool sequence_type##_add_at(sequence_type& sequence, uint32_t index, item_type&& value) noexcept {          \
		return detail::SequenceItemAccess::Add(                                                                        \
		    sequence, value, [index](::sequence_type* dst, item_type::native_type* raw) {                              \
			    return ::sequence_type##_add_at(dst, index, raw);                                                      \
		    });                                                                                                        \
	}                                                                                                                  \
	inline bool sequence_type##_add_at(sequence_type& sequence, uint32_t index, const item_type::native_type* value) { \
		if(!sequence)                                                                                                  \
			return false;                                                                                              \
		return raii::sequence_type##_add_at(sequence, index, detail::SequenceItemAccess::Copy<item_type>(value));      \
	}                                                                                                                  \
	inline bool sequence_type##_add_array(sequence_type& sequence, std::vector<item_type>&& values) {                  \
		return detail::SequenceItemAccess::AddArray(                                                                   \
		    sequence, values, ::sequence_type##_add, ::sequence_type##_remove, ::sequence_type##_length);              \
	}                                                                                                                  \
	inline bool sequence_type##_add_array(                                                                             \
	    sequence_type& sequence, const item_type::native_type* const* values, uint32_t size) {                         \
		return detail::SequenceItemAccess::AddArrayCopy<item_type>(                                                    \
		    sequence, values, size, ::sequence_type##_add, ::sequence_type##_remove, ::sequence_type##_length);        \
	}                                                                                                                  \
	inline item_type sequence_type##_remove(sequence_type& sequence, uint32_t index) {                                 \
		if(!sequence)                                                                                                  \
			return {};                                                                                                 \
		return item_type::Create([&]() { return ::sequence_type##_remove(sequence.get(), index); });                   \
	}

RAII_SEQUENCE_ITEM_DEFINE(dds_StringSeq, dds_String)
RAII_SEQUENCE_ITEM_DEFINE(dds_WstringSeq, dds_Wstring)
RAII_SEQUENCE_ITEM_DEFINE(dds_PropertySeq, dds_Property_t)
RAII_SEQUENCE_ITEM_DEFINE(dds_BinaryPropertySeq, dds_BinaryProperty_t)
RAII_SEQUENCE_ITEM_DEFINE(dds_TagSeq, dds_Tag)
RAII_SEQUENCE_ITEM_DEFINE(dds_KeyedStringSeq, dds_KeyedString)
RAII_SEQUENCE_ITEM_DEFINE(dds_BytesSeq, dds_Bytes)
RAII_SEQUENCE_ITEM_DEFINE(dds_KeyedBytesSeq, dds_KeyedBytes)
#undef RAII_SEQUENCE_ITEM_DEFINE

#define RAII_SEQUENCE_VOID_ITEM_ARRAY_DEFINE(sequence_type, item_type)                                                 \
	template <typename TItem>                                                                                          \
	inline typename std::enable_if<std::is_void<TItem>::value, bool>::type sequence_type##_add_array(                  \
	    sequence_type& sequence, TItem* const* values, uint32_t size) {                                                \
		return detail::SequenceItemAccess::AddArrayCopy<item_type>(                                                    \
		    sequence, values, size, ::sequence_type##_add, ::sequence_type##_remove, ::sequence_type##_length);        \
	}

RAII_SEQUENCE_VOID_ITEM_ARRAY_DEFINE(dds_PropertySeq, dds_Property_t)
RAII_SEQUENCE_VOID_ITEM_ARRAY_DEFINE(dds_BinaryPropertySeq, dds_BinaryProperty_t)
RAII_SEQUENCE_VOID_ITEM_ARRAY_DEFINE(dds_TagSeq, dds_Tag)
#undef RAII_SEQUENCE_VOID_ITEM_ARRAY_DEFINE

inline dds_DynamicType dds_DynamicTypeBuilder_build(::dds_DynamicTypeBuilder* self) {
	return detail::ResourceAccess::Create<dds_DynamicType>([self]() { return ::dds_DynamicTypeBuilder_build(self); });
}

inline dds_DynamicType dds_DynamicTypeBuilderFactory_get_primitive_type(
    ::dds_DynamicTypeBuilderFactory* self, ::dds_TypeKind kind) {
	return detail::ResourceAccess::Create<dds_DynamicType>(
	    [=]() { return ::dds_DynamicTypeBuilderFactory_get_primitive_type(self, kind); },
	    dds_DynamicType_deleter{self});
}

inline dds_DynamicTypeMember dds_DynamicTypeMember_alloc() {
	return dds_DynamicTypeMember::Create([]() { return ::dds_DynamicTypeMember_alloc(); });
}

inline dds_DynamicTypeSupport dds_DynamicTypeSupport_create_type_support(::dds_DynamicType* type) {
	return detail::DynamicDataAccess::CreateSupport(
	    [type]() { return ::dds_DynamicTypeSupport_create_type_support(type); });
}

inline dds_DynamicType dds_DynamicTypeSupport_get_dynamictype(const dds_DynamicTypeSupport& self) {
	if(!self)
		return {};
	dds_DynamicType_deleter cleanup;
	cleanup.m_supportOwner = self.GetOwner();
	return dds_DynamicType::Create(
	    [&]() { return ::dds_DynamicTypeSupport_get_dynamictype(self.get()); }, std::move(cleanup));
}

namespace detail {
inline dds_DynamicData CreateDynamicData(
    ::dds_DynamicDataFactory* factory, const ::dds_DynamicType* type, std::shared_ptr<void> typeOwner) {
	dds_DynamicData_deleter cleanup(factory);
	cleanup.m_typeOwner = std::move(typeOwner);
	return ResourceAccess::Create<dds_DynamicData>(
	    [=]() { return ::dds_DynamicDataFactory_create_data(factory, type); }, std::move(cleanup));
}
} // namespace detail

// The caller must keep types unknown to the RAII registry alive.
inline dds_DynamicData dds_DynamicDataFactory_create_data(
    ::dds_DynamicDataFactory* self, const ::dds_DynamicType* type) {
	return detail::CreateDynamicData(self, type, detail::DynamicDataAccess::GetTypeOwner(type));
}

inline dds_DynamicData dds_DynamicDataFactory_create_data(::dds_DynamicDataFactory* self, const dds_DynamicType& type) {
	return detail::CreateDynamicData(self, type.get(), detail::DynamicDataAccess::GetTypeOwner(type));
}

inline dds_DynamicData dds_DynamicData_clone(const dds_DynamicData& self) {
	return detail::DynamicDataAccess::Clone(self.get(), detail::DynamicDataAccess::GetTypeOwner(self));
}

inline dds_DynamicData dds_DynamicData_clone(::dds_DynamicData* self) {
	auto owner{raii::dds_DynamicData_lookup(self)};
	return detail::DynamicDataAccess::Clone(self, detail::DynamicDataAccess::GetTypeOwner(owner));
}

inline dds_TypeSupport dds_TypeSupport_create(const char* metaStr) {
	return detail::ResourceAccess::Create<dds_TypeSupport>([&]() { return ::dds_TypeSupport_create(metaStr); });
}

inline dds_TypeSupport dds_TypeSupport_create2(const char** metaStrArray, uint32_t metaStrArrayLen) {
	return detail::ResourceAccess::Create<dds_TypeSupport>(
	    [&]() { return ::dds_TypeSupport_create2(metaStrArray, metaStrArrayLen); });
}

inline dds_TypeSupport dds_TypeSupport_early_create(const char* metaStr) {
	return detail::ResourceAccess::Create<dds_TypeSupport>([&]() { return ::dds_TypeSupport_early_create(metaStr); });
}

inline dds_TypeSupport dds_TypeSupport_early_create2(const char** metaStrArray, uint32_t metaStrArrayLen) {
	return detail::ResourceAccess::Create<dds_TypeSupport>(
	    [&]() { return ::dds_TypeSupport_early_create2(metaStrArray, metaStrArrayLen); });
}

inline dds_Memory dds_TypeSupport_serialize(::dds_TypeSupport* self, void* data, size_t* outputSize) {
	return dds_Memory::Create([&]() { return ::dds_TypeSupport_serialize(self, data, outputSize); });
}

inline dds_Memory dds_TypeSupport_serialize_w_repr_id(
    ::dds_TypeSupport* self, void* data, size_t* outputSize, dds_DataRepresentationId_t reprId) {
	return dds_Memory::Create([&]() { return ::dds_TypeSupport_serialize_w_repr_id(self, data, outputSize, reprId); });
}

inline dds_Memory dds_StringTypeSupport_serialize(::dds_String data, size_t* outputSize) {
	return dds_Memory::Create([&]() { return ::dds_StringTypeSupport_serialize(data, outputSize); });
}

inline dds_KeyedString dds_KeyedStringTypeSupport_alloc() {
	return dds_KeyedString::Create([&]() { return ::dds_KeyedStringTypeSupport_alloc(); });
}

inline dds_Memory dds_KeyedStringTypeSupport_serialize(::dds_KeyedString* data, size_t* outputSize) {
	return dds_Memory::Create([&]() { return ::dds_KeyedStringTypeSupport_serialize(data, outputSize); });
}

inline dds_KeyedString dds_KeyedStringTypeSupport_deserialize(void* input, size_t inputSize) {
	return dds_KeyedString::Create([&]() { return ::dds_KeyedStringTypeSupport_deserialize(input, inputSize); });
}

inline dds_Bytes dds_BytesTypeSupport_alloc() {
	return dds_Bytes::Create([&]() { return ::dds_BytesTypeSupport_alloc(); });
}

inline dds_Memory dds_BytesTypeSupport_serialize(::dds_Bytes* data, size_t* outputSize) {
	return dds_Memory::Create([&]() { return ::dds_BytesTypeSupport_serialize(data, outputSize); });
}

inline dds_Bytes dds_BytesTypeSupport_deserialize(void* input, size_t inputSize) {
	return dds_Bytes::Create([&]() { return ::dds_BytesTypeSupport_deserialize(input, inputSize); });
}

inline dds_KeyedBytes dds_KeyedBytesTypeSupport_alloc() {
	return dds_KeyedBytes::Create([&]() { return ::dds_KeyedBytesTypeSupport_alloc(); });
}

inline dds_Memory dds_KeyedBytesTypeSupport_serialize(::dds_KeyedBytes* data, size_t* outputSize) {
	return dds_Memory::Create([&]() { return ::dds_KeyedBytesTypeSupport_serialize(data, outputSize); });
}

inline dds_KeyedBytes dds_KeyedBytesTypeSupport_deserialize(void* input, size_t inputSize) {
	return dds_KeyedBytes::Create([&]() { return ::dds_KeyedBytesTypeSupport_deserialize(input, inputSize); });
}

inline dds_String dds_DomainParticipantFactory_dump_as_string(::dds_DomainParticipantFactory* self) {
	return dds_String::Create([&]() { return ::dds_DomainParticipantFactory_dump_as_string(self); });
}

inline dds_TypeSupport dds_TypeSupport_raw_create() {
	return detail::ResourceAccess::Create<dds_TypeSupport>([&]() { return ::dds_TypeSupport_raw_create(); });
}

inline dds_TypeSupport dds_TypeSupport_raw_create_w_key() {
	return detail::ResourceAccess::Create<dds_TypeSupport>([&]() { return ::dds_TypeSupport_raw_create_w_key(); });
}

inline dds_TypeSupport dds_TypeSupport_raw_create_w_typehash(void* typehash, bool hasKey) {
	return detail::ResourceAccess::Create<dds_TypeSupport>(
	    [&]() { return ::dds_TypeSupport_raw_create_w_typehash(typehash, hasKey); });
}

inline dds_Memory dds_TypeSupport_serialize_be(::dds_TypeSupport* self, void* data, size_t* outputSize) {
	return dds_Memory::Create([&]() { return ::dds_TypeSupport_serialize_be(self, data, outputSize); });
}

inline dds_Memory dds_TypeSupport_serialize_le(::dds_TypeSupport* self, void* data, size_t* outputSize) {
	return dds_Memory::Create([&]() { return ::dds_TypeSupport_serialize_le(self, data, outputSize); });
}

inline dds_String dds_TypeSupport_extract_metastring(::dds_TypeSupport* self) {
	return dds_String::Create([&]() { return ::dds_TypeSupport_extract_metastring(self); });
}

inline dds_DynamicTypeSupport dds_DynamicTypeSupport_create_type_support_with_metastring(char* metastring) {
	return detail::DynamicDataAccess::CreateSupport(
	    [metastring]() { return ::dds_DynamicTypeSupport_create_type_support_with_metastring(metastring); });
}

inline dds_String dds_DomainParticipantQos_dump_as_string(::dds_DomainParticipantQos* qos) {
	return dds_String::Create([&]() { return ::dds_DomainParticipantQos_dump_as_string(qos); });
}

inline dds_String dds_TopicQos_dump_as_string(::dds_TopicQos* qos) {
	return dds_String::Create([&]() { return ::dds_TopicQos_dump_as_string(qos); });
}

inline dds_String dds_PublisherQos_dump_as_string(::dds_PublisherQos* qos) {
	return dds_String::Create([&]() { return ::dds_PublisherQos_dump_as_string(qos); });
}

inline dds_String dds_SubscriberQos_dump_as_string(::dds_SubscriberQos* qos) {
	return dds_String::Create([&]() { return ::dds_SubscriberQos_dump_as_string(qos); });
}

inline dds_String dds_DataWriterQos_dump_as_string(::dds_DataWriterQos* qos) {
	return dds_String::Create([&]() { return ::dds_DataWriterQos_dump_as_string(qos); });
}

inline dds_String dds_DataReaderQos_dump_as_string(::dds_DataReaderQos* qos) {
	return dds_String::Create([&]() { return ::dds_DataReaderQos_dump_as_string(qos); });
}

struct dds_TypeSupportData_deleter {
	::dds_TypeSupport* m_support{};
	bool m_initialized{true};
	std::shared_ptr<void> m_supportOwner;

	detail::NativeOwnership GetNativeOwnership() const noexcept {
		return detail::NativeOwnership::Dependent;
	}

	::dds_ReturnCode_t operator()(void* raw) const noexcept {
		if(m_initialized)
			::dds_TypeSupport_delete_data(m_support, raw);
		else
			::dds_TypeSupport_free(m_support, raw);
		return dds_RETCODE_OK;
	}
};

using dds_TypeSupportData = Resource<void, dds_TypeSupportData_deleter>;

namespace detail {
struct TypeSupportDataAccess {
	template <typename TAllocate>
	static dds_TypeSupportData Create(
	    ::dds_TypeSupport* support, std::shared_ptr<void> owner, bool initialized, TAllocate allocate) {
		if(!support)
			return {};
		dds_TypeSupportData_deleter cleanup;
		cleanup.m_support = support;
		cleanup.m_supportOwner = std::move(owner);
		cleanup.m_initialized = initialized;
		return dds_TypeSupportData::Create(allocate, std::move(cleanup));
	}

	template <typename TCleanup>
	static void Delete(::dds_TypeSupport* support, dds_TypeSupportData& data, TCleanup cleanup) {
		if(!data)
			return;
		if(!support || data.GetDeleter()->m_support != support)
			throw std::invalid_argument("TypeSupport does not own this sample");
		const auto ret{data.CloseWith([&](void* raw) {
			cleanup(support, raw);
			return dds_RETCODE_OK;
		})};
		if(ret != dds_RETCODE_OK)
			throw std::logic_error("DDS sample is still in use");
	}
};
} // namespace detail

inline dds_TypeSupportData dds_TypeSupport_create_data(const dds_TypeSupport& self) {
	return detail::TypeSupportDataAccess::Create(
	    self.get(), self.GetOwner(), true, [&]() { return ::dds_TypeSupport_create_data(self.get()); });
}

inline dds_TypeSupportData dds_TypeSupport_create_data(::dds_TypeSupport* self) {
	auto owner{detail::ResourceAccess::Lookup<dds_TypeSupport>(self)};
	return detail::TypeSupportDataAccess::Create(
	    self, owner.GetOwner(), true, [=]() { return ::dds_TypeSupport_create_data(self); });
}

inline dds_TypeSupportData dds_TypeSupport_alloc(const dds_TypeSupport& self) {
	return detail::TypeSupportDataAccess::Create(
	    self.get(), self.GetOwner(), false, [&]() { return ::dds_TypeSupport_alloc(self.get()); });
}

inline dds_TypeSupportData dds_TypeSupport_alloc(::dds_TypeSupport* self) {
	auto owner{detail::ResourceAccess::Lookup<dds_TypeSupport>(self)};
	return detail::TypeSupportDataAccess::Create(
	    self, owner.GetOwner(), false, [=]() { return ::dds_TypeSupport_alloc(self); });
}

inline dds_TypeSupportData dds_TypeSupport_clone_data(const dds_TypeSupport& self, void* data) {
	return detail::TypeSupportDataAccess::Create(
	    self.get(), self.GetOwner(), true, [&]() { return ::dds_TypeSupport_clone_data(self.get(), data); });
}

inline dds_TypeSupportData dds_TypeSupport_clone_data(::dds_TypeSupport* self, void* data) {
	auto owner{detail::ResourceAccess::Lookup<dds_TypeSupport>(self)};
	return detail::TypeSupportDataAccess::Create(
	    self, owner.GetOwner(), true, [=]() { return ::dds_TypeSupport_clone_data(self, data); });
}

inline dds_TypeSupportData dds_TypeSupport_deserialize(const dds_TypeSupport& self, void* input, size_t inputSize) {
	return detail::TypeSupportDataAccess::Create(self.get(), self.GetOwner(), true, [&]() {
		return ::dds_TypeSupport_deserialize(self.get(), input, inputSize);
	});
}

inline dds_TypeSupportData dds_TypeSupport_deserialize(::dds_TypeSupport* self, void* input, size_t inputSize) {
	auto owner{detail::ResourceAccess::Lookup<dds_TypeSupport>(self)};
	return detail::TypeSupportDataAccess::Create(
	    self, owner.GetOwner(), true, [=]() { return ::dds_TypeSupport_deserialize(self, input, inputSize); });
}

inline void dds_TypeSupport_delete_data(::dds_TypeSupport* self, dds_TypeSupportData& data) {
	detail::TypeSupportDataAccess::Delete(self, data, ::dds_TypeSupport_delete_data);
}

inline void dds_TypeSupport_delete_data(const dds_TypeSupport& self, dds_TypeSupportData& data) {
	raii::dds_TypeSupport_delete_data(self.get(), data);
}

inline void dds_TypeSupport_free(::dds_TypeSupport* self, dds_TypeSupportData& data) {
	detail::TypeSupportDataAccess::Delete(self, data, ::dds_TypeSupport_free);
}

inline void dds_TypeSupport_free(const dds_TypeSupport& self, dds_TypeSupportData& data) {
	raii::dds_TypeSupport_free(self.get(), data);
}

namespace detail {
inline void SetLoanData(SequenceLoan& loan, ::dds_DataSeq* data) noexcept {
	loan.m_data = data;
}

inline void SetLoanData(SequenceLoan& loan, ::dds_DynamicDataSeq* data) noexcept {
	loan.m_dynamicData = data;
}

inline uint32_t GetLoanOutputLength(::dds_DataSeq* sequence) noexcept {
	return ::dds_DataSeq_length(sequence);
}

inline uint32_t GetLoanOutputLength(::dds_DynamicDataSeq* sequence) noexcept {
	return ::dds_DynamicDataSeq_length(sequence);
}

template <typename TData, typename TRead>
::dds_ReturnCode_t ReadSequence(
    const dds_DataReader& reader, TData& data, dds_SampleInfoSeq& infos, dds_UnsignedLongSeq* sizes, TRead read) {
	if(!reader || !data || !infos || (sizes && !*sizes))
		return dds_RETCODE_BAD_PARAMETER;

	auto loan{std::make_shared<SequenceLoan>()};
	loan->m_reader = SharedImpl(reader);
	SetLoanData(*loan, data.get());
	loan->m_infos = infos.get();

	auto ret{SequenceLoanAccess::Prepare(data)};
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = SequenceLoanAccess::Prepare(infos);
	if(ret != dds_RETCODE_OK)
		return ret;
	if(sizes) {
		ret = SequenceLoanAccess::Prepare(*sizes);
		if(ret != dds_RETCODE_OK)
			return ret;
		loan->m_sizes = sizes->get();
	}

	ret = SequenceLoanAccess::Finish(data);
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = SequenceLoanAccess::Finish(infos);
	if(ret != dds_RETCODE_OK)
		return ret;
	if(sizes) {
		ret = SequenceLoanAccess::Finish(*sizes);
		if(ret != dds_RETCODE_OK)
			return ret;
	}

	// Mixing caller-supplied items with SDK loans makes their cleanup policy ambiguous.
	if(GetLoanOutputLength(data.get()) != 0 || ::dds_SampleInfoSeq_length(infos.get()) != 0 ||
	    (sizes && ::dds_UnsignedLongSeq_length(sizes->get()) != 0))
		return dds_RETCODE_PRECONDITION_NOT_MET;

	ret = read();
	if(ret != dds_RETCODE_OK)
		return ret;

	loan->m_active = true;
	SequenceLoanAccess::Commit(data, loan);
	SequenceLoanAccess::Commit(infos, loan);
	if(sizes)
		SequenceLoanAccess::Commit(*sizes, loan);
	return ret;
}

template <typename TData, typename TReturn>
::dds_ReturnCode_t ReturnSequence(const dds_DataReader& reader,
    TData& data,
    dds_SampleInfoSeq& infos,
    dds_UnsignedLongSeq* sizes,
    TReturn returnNative) noexcept {
	if(!reader || !data || !infos || (sizes && !*sizes))
		return dds_RETCODE_BAD_PARAMETER;

	auto loan{SequenceLoanAccess::Active(data)};
	auto infoLoan{SequenceLoanAccess::Active(infos)};
	std::shared_ptr<SequenceLoan> sizeLoan;
	if(sizes)
		sizeLoan = SequenceLoanAccess::Active(*sizes);

	if(loan) {
		if(loan != infoLoan || **loan->m_reader != reader.get())
			return dds_RETCODE_PRECONDITION_NOT_MET;
		if(sizes && loan != sizeLoan)
			return dds_RETCODE_PRECONDITION_NOT_MET;
		if(!sizes && loan->m_sizes)
			return dds_RETCODE_PRECONDITION_NOT_MET;
		return loan->Finish();
	}

	if(infoLoan || sizeLoan)
		return dds_RETCODE_PRECONDITION_NOT_MET;
	return returnNative();
}
} // namespace detail

// Items cloned from an active loan remain valid after the loan is returned.
inline dds_DynamicDataSeq dds_DynamicDataSeq_clone(const dds_DynamicDataSeq& sequence) {
	return raii::dds_DynamicDataSeq_clone(sequence.get());
}

inline ::dds_ReturnCode_t dds_DataReader_read(const dds_DataReader& self,
    dds_DataSeq& dataValues,
    dds_SampleInfoSeq& sampleInfos,
    int32_t maxSamples,
    ::dds_SampleStateMask sampleStates,
    ::dds_ViewStateMask viewStates,
    ::dds_InstanceStateMask instanceStates) {
	return detail::ReadSequence(self, dataValues, sampleInfos, nullptr, [&]() {
		return ::dds_DataReader_read(
		    self, dataValues, sampleInfos, maxSamples, sampleStates, viewStates, instanceStates);
	});
}

inline ::dds_ReturnCode_t dds_DataReader_take(const dds_DataReader& self,
    dds_DataSeq& dataValues,
    dds_SampleInfoSeq& sampleInfos,
    int32_t maxSamples,
    ::dds_SampleStateMask sampleStates,
    ::dds_ViewStateMask viewStates,
    ::dds_InstanceStateMask instanceStates) {
	return detail::ReadSequence(self, dataValues, sampleInfos, nullptr, [&]() {
		return ::dds_DataReader_take(
		    self, dataValues, sampleInfos, maxSamples, sampleStates, viewStates, instanceStates);
	});
}

inline ::dds_ReturnCode_t dds_DataReader_read_w_condition(const dds_DataReader& self,
    dds_DataSeq& dataValues,
    dds_SampleInfoSeq& sampleInfos,
    int32_t maxSamples,
    const ::dds_ReadCondition* aCondition) {
	return detail::ReadSequence(self, dataValues, sampleInfos, nullptr, [&]() {
		return ::dds_DataReader_read_w_condition(self, dataValues, sampleInfos, maxSamples, aCondition);
	});
}

inline ::dds_ReturnCode_t dds_DataReader_take_w_condition(const dds_DataReader& self,
    dds_DataSeq& dataValues,
    dds_SampleInfoSeq& sampleInfos,
    int32_t maxSamples,
    const ::dds_ReadCondition* aCondition) {
	return detail::ReadSequence(self, dataValues, sampleInfos, nullptr, [&]() {
		return ::dds_DataReader_take_w_condition(self, dataValues, sampleInfos, maxSamples, aCondition);
	});
}

inline ::dds_ReturnCode_t dds_DataReader_read_instance(const dds_DataReader& self,
    dds_DataSeq& dataValues,
    dds_SampleInfoSeq& sampleInfos,
    int32_t maxSamples,
    ::dds_InstanceHandle_t aHandle,
    ::dds_SampleStateMask sampleStates,
    ::dds_ViewStateMask viewStates,
    ::dds_InstanceStateMask instanceStates) {
	return detail::ReadSequence(self, dataValues, sampleInfos, nullptr, [&]() {
		return ::dds_DataReader_read_instance(
		    self, dataValues, sampleInfos, maxSamples, aHandle, sampleStates, viewStates, instanceStates);
	});
}

inline ::dds_ReturnCode_t dds_DataReader_take_instance(const dds_DataReader& self,
    dds_DataSeq& dataValues,
    dds_SampleInfoSeq& sampleInfos,
    int32_t maxSamples,
    ::dds_InstanceHandle_t aHandle,
    ::dds_SampleStateMask sampleStates,
    ::dds_ViewStateMask viewStates,
    ::dds_InstanceStateMask instanceStates) {
	return detail::ReadSequence(self, dataValues, sampleInfos, nullptr, [&]() {
		return ::dds_DataReader_take_instance(
		    self, dataValues, sampleInfos, maxSamples, aHandle, sampleStates, viewStates, instanceStates);
	});
}

inline ::dds_ReturnCode_t dds_DataReader_read_next_instance(const dds_DataReader& self,
    dds_DataSeq& dataValues,
    dds_SampleInfoSeq& sampleInfos,
    int32_t maxSamples,
    ::dds_InstanceHandle_t previousHandle,
    ::dds_SampleStateMask sampleStates,
    ::dds_ViewStateMask viewStates,
    ::dds_InstanceStateMask instanceStates) {
	return detail::ReadSequence(self, dataValues, sampleInfos, nullptr, [&]() {
		return ::dds_DataReader_read_next_instance(
		    self, dataValues, sampleInfos, maxSamples, previousHandle, sampleStates, viewStates, instanceStates);
	});
}

inline ::dds_ReturnCode_t dds_DataReader_take_next_instance(const dds_DataReader& self,
    dds_DataSeq& dataValues,
    dds_SampleInfoSeq& sampleInfos,
    int32_t maxSamples,
    ::dds_InstanceHandle_t previousHandle,
    ::dds_SampleStateMask sampleStates,
    ::dds_ViewStateMask viewStates,
    ::dds_InstanceStateMask instanceStates) {
	return detail::ReadSequence(self, dataValues, sampleInfos, nullptr, [&]() {
		return ::dds_DataReader_take_next_instance(
		    self, dataValues, sampleInfos, maxSamples, previousHandle, sampleStates, viewStates, instanceStates);
	});
}

inline ::dds_ReturnCode_t dds_DataReader_read_next_instance_w_condition(const dds_DataReader& self,
    dds_DataSeq& dataValues,
    dds_SampleInfoSeq& sampleInfos,
    int32_t maxSamples,
    ::dds_InstanceHandle_t previousHandle,
    const ::dds_ReadCondition* aCondition) {
	return detail::ReadSequence(self, dataValues, sampleInfos, nullptr, [&]() {
		return ::dds_DataReader_read_next_instance_w_condition(
		    self, dataValues, sampleInfos, maxSamples, previousHandle, aCondition);
	});
}

inline ::dds_ReturnCode_t dds_DataReader_take_next_instance_w_condition(const dds_DataReader& self,
    dds_DataSeq& dataValues,
    dds_SampleInfoSeq& sampleInfos,
    int32_t maxSamples,
    ::dds_InstanceHandle_t previousHandle,
    const ::dds_ReadCondition* aCondition) {
	return detail::ReadSequence(self, dataValues, sampleInfos, nullptr, [&]() {
		return ::dds_DataReader_take_next_instance_w_condition(
		    self, dataValues, sampleInfos, maxSamples, previousHandle, aCondition);
	});
}

inline ::dds_ReturnCode_t dds_DynamicDataReader_read(const dds_DataReader& self,
    dds_DynamicDataSeq& dataValues,
    dds_SampleInfoSeq& sampleInfos,
    int32_t maxSamples,
    ::dds_SampleStateMask sampleStates,
    ::dds_ViewStateMask viewStates,
    ::dds_InstanceStateMask instanceStates) {
	return detail::ReadSequence(self, dataValues, sampleInfos, nullptr, [&]() {
		return ::dds_DynamicDataReader_read(
		    self, dataValues, sampleInfos, maxSamples, sampleStates, viewStates, instanceStates);
	});
}

inline ::dds_ReturnCode_t dds_DynamicDataReader_take(const dds_DataReader& self,
    dds_DynamicDataSeq& dataValues,
    dds_SampleInfoSeq& sampleInfos,
    int32_t maxSamples,
    ::dds_SampleStateMask sampleStates,
    ::dds_ViewStateMask viewStates,
    ::dds_InstanceStateMask instanceStates) {
	return detail::ReadSequence(self, dataValues, sampleInfos, nullptr, [&]() {
		return ::dds_DynamicDataReader_take(
		    self, dataValues, sampleInfos, maxSamples, sampleStates, viewStates, instanceStates);
	});
}

inline ::dds_ReturnCode_t dds_DynamicDataReader_read_w_condition(const dds_DataReader& self,
    dds_DynamicDataSeq& dataValues,
    dds_SampleInfoSeq& sampleInfos,
    int32_t maxSamples,
    const ::dds_ReadCondition* aCondition) {
	return detail::ReadSequence(self, dataValues, sampleInfos, nullptr, [&]() {
		return ::dds_DynamicDataReader_read_w_condition(self, dataValues, sampleInfos, maxSamples, aCondition);
	});
}

inline ::dds_ReturnCode_t dds_DynamicDataReader_take_w_condition(const dds_DataReader& self,
    dds_DynamicDataSeq& dataValues,
    dds_SampleInfoSeq& sampleInfos,
    int32_t maxSamples,
    const ::dds_ReadCondition* aCondition) {
	return detail::ReadSequence(self, dataValues, sampleInfos, nullptr, [&]() {
		return ::dds_DynamicDataReader_take_w_condition(self, dataValues, sampleInfos, maxSamples, aCondition);
	});
}

inline ::dds_ReturnCode_t dds_DynamicDataReader_read_instance(const dds_DataReader& self,
    dds_DynamicDataSeq& dataValues,
    dds_SampleInfoSeq& sampleInfos,
    int32_t maxSamples,
    ::dds_InstanceHandle_t aHandle,
    ::dds_SampleStateMask sampleStates,
    ::dds_ViewStateMask viewStates,
    ::dds_InstanceStateMask instanceStates) {
	return detail::ReadSequence(self, dataValues, sampleInfos, nullptr, [&]() {
		return ::dds_DynamicDataReader_read_instance(
		    self, dataValues, sampleInfos, maxSamples, aHandle, sampleStates, viewStates, instanceStates);
	});
}

inline ::dds_ReturnCode_t dds_DynamicDataReader_take_instance(const dds_DataReader& self,
    dds_DynamicDataSeq& dataValues,
    dds_SampleInfoSeq& sampleInfos,
    int32_t maxSamples,
    ::dds_InstanceHandle_t aHandle,
    ::dds_SampleStateMask sampleStates,
    ::dds_ViewStateMask viewStates,
    ::dds_InstanceStateMask instanceStates) {
	return detail::ReadSequence(self, dataValues, sampleInfos, nullptr, [&]() {
		return ::dds_DynamicDataReader_take_instance(
		    self, dataValues, sampleInfos, maxSamples, aHandle, sampleStates, viewStates, instanceStates);
	});
}

inline ::dds_ReturnCode_t dds_DynamicDataReader_read_next_instance(const dds_DataReader& self,
    dds_DynamicDataSeq& dataValues,
    dds_SampleInfoSeq& sampleInfos,
    int32_t maxSamples,
    ::dds_InstanceHandle_t previousHandle,
    ::dds_SampleStateMask sampleStates,
    ::dds_ViewStateMask viewStates,
    ::dds_InstanceStateMask instanceStates) {
	return detail::ReadSequence(self, dataValues, sampleInfos, nullptr, [&]() {
		return ::dds_DynamicDataReader_read_next_instance(
		    self, dataValues, sampleInfos, maxSamples, previousHandle, sampleStates, viewStates, instanceStates);
	});
}

inline ::dds_ReturnCode_t dds_DynamicDataReader_take_next_instance(const dds_DataReader& self,
    dds_DynamicDataSeq& dataValues,
    dds_SampleInfoSeq& sampleInfos,
    int32_t maxSamples,
    ::dds_InstanceHandle_t previousHandle,
    ::dds_SampleStateMask sampleStates,
    ::dds_ViewStateMask viewStates,
    ::dds_InstanceStateMask instanceStates) {
	return detail::ReadSequence(self, dataValues, sampleInfos, nullptr, [&]() {
		return ::dds_DynamicDataReader_take_next_instance(
		    self, dataValues, sampleInfos, maxSamples, previousHandle, sampleStates, viewStates, instanceStates);
	});
}

inline ::dds_ReturnCode_t dds_DynamicDataReader_read_next_instance_w_condition(const dds_DataReader& self,
    dds_DynamicDataSeq& dataValues,
    dds_SampleInfoSeq& sampleInfos,
    int32_t maxSamples,
    ::dds_InstanceHandle_t previousHandle,
    const ::dds_ReadCondition* aCondition) {
	return detail::ReadSequence(self, dataValues, sampleInfos, nullptr, [&]() {
		return ::dds_DynamicDataReader_read_next_instance_w_condition(
		    self, dataValues, sampleInfos, maxSamples, previousHandle, aCondition);
	});
}

inline ::dds_ReturnCode_t dds_DynamicDataReader_take_next_instance_w_condition(const dds_DataReader& self,
    dds_DynamicDataSeq& dataValues,
    dds_SampleInfoSeq& sampleInfos,
    int32_t maxSamples,
    ::dds_InstanceHandle_t previousHandle,
    const ::dds_ReadCondition* aCondition) {
	return detail::ReadSequence(self, dataValues, sampleInfos, nullptr, [&]() {
		return ::dds_DynamicDataReader_take_next_instance_w_condition(
		    self, dataValues, sampleInfos, maxSamples, previousHandle, aCondition);
	});
}

inline ::dds_ReturnCode_t dds_DataReader_read_w_sampleinfoex(const dds_DataReader& self,
    dds_DataSeq& dataValues,
    dds_SampleInfoSeq& sampleInfos,
    int32_t maxSamples,
    ::dds_SampleStateMask sampleStates,
    ::dds_ViewStateMask viewStates,
    ::dds_InstanceStateMask instanceStates) {
	return detail::ReadSequence(self, dataValues, sampleInfos, nullptr, [&]() {
		return ::dds_DataReader_read_w_sampleinfoex(
		    self, dataValues, sampleInfos, maxSamples, sampleStates, viewStates, instanceStates);
	});
}

inline ::dds_ReturnCode_t dds_DataReader_take_w_sampleinfoex(const dds_DataReader& self,
    dds_DataSeq& dataValues,
    dds_SampleInfoSeq& sampleInfos,
    int32_t maxSamples,
    ::dds_SampleStateMask sampleStates,
    ::dds_ViewStateMask viewStates,
    ::dds_InstanceStateMask instanceStates) {
	return detail::ReadSequence(self, dataValues, sampleInfos, nullptr, [&]() {
		return ::dds_DataReader_take_w_sampleinfoex(
		    self, dataValues, sampleInfos, maxSamples, sampleStates, viewStates, instanceStates);
	});
}

inline ::dds_ReturnCode_t dds_DataReader_raw_read(const dds_DataReader& self,
    ::dds_InstanceHandle_t handle,
    dds_DataSeq& rawDataValues,
    dds_SampleInfoSeq& sampleInfos,
    dds_UnsignedLongSeq& rawDataSizes,
    int32_t maxSamples,
    ::dds_SampleStateMask sampleStates,
    ::dds_ViewStateMask viewStates,
    ::dds_InstanceStateMask instanceStates) {
	return detail::ReadSequence(self, rawDataValues, sampleInfos, &rawDataSizes, [&]() {
		return ::dds_DataReader_raw_read(self,
		    handle,
		    rawDataValues,
		    sampleInfos,
		    rawDataSizes,
		    maxSamples,
		    sampleStates,
		    viewStates,
		    instanceStates);
	});
}

inline ::dds_ReturnCode_t dds_DataReader_raw_read_w_sampleinfoex(const dds_DataReader& self,
    ::dds_InstanceHandle_t handle,
    dds_DataSeq& rawDataValues,
    dds_SampleInfoSeq& sampleInfoexs,
    dds_UnsignedLongSeq& rawDataSizes,
    int32_t maxSamples,
    ::dds_SampleStateMask sampleStates,
    ::dds_ViewStateMask viewStates,
    ::dds_InstanceStateMask instanceStates) {
	return detail::ReadSequence(self, rawDataValues, sampleInfoexs, &rawDataSizes, [&]() {
		return ::dds_DataReader_raw_read_w_sampleinfoex(self,
		    handle,
		    rawDataValues,
		    sampleInfoexs,
		    rawDataSizes,
		    maxSamples,
		    sampleStates,
		    viewStates,
		    instanceStates);
	});
}

inline ::dds_ReturnCode_t dds_DataReader_raw_take(const dds_DataReader& self,
    ::dds_InstanceHandle_t handle,
    dds_DataSeq& rawDataValues,
    dds_SampleInfoSeq& sampleInfos,
    dds_UnsignedLongSeq& rawDataSizes,
    int32_t maxSamples,
    ::dds_SampleStateMask sampleStates,
    ::dds_ViewStateMask viewStates,
    ::dds_InstanceStateMask instanceStates) {
	return detail::ReadSequence(self, rawDataValues, sampleInfos, &rawDataSizes, [&]() {
		return ::dds_DataReader_raw_take(self,
		    handle,
		    rawDataValues,
		    sampleInfos,
		    rawDataSizes,
		    maxSamples,
		    sampleStates,
		    viewStates,
		    instanceStates);
	});
}

inline ::dds_ReturnCode_t dds_DataReader_raw_take_w_sampleinfoex(const dds_DataReader& self,
    ::dds_InstanceHandle_t handle,
    dds_DataSeq& rawDataValues,
    dds_SampleInfoSeq& sampleInfoexs,
    dds_UnsignedLongSeq& rawDataSizes,
    int32_t maxSamples,
    ::dds_SampleStateMask sampleStates,
    ::dds_ViewStateMask viewStates,
    ::dds_InstanceStateMask instanceStates) {
	return detail::ReadSequence(self, rawDataValues, sampleInfoexs, &rawDataSizes, [&]() {
		return ::dds_DataReader_raw_take_w_sampleinfoex(self,
		    handle,
		    rawDataValues,
		    sampleInfoexs,
		    rawDataSizes,
		    maxSamples,
		    sampleStates,
		    viewStates,
		    instanceStates);
	});
}

inline ::dds_ReturnCode_t dds_DataReader_return_loan(
    const dds_DataReader& self, dds_DataSeq& data, dds_SampleInfoSeq& infos) noexcept {
	return detail::ReturnSequence(
	    self, data, infos, nullptr, [&]() { return ::dds_DataReader_return_loan(self, data, infos); });
}

inline ::dds_ReturnCode_t dds_DataReader_raw_return_loan(
    const dds_DataReader& self, dds_DataSeq& data, dds_SampleInfoSeq& infos, dds_UnsignedLongSeq& sizes) noexcept {
	return detail::ReturnSequence(
	    self, data, infos, &sizes, [&]() { return ::dds_DataReader_raw_return_loan(self, data, infos, sizes); });
}

inline ::dds_ReturnCode_t dds_DynamicDataReader_return_loan(
    const dds_DataReader& self, dds_DynamicDataSeq& data, dds_SampleInfoSeq& infos) noexcept {
	return detail::ReturnSequence(self, data, infos, nullptr, [&]() {
		if(data.GetDeleter()->m_itemsOwner)
			return dds_RETCODE_PRECONDITION_NOT_MET;
		return ::dds_DynamicDataReader_return_loan(self, data, infos);
	});
}

struct dds_ReaderLoan_deleter {
	::dds_DataReader* m_reader{};
	std::shared_ptr<DataReader> m_owner;

	detail::NativeOwnership GetNativeOwnership() const noexcept {
		return detail::NativeOwnership::Dependent;
	}

	::dds_ReturnCode_t operator()(void* raw) const noexcept {
		if(!m_reader)
			return dds_RETCODE_BAD_PARAMETER;
		return ::dds_DataReader_return_loaned_sample(m_reader, raw);
	}
};

using dds_ReaderLoan = Resource<void, dds_ReaderLoan_deleter>;

struct dds_WriterLoan_deleter {
	::dds_DataWriter* m_writer{};
	std::shared_ptr<DataWriter> m_owner;
	size_t m_capacity{};

	detail::NativeOwnership GetNativeOwnership() const noexcept {
		return detail::NativeOwnership::Dependent;
	}

	::dds_ReturnCode_t operator()(void* raw) const noexcept {
		if(!m_writer)
			return dds_RETCODE_BAD_PARAMETER;
		::dds_DataWriter_return_loaned_sample(m_writer, raw);
		return dds_RETCODE_OK;
	}
};

using dds_WriterLoan = Resource<void, dds_WriterLoan_deleter>;

namespace detail {
template <typename TEntity>
typename TEntity::DDSType* GetLoanEndpoint(typename TEntity::DDSType* raw) noexcept {
	return raw;
}

template <typename TEntity>
typename TEntity::DDSType* GetLoanEndpoint(const DDSEntity<TEntity>& entity) noexcept {
	return entity.get();
}

template <typename TEntity>
std::shared_ptr<TEntity> GetLoanEndpointOwner(typename TEntity::DDSType* raw) {
	return RegisteredOwner<TEntity>(raw);
}

template <typename TEntity>
std::shared_ptr<TEntity> GetLoanEndpointOwner(const DDSEntity<TEntity>& entity) noexcept {
	return SharedImpl(entity);
}

template <typename TLoan, typename TDeleter, typename TAcquire>
::dds_ReturnCode_t AcquireSampleLoan(TLoan& sample, TDeleter cleanup, TAcquire acquire) {
	auto ret{sample.close()};
	if(ret != dds_RETCODE_OK)
		return ret;

	auto next{TLoan::Create(
	    [&]() -> void* {
		    void* raw{};
		    ret = acquire(&raw);
		    return raw;
	    },
	    std::move(cleanup))};
	if(ret == dds_RETCODE_OK)
		sample = std::move(next);
	return ret;
}

template <typename TReader, typename TRead>
::dds_ReturnCode_t ReadSampleLoan(const TReader& self, dds_ReaderLoan& sample, ::dds_SampleInfoEx* info, TRead read) {
	auto* reader{GetLoanEndpoint<DataReader>(self)};
	if(!reader || !info)
		return dds_RETCODE_BAD_PARAMETER;
	dds_ReaderLoan_deleter cleanup;
	cleanup.m_reader = reader;
	cleanup.m_owner = GetLoanEndpointOwner<DataReader>(self);
	if(!cleanup.m_owner && sample && sample.GetDeleter()->m_reader == reader)
		cleanup.m_owner = sample.GetDeleter()->m_owner;
	return AcquireSampleLoan(sample, std::move(cleanup), [=](void** raw) { return read(reader, raw, info); });
}

struct WriterLoanAccess {
	static void SetCapacity(dds_WriterLoan& sample, size_t capacity) noexcept {
		sample.m_state->m_deleter.m_capacity = capacity;
	}

	template <typename TWriter, typename TWrite>
	static ::dds_ReturnCode_t Write(const TWriter& self, dds_WriterLoan& sample, uint32_t size, TWrite write) {
		constexpr uint32_t CDR_HEADER_SIZE{4};
		auto* writer{GetLoanEndpoint<DataWriter>(self)};
		if(!writer || !sample || size < CDR_HEADER_SIZE || size > sample.GetDeleter()->m_capacity)
			return dds_RETCODE_BAD_PARAMETER;
		const auto* cleanup{sample.GetDeleter()};
		if(cleanup->m_writer != writer || sample.m_state.use_count() != 1)
			return dds_RETCODE_PRECONDITION_NOT_MET;

		// The SDK may consume the loan before reporting a write error.
		// Return the original loan first and retain the writer while writing from a separate buffer.
		auto writerOwner{cleanup->m_owner};
		std::unique_ptr<void, decltype(&::dds_free)> payload{::dds_malloc(size), ::dds_free};
		if(!payload)
			return dds_RETCODE_OUT_OF_RESOURCES;
		std::memcpy(payload.get(), sample.get(), size);
		const auto ret{sample.close()};
		if(ret != dds_RETCODE_OK)
			return ret;
		return write(writer, payload.get());
	}
};
} // namespace detail

using ::dds_DataReader_read_next_loaned_sample;
using ::dds_DataReader_return_loaned_sample;
using ::dds_DataReader_take_next_loaned_sample;
using ::dds_DataWriter_get_loan;
using ::dds_DataWriter_raw_write;
using ::dds_DataWriter_raw_write_w_sampleinfoex;
using ::dds_DataWriter_raw_write_w_timestamp;
using ::dds_DataWriter_return_loaned_sample;

template <typename TReader>
inline ::dds_ReturnCode_t dds_DataReader_return_loaned_sample(const TReader& self, dds_ReaderLoan& sample) noexcept {
	auto* reader{detail::GetLoanEndpoint<DataReader>(self)};
	if(!reader || !sample)
		return dds_RETCODE_BAD_PARAMETER;
	if(sample.GetDeleter()->m_reader != reader)
		return dds_RETCODE_PRECONDITION_NOT_MET;
	return sample.close();
}

template <typename TReader>
::dds_ReturnCode_t dds_DataReader_return_loaned_sample(const TReader&, const dds_ReaderLoan&) = delete;

template <typename TWriter>
inline void dds_DataWriter_return_loaned_sample(const TWriter& self, dds_WriterLoan& sample) {
	if(!sample)
		return;
	if(sample.GetDeleter()->m_writer != detail::GetLoanEndpoint<DataWriter>(self))
		throw std::invalid_argument("DataWriter does not own this loan");
	if(sample.close() != dds_RETCODE_OK)
		throw std::logic_error("DDS writer loan is still in use");
}

template <typename TWriter>
void dds_DataWriter_return_loaned_sample(const TWriter&, const dds_WriterLoan&) = delete;

template <typename TWriter>
inline ::dds_ReturnCode_t dds_DataWriter_get_loan(const TWriter& self, dds_WriterLoan& sample, size_t* size) {
	auto* writer{detail::GetLoanEndpoint<DataWriter>(self)};
	if(!writer || !size)
		return dds_RETCODE_BAD_PARAMETER;
	dds_WriterLoan_deleter cleanup;
	cleanup.m_writer = writer;
	cleanup.m_owner = detail::GetLoanEndpointOwner<DataWriter>(self);
	if(!cleanup.m_owner && sample && sample.GetDeleter()->m_writer == writer)
		cleanup.m_owner = sample.GetDeleter()->m_owner;
	const auto ret{detail::AcquireSampleLoan(
	    sample, std::move(cleanup), [=](void** raw) { return ::dds_DataWriter_get_loan(writer, raw, size); })};
	if(sample && ret == dds_RETCODE_OK)
		detail::WriterLoanAccess::SetCapacity(sample, *size);
	return ret;
}

template <typename TWriter>
inline ::dds_ReturnCode_t dds_DataWriter_get_loan(const TWriter& self, dds_WriterLoan* sample, size_t* size) {
	if(!sample)
		return dds_RETCODE_BAD_PARAMETER;
	return raii::dds_DataWriter_get_loan(self, *sample, size);
}

// The original loan is returned before the native write, even if the write later fails.
template <typename TWriter>
inline ::dds_ReturnCode_t dds_DataWriter_raw_write(const TWriter& self, dds_WriterLoan& sample, uint32_t size) {
	return detail::WriterLoanAccess::Write(self, sample, size, [size](::dds_DataWriter* writer, const void* raw) {
		return ::dds_DataWriter_raw_write(writer, raw, size);
	});
}

template <typename TWriter>
inline ::dds_ReturnCode_t dds_DataWriter_raw_write(const TWriter& self, dds_WriterLoan&& sample, uint32_t size) {
	return raii::dds_DataWriter_raw_write(self, sample, size);
}

template <typename TWriter>
::dds_ReturnCode_t dds_DataWriter_raw_write(const TWriter&, const dds_WriterLoan&, uint32_t) = delete;

template <typename TWriter>
inline ::dds_ReturnCode_t dds_DataWriter_raw_write_w_timestamp(
    const TWriter& self, dds_WriterLoan& sample, uint32_t size, const ::dds_Time_t* timestamp) {
	if(!timestamp)
		return dds_RETCODE_BAD_PARAMETER;
	return detail::WriterLoanAccess::Write(self, sample, size, [=](::dds_DataWriter* writer, const void* raw) {
		return ::dds_DataWriter_raw_write_w_timestamp(writer, raw, size, timestamp);
	});
}

template <typename TWriter>
inline ::dds_ReturnCode_t dds_DataWriter_raw_write_w_timestamp(
    const TWriter& self, dds_WriterLoan&& sample, uint32_t size, const ::dds_Time_t* timestamp) {
	return raii::dds_DataWriter_raw_write_w_timestamp(self, sample, size, timestamp);
}

template <typename TWriter>
::dds_ReturnCode_t dds_DataWriter_raw_write_w_timestamp(
    const TWriter&, const dds_WriterLoan&, uint32_t, const ::dds_Time_t*) = delete;

template <typename TWriter>
inline ::dds_ReturnCode_t dds_DataWriter_raw_write_w_sampleinfoex(
    const TWriter& self, dds_WriterLoan& sample, uint32_t size, ::dds_SampleInfoEx* info) {
	if(!info)
		return dds_RETCODE_BAD_PARAMETER;
	return detail::WriterLoanAccess::Write(self, sample, size, [=](::dds_DataWriter* writer, const void* raw) {
		return ::dds_DataWriter_raw_write_w_sampleinfoex(writer, raw, size, info);
	});
}

template <typename TWriter>
inline ::dds_ReturnCode_t dds_DataWriter_raw_write_w_sampleinfoex(
    const TWriter& self, dds_WriterLoan&& sample, uint32_t size, ::dds_SampleInfoEx* info) {
	return raii::dds_DataWriter_raw_write_w_sampleinfoex(self, sample, size, info);
}

template <typename TWriter>
::dds_ReturnCode_t dds_DataWriter_raw_write_w_sampleinfoex(
    const TWriter&, const dds_WriterLoan&, uint32_t, ::dds_SampleInfoEx*) = delete;

template <typename TReader>
inline ::dds_ReturnCode_t dds_DataReader_read_next_loaned_sample(
    const TReader& self, dds_ReaderLoan& sample, ::dds_SampleInfoEx* info) {
	return detail::ReadSampleLoan(self, sample, info, ::dds_DataReader_read_next_loaned_sample);
}

template <typename TReader>
inline ::dds_ReturnCode_t dds_DataReader_read_next_loaned_sample(
    const TReader& self, dds_ReaderLoan* sample, ::dds_SampleInfoEx* info) {
	if(!sample)
		return dds_RETCODE_BAD_PARAMETER;
	return raii::dds_DataReader_read_next_loaned_sample(self, *sample, info);
}

template <typename TReader>
inline ::dds_ReturnCode_t dds_DataReader_take_next_loaned_sample(
    const TReader& self, dds_ReaderLoan& sample, ::dds_SampleInfoEx* info) {
	return detail::ReadSampleLoan(self, sample, info, ::dds_DataReader_take_next_loaned_sample);
}

template <typename TReader>
inline ::dds_ReturnCode_t dds_DataReader_take_next_loaned_sample(
    const TReader& self, dds_ReaderLoan* sample, ::dds_SampleInfoEx* info) {
	if(!sample)
		return dds_RETCODE_BAD_PARAMETER;
	return raii::dds_DataReader_take_next_loaned_sample(self, *sample, info);
}

class StatusCondition {
public:
	using DDSType = ::dds_StatusCondition;

	StatusCondition(::dds_StatusCondition* raw, std::shared_ptr<void> parent) noexcept
	    : m_parent(std::move(parent))
	    , m_raw(raw) {
	}

	StatusCondition(const StatusCondition&) = delete;
	StatusCondition& operator=(const StatusCondition&) = delete;

	::dds_StatusCondition* operator*() noexcept {
		return m_raw;
	}

	const ::dds_StatusCondition* operator*() const noexcept {
		return m_raw;
	}

	StatusCondition& operator=(std::nullptr_t) noexcept {
		m_raw = nullptr;
		return *this;
	}

private:
	std::shared_ptr<void> m_parent;
	::dds_StatusCondition* m_raw;
};

using dds_StatusCondition = DDSEntity<StatusCondition>;

inline dds_StatusCondition dds_DomainParticipant_get_statuscondition(const dds_DomainParticipant& self) {
	if(!self)
		return {};
	auto raw = ::dds_DomainParticipant_get_statuscondition(self);
	if(!raw)
		return {};
	return dds_StatusCondition(detail::MakeOwned<StatusCondition>(raw, detail::SharedImpl(self)));
}

inline dds_StatusCondition dds_Topic_get_statuscondition(const dds_Topic& self) {
	if(!self)
		return {};
	auto raw = ::dds_Topic_get_statuscondition(self);
	if(!raw)
		return {};
	return dds_StatusCondition(detail::MakeOwned<StatusCondition>(raw, detail::SharedImpl(self)));
}

inline dds_StatusCondition dds_Publisher_get_statuscondition(const dds_Publisher& self) {
	if(!self)
		return {};
	auto raw = ::dds_Publisher_get_statuscondition(self);
	if(!raw)
		return {};
	return dds_StatusCondition(detail::MakeOwned<StatusCondition>(raw, detail::SharedImpl(self)));
}

inline dds_StatusCondition dds_Subscriber_get_statuscondition(const dds_Subscriber& self) {
	if(!self)
		return {};
	auto raw = ::dds_Subscriber_get_statuscondition(self);
	if(!raw)
		return {};
	return dds_StatusCondition(detail::MakeOwned<StatusCondition>(raw, detail::SharedImpl(self)));
}

inline dds_StatusCondition dds_DataWriter_get_statuscondition(const dds_DataWriter& self) {
	if(!self)
		return {};
	auto raw = ::dds_DataWriter_get_statuscondition(self);
	if(!raw)
		return {};
	return dds_StatusCondition(detail::MakeOwned<StatusCondition>(raw, detail::SharedImpl(self)));
}

inline dds_StatusCondition dds_DataReader_get_statuscondition(const dds_DataReader& self) {
	if(!self)
		return {};
	auto raw = ::dds_DataReader_get_statuscondition(self);
	if(!raw)
		return {};
	return dds_StatusCondition(detail::MakeOwned<StatusCondition>(raw, detail::SharedImpl(self)));
}

inline ::dds_ReturnCode_t dds_WaitSet_attach_condition(const dds_WaitSet& self, const dds_StatusCondition& condition) {
	if(!self || !condition)
		return dds_RETCODE_BAD_PARAMETER;
	return detail::SharedImpl(self)->Attach(condition);
}

inline ::dds_ReturnCode_t dds_WaitSet_detach_condition(const dds_WaitSet& self, const dds_StatusCondition& condition) {
	if(!self || !condition)
		return dds_RETCODE_BAD_PARAMETER;
	return detail::SharedImpl(self)->Detach(condition);
}

// DynamicTypeBuilder objects and singleton factories are factory-owned in this
// C API: no public per-builder delete exists. Keep those as native C handles.
// Likewise get/lookup/find results remain borrowed and must not be adopted.

inline void dds_free(dds_Wstring& value) noexcept {
	value.reset();
}

inline void dds_free(dds_WideString& value) noexcept {
	value.reset();
}

inline dds_Wstring dds_wide_to_utf16(const wchar_t* value) {
	return dds_Wstring::Create([=]() { return ::dds_wide_to_utf16(value); });
}

inline dds_WideString dds_utf16_to_wide(const ::dds_Wchar* value) {
	return dds_WideString::Create([=]() { return ::dds_utf16_to_wide(value); });
}

inline dds_String dds_StringTypeSupport_deserialize(void* input, size_t inputSize) {
	return dds_String::Create([=]() { return ::dds_StringTypeSupport_deserialize(input, inputSize); });
}

struct dds_md5_ctx_deleter {
	::dds_ReturnCode_t operator()(::dds_md5_ctx* raw) const noexcept {
		::dds_md5_ctx_free(raw);
		return dds_RETCODE_OK;
	}
};

using dds_md5_ctx = Resource<::dds_md5_ctx, dds_md5_ctx_deleter>;

inline dds_md5_ctx dds_md5_ctx_alloc() {
	return dds_md5_ctx::Create([]() { return static_cast<::dds_md5_ctx*>(::dds_md5_ctx_alloc()); });
}

inline void dds_md5_ctx_free(dds_md5_ctx& context) noexcept {
	context.reset();
}

inline ::dds_ReturnCode_t dds_DynamicType_get_descriptor(::dds_DynamicType* self, dds_TypeDescriptor& value) noexcept {
	if(!self)
		return dds_RETCODE_BAD_PARAMETER;
	return detail::ReplaceScopedValue(
	    value, [=](::dds_TypeDescriptor* next) { return ::dds_DynamicType_get_descriptor(self, next); });
}

inline ::dds_ReturnCode_t dds_DynamicType_get_descriptor(::dds_DynamicType* self, dds_TypeDescriptor* value) noexcept {
	if(!value)
		return dds_RETCODE_BAD_PARAMETER;
	return raii::dds_DynamicType_get_descriptor(self, *value);
}

inline ::dds_ReturnCode_t dds_DynamicTypeMember_get_descriptor(
    ::dds_DynamicTypeMember* self, dds_MemberDescriptor& value) noexcept {
	if(!self)
		return dds_RETCODE_BAD_PARAMETER;
	return detail::ReplaceScopedValue(
	    value, [=](::dds_MemberDescriptor* next) { return ::dds_DynamicTypeMember_get_descriptor(self, next); });
}

inline ::dds_ReturnCode_t dds_DynamicTypeMember_get_descriptor(
    ::dds_DynamicTypeMember* self, dds_MemberDescriptor* value) noexcept {
	if(!value)
		return dds_RETCODE_BAD_PARAMETER;
	return raii::dds_DynamicTypeMember_get_descriptor(self, *value);
}

inline ::dds_ReturnCode_t dds_DynamicData_get_descriptor(
    ::dds_DynamicData* self, dds_MemberDescriptor& value, ::dds_MemberId_t id) noexcept {
	if(!self)
		return dds_RETCODE_BAD_PARAMETER;
	return detail::ReplaceScopedValue(
	    value, [=](::dds_MemberDescriptor* next) { return ::dds_DynamicData_get_descriptor(self, next, id); });
}

inline ::dds_ReturnCode_t dds_DynamicData_get_descriptor(
    ::dds_DynamicData* self, dds_MemberDescriptor* value, ::dds_MemberId_t id) noexcept {
	if(!value)
		return dds_RETCODE_BAD_PARAMETER;
	return raii::dds_DynamicData_get_descriptor(self, *value, id);
}

// Instance handles are values, not owned DDS entities.
inline ::dds_InstanceHandle_t dds_DataWriter_lookup_instance(const dds_DataWriter& self, const void* data) {
	if(!self)
		return dds_HANDLE_NIL;
	return ::dds_DataWriter_lookup_instance(self, data);
}

inline ::dds_InstanceHandle_t dds_DataReader_lookup_instance(const dds_DataReader& self, const void* data) {
	if(!self)
		return dds_HANDLE_NIL;
	return ::dds_DataReader_lookup_instance(self, data);
}

inline ::dds_InstanceHandle_t dds_DynamicDataWriter_lookup_instance(
    const dds_DataWriter& self, const ::dds_DynamicData* data) {
	if(!self)
		return dds_HANDLE_NIL;
	return ::dds_DynamicDataWriter_lookup_instance(self, data);
}

inline ::dds_InstanceHandle_t dds_DynamicDataReader_lookup_instance(
    const dds_DataReader& self, const ::dds_DynamicData* data) {
	if(!self)
		return dds_HANDLE_NIL;
	return ::dds_DynamicDataReader_lookup_instance(self, data);
}

// Unlike entity lookups, the SDK allocates this sequence AND its type supports.
// Return individual move-only owners and release the native sequence here.
inline std::vector<dds_DynamicTypeSupport> dds_DomainParticipant_lookup_delivered_types(
    const dds_DomainParticipant& self, const char* typeName) {
	if(!self || !typeName)
		return {};
	auto cleanup = [](::dds_DataSeq* sequence) {
		if(!sequence)
			return;
		while(::dds_DataSeq_length(sequence) != 0) {
			auto support = static_cast<::dds_DynamicTypeSupport*>(::dds_DataSeq_remove(sequence, 0));
			if(support)
				detail::TerminateOnError(
				    ::dds_DynamicTypeSupport_delete_type_support(support), "delivered type cleanup");
		}
		::dds_DataSeq_delete(sequence);
	};
	std::unique_ptr<::dds_DataSeq, decltype(cleanup)> sequence(
	    ::dds_DomainParticipant_lookup_delivered_types(self, typeName), cleanup);
	std::vector<dds_DynamicTypeSupport> result;
	if(!sequence)
		return result;
	result.reserve(::dds_DataSeq_length(sequence.get()));
	while(::dds_DataSeq_length(sequence.get()) != 0) {
		result.push_back(detail::DynamicDataAccess::CreateSupport(
		    [&]() { return static_cast<::dds_DynamicTypeSupport*>(::dds_DataSeq_remove(sequence.get(), 0)); }));
	}
	return result;
}

} // namespace raii

#endif // RAII_HPP
