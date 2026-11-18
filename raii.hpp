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
#include <exception>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <stdexcept>
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
		return m_sharedptr ? **m_sharedptr : nullptr;
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

// Weak entries never keep entities alive. Native DDS operations still require
// external synchronization, including lookup versus final-owner destruction.
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

template <typename TImpl, typename TRaw, typename... TArgs>
std::shared_ptr<TImpl> MakeRegisteredOwned(TRaw* raw, const TArgs&... args) {
	auto owner = MakeOwned<TImpl>(raw, args...);
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
	template <typename>                                                                                                \
	friend class ::raii::DDSEntity

class DomainParticipant {
	RAII_DDS_IMPL_OPERATORS(DomainParticipant, ::dds_DomainParticipant);

public:
	explicit DomainParticipant(::dds_DomainParticipant* entity) noexcept
	    : m_entity(entity) {
	}

	~DomainParticipant() noexcept {
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
	::dds_DomainParticipant* m_entity{nullptr};
};

class Topic {
	RAII_DDS_IMPL_OPERATORS(Topic, ::dds_Topic);

public:
	Topic(::dds_Topic* entity, std::shared_ptr<DomainParticipant> parent) noexcept
	    : m_parent(std::move(parent))
	    , m_entity(entity) {
	}

	~Topic() noexcept {
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
		return m_kind == Kind::Topic ? dds_Topic(std::static_pointer_cast<Topic>(m_owner)) : dds_Topic{};
	}

	dds_ContentFilteredTopic as_contentfilteredtopic() const noexcept {
		return m_kind == Kind::Filtered
		           ? dds_ContentFilteredTopic(std::static_pointer_cast<ContentFilteredTopic>(m_owner))
		           : dds_ContentFilteredTopic{};
	}

	dds_MultiTopic as_multitopic() const noexcept {
		return m_kind == Kind::Multi ? dds_MultiTopic(std::static_pointer_cast<MultiTopic>(m_owner)) : dds_MultiTopic{};
	}

	const std::shared_ptr<void>& owner() const noexcept {
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
inline dds_TopicDescription RegisteredDescription(::dds_TopicDescription* raw) {
	if(auto owner = RegisteredOwner<Topic>(reinterpret_cast<::dds_Topic*>(raw)))
		return dds_TopicDescription(dds_Topic(owner));
	if(auto owner = RegisteredOwner<ContentFilteredTopic>(reinterpret_cast<::dds_ContentFilteredTopic*>(raw)))
		return dds_TopicDescription(dds_ContentFilteredTopic(owner));
	if(auto owner = RegisteredOwner<MultiTopic>(reinterpret_cast<::dds_MultiTopic*>(raw)))
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
	::dds_ReturnCode_t attach(const TC& condition) {
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
	::dds_ReturnCode_t detach(const TC& condition) {
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

	void clear_attachments() noexcept {
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
	::dds_DomainParticipant* raw =
	    ::dds_DomainParticipantFactory_create_participant(self, domainId, qos, aListener, mask);
	if(raw == nullptr) {
		return {};
	}
	return dds_DomainParticipant(detail::MakeRegisteredOwned<DomainParticipant>(raw));
}

inline ::dds_ReturnCode_t dds_DomainParticipantFactory_delete_participant(
    ::dds_DomainParticipantFactory* self, dds_DomainParticipant& aParticipant) {
	if(detail::SharedImpl(aParticipant).use_count() > 1)
		return dds_RETCODE_PRECONDITION_NOT_MET;
	if(!aParticipant) {
		return detail::BadParameter();
	}
	detail::ClearListener(static_cast<::dds_DomainParticipant*>(aParticipant));
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
	::dds_Publisher* raw =
	    ::dds_DomainParticipant_create_publisher(static_cast<::dds_DomainParticipant*>(self), qos, aListener, mask);
	if(raw == nullptr) {
		return {};
	}
	return dds_Publisher(detail::MakeOwned<Publisher>(raw, detail::SharedImpl(self)));
}

inline ::dds_ReturnCode_t dds_DomainParticipant_delete_publisher(
    const dds_DomainParticipant& self, dds_Publisher& publisher) {
	if(!self || !publisher) {
		return detail::BadParameter();
	}
	if(detail::SharedImpl(publisher).use_count() > 1)
		return dds_RETCODE_PRECONDITION_NOT_MET;
	detail::ClearListener(static_cast<::dds_Publisher*>(publisher));
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
	::dds_Subscriber* raw =
	    ::dds_DomainParticipant_create_subscriber(static_cast<::dds_DomainParticipant*>(self), qos, aListener, mask);
	if(raw == nullptr) {
		return {};
	}
	return dds_Subscriber(detail::MakeOwned<Subscriber>(raw, detail::SharedImpl(self)));
}

inline ::dds_ReturnCode_t dds_DomainParticipant_delete_subscriber(
    const dds_DomainParticipant& self, dds_Subscriber& subscriber) {
	if(!self || !subscriber) {
		return detail::BadParameter();
	}
	if(detail::SharedImpl(subscriber).use_count() > 1)
		return dds_RETCODE_PRECONDITION_NOT_MET;
	detail::ClearListener(static_cast<::dds_Subscriber*>(subscriber));
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
	::dds_Topic* raw = ::dds_DomainParticipant_create_topic(
	    static_cast<::dds_DomainParticipant*>(self), topicName, typeName, qos, aListener, mask);
	if(raw == nullptr) {
		return {};
	}
	return dds_Topic(detail::MakeRegisteredOwned<Topic>(raw, detail::SharedImpl(self)));
}

inline ::dds_ReturnCode_t dds_DomainParticipant_delete_topic(const dds_DomainParticipant& self, dds_Topic& topic) {
	if(!self || !topic) {
		return detail::BadParameter();
	}
	if(detail::SharedImpl(topic).use_count() > 1)
		return dds_RETCODE_PRECONDITION_NOT_MET;
	detail::ClearListener(static_cast<::dds_Topic*>(topic));
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
	::dds_MultiTopic* raw = ::dds_DomainParticipant_create_multitopic(
	    static_cast<::dds_DomainParticipant*>(self), name, typeName, subscriptionExpression, expressionParameters);
	if(raw == nullptr) {
		return {};
	}
	return dds_MultiTopic(detail::MakeRegisteredOwned<MultiTopic>(raw, detail::SharedImpl(self)));
}

inline ::dds_ReturnCode_t dds_DomainParticipant_delete_multitopic(
    const dds_DomainParticipant& self, dds_MultiTopic& topic) {
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
	if(!self)
		return {};
	return dds_DomainParticipant(
	    detail::RegisteredOwner<DomainParticipant>(::dds_DomainParticipantFactory_lookup_participant(self, domainId)));
}

inline dds_Topic dds_DomainParticipant_find_topic(
    const dds_DomainParticipant& self, const char* topicName, const ::dds_Duration_t* timeout) {
	if(!self || !topicName || !timeout)
		return {};
	return dds_Topic(detail::RegisteredOwner<Topic>(::dds_DomainParticipant_find_topic(self, topicName, timeout)));
}

inline dds_TopicDescription dds_DomainParticipant_lookup_topicdescription(
    const dds_DomainParticipant& self, const char* name) {
	if(!self || !name)
		return {};
	return detail::RegisteredDescription(::dds_DomainParticipant_lookup_topicdescription(self, name));
}

inline dds_TopicDescription dds_DomainParticipant_lookup_topicdescription_w_index(
    const dds_DomainParticipant& self, const char* name, ::dds_UnsignedLong index) {
	if(!self || !name)
		return {};
	return detail::RegisteredDescription(::dds_DomainParticipant_lookup_topicdescription_w_index(self, name, index));
}

inline dds_DataWriter dds_Publisher_lookup_datawriter(const dds_Publisher& self, const char* topicName) {
	if(!self || !topicName)
		return {};
	return dds_DataWriter(detail::RegisteredOwner<DataWriter>(::dds_Publisher_lookup_datawriter(self, topicName)));
}

inline dds_DataReader dds_Subscriber_lookup_datareader(const dds_Subscriber& self, const char* topicName) {
	if(!self || !topicName)
		return {};
	return dds_DataReader(detail::RegisteredOwner<DataReader>(::dds_Subscriber_lookup_datareader(self, topicName)));
}

inline dds_DataWriter dds_Publisher_get_contained_datawriter_by_name(const dds_Publisher& self, char* name) {
	if(!self || !name)
		return {};
	return dds_DataWriter(
	    detail::RegisteredOwner<DataWriter>(::dds_Publisher_get_contained_datawriter_by_name(self, name)));
}

inline dds_DataReader dds_Subscriber_get_contained_datareader_by_name(const dds_Subscriber& self, char* name) {
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
	::dds_DataWriter* raw = ::dds_Publisher_create_datawriter_w_profile(
	    static_cast<::dds_Publisher*>(self), static_cast<::dds_Topic*>(aTopic), profileName, aListener, mask);
	if(raw == nullptr) {
		return {};
	}
	return dds_DataWriter(
	    detail::MakeRegisteredOwned<DataWriter>(raw, detail::SharedImpl(self), detail::SharedImpl(aTopic)));
}

// Raw topic pointers are borrowed. The caller must keep the topic alive until
// the endpoint is deleted; only its publisher/subscriber is retained.
inline dds_DataWriter dds_Publisher_create_datawriter(const dds_Publisher& self,
    const ::dds_Topic* aTopic,
    const ::dds_DataWriterQos* qos,
    const ::dds_DataWriterListener* aListener,
    ::dds_StatusMask mask) {
	::dds_DataWriter* raw =
	    ::dds_Publisher_create_datawriter(static_cast<::dds_Publisher*>(self), aTopic, qos, aListener, mask);
	if(raw == nullptr) {
		return {};
	}
	return dds_DataWriter(
	    detail::MakeRegisteredOwned<DataWriter>(raw, detail::SharedImpl(self), std::shared_ptr<Topic>{}));
}

inline dds_DataWriter dds_Publisher_create_datawriter_w_props(const dds_Publisher& self,
    const ::dds_Topic* aTopic,
    const ::dds_DataWriterQos* qos,
    const ::dds_DataWriterListener* aListener,
    ::dds_StatusMask mask,
    ::dds_StringProperty* props) {
	::dds_DataWriter* raw = ::dds_Publisher_create_datawriter_w_props(
	    static_cast<::dds_Publisher*>(self), aTopic, qos, aListener, mask, props);
	if(raw == nullptr) {
		return {};
	}
	return dds_DataWriter(
	    detail::MakeRegisteredOwned<DataWriter>(raw, detail::SharedImpl(self), std::shared_ptr<Topic>{}));
}

inline dds_DataWriter dds_Publisher_create_datawriter_w_profile(const dds_Publisher& self,
    const ::dds_Topic* aTopic,
    const char* profileName,
    const ::dds_DataWriterListener* aListener,
    ::dds_StatusMask mask) {
	::dds_DataWriter* raw = ::dds_Publisher_create_datawriter_w_profile(
	    static_cast<::dds_Publisher*>(self), aTopic, profileName, aListener, mask);
	if(raw == nullptr) {
		return {};
	}
	return dds_DataWriter(
	    detail::MakeRegisteredOwned<DataWriter>(raw, detail::SharedImpl(self), std::shared_ptr<Topic>{}));
}

inline ::dds_ReturnCode_t dds_Publisher_delete_datawriter(const dds_Publisher& self, dds_DataWriter& aDatawriter) {
	if(!self || !aDatawriter) {
		return detail::BadParameter();
	}
	if(detail::SharedImpl(aDatawriter).use_count() > 1)
		return dds_RETCODE_PRECONDITION_NOT_MET;
	detail::ClearListener(static_cast<::dds_DataWriter*>(aDatawriter));
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
	::dds_DataReader* raw = ::dds_Subscriber_create_datareader(self, aTopic.get(), qos, aListener, mask);
	if(!raw)
		return {};
	return dds_DataReader(detail::MakeRegisteredOwned<DataReader>(raw, detail::SharedImpl(self), aTopic.owner()));
}

// Raw topic descriptions are borrowed; no C++ topic owner is retained.
inline dds_DataReader dds_Subscriber_create_datareader(const dds_Subscriber& self,
    const ::dds_TopicDescription* aTopic,
    const ::dds_DataReaderQos* qos,
    const ::dds_DataReaderListener* aListener,
    ::dds_StatusMask mask) {
	::dds_DataReader* raw =
	    ::dds_Subscriber_create_datareader(static_cast<::dds_Subscriber*>(self), aTopic, qos, aListener, mask);
	if(raw == nullptr) {
		return {};
	}
	return dds_DataReader(
	    detail::MakeRegisteredOwned<DataReader>(raw, detail::SharedImpl(self), std::shared_ptr<void>{}));
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
	::dds_DataReader* raw = ::dds_Subscriber_create_datareader_w_profile(
	    static_cast<::dds_Subscriber*>(self), aTopic, profileName, aListener, mask);
	if(raw == nullptr) {
		return {};
	}
	return dds_DataReader(
	    detail::MakeRegisteredOwned<DataReader>(raw, detail::SharedImpl(self), std::shared_ptr<void>{}));
}

inline ::dds_ReturnCode_t dds_Subscriber_delete_datareader(const dds_Subscriber& self, dds_DataReader& aDatareader) {
	if(!self || !aDatareader) {
		return detail::BadParameter();
	}
	if(detail::SharedImpl(aDatareader).use_count() > 1)
		return dds_RETCODE_PRECONDITION_NOT_MET;
	detail::ClearListener(static_cast<::dds_DataReader*>(aDatareader));
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
	::dds_ReturnCode_t ret = ::dds_WaitSet_delete(self);
	if(ret == dds_RETCODE_OK) {
		*detail::SharedImpl(self) = nullptr;
		detail::SharedImpl(self)->clear_attachments();
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
	return detail::SharedImpl(self)->attach(cond);
}

inline ::dds_ReturnCode_t dds_WaitSet_attach_condition(const dds_WaitSet& self, const dds_ReadCondition& cond) {
	if(!self || !cond) {
		return detail::BadParameter();
	}
	return detail::SharedImpl(self)->attach(cond);
}

inline ::dds_ReturnCode_t dds_WaitSet_attach_condition(const dds_WaitSet& self, const dds_QueryCondition& cond) {
	if(!self || !cond) {
		return detail::BadParameter();
	}
	return detail::SharedImpl(self)->attach(cond);
}

inline ::dds_ReturnCode_t dds_WaitSet_detach_condition(const dds_WaitSet& self, const dds_GuardCondition& cond) {
	if(!self || !cond) {
		return detail::BadParameter();
	}
	return detail::SharedImpl(self)->detach(cond);
}

inline ::dds_ReturnCode_t dds_WaitSet_detach_condition(const dds_WaitSet& self, const dds_ReadCondition& cond) {
	if(!self || !cond) {
		return detail::BadParameter();
	}
	return detail::SharedImpl(self)->detach(cond);
}

inline ::dds_ReturnCode_t dds_WaitSet_detach_condition(const dds_WaitSet& self, const dds_QueryCondition& cond) {
	if(!self || !cond) {
		return detail::BadParameter();
	}
	return detail::SharedImpl(self)->detach(cond);
}

// Move-only owners for sequences, TypeSupport, samples, and dynamic types.
// Deleters return the native return code. Destructor failures terminate, matching
// the entity wrappers; use close()/the explicit delete wrapper to inspect errors.
template <typename TValue, typename TDeleter>
class Resource {
	struct State {
		TValue* m_raw = nullptr;
		TDeleter m_deleter;
		std::function<::dds_ReturnCode_t()> m_beforeDelete;
		const void* m_loanIdentity = nullptr;
		const void* m_loanReader = nullptr;

		explicit State(TDeleter d)
		    : m_deleter(std::move(d)) {
		}

		~State() noexcept {
			if(m_raw) {
				if(m_beforeDelete)
					detail::TerminateOnError(m_beforeDelete(), "return sequence loan");
				detail::TerminateOnError(m_deleter(m_raw), "RAII resource cleanup");
			}
		}
	};

	std::shared_ptr<State> m_state;

public:
	using native_type = TValue;
	Resource() noexcept = default;
	Resource(const Resource&) = delete;
	Resource& operator=(const Resource&) = delete;
	Resource(Resource&&) noexcept = default;
	Resource& operator=(Resource&&) noexcept = default;

	// Adopts an owning C pointer, never a borrowed get/lookup result.
	Resource(TValue* raw, TDeleter deleter = TDeleter()) {
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
		return m_state ? m_state->m_raw : nullptr;
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

	std::shared_ptr<void> owner() const noexcept {
		return m_state;
	}

	const TDeleter* deleter() const noexcept {
		return m_state ? &m_state->m_deleter : nullptr;
	}

	void reset() noexcept {
		m_state.reset();
	}

	::dds_ReturnCode_t finish_loan() noexcept {
		if(!m_state || !m_state->m_beforeDelete)
			return dds_RETCODE_OK;
		const auto ret = m_state->m_beforeDelete();
		if(ret == dds_RETCODE_OK) {
			m_state->m_beforeDelete = {};
			m_state->m_loanIdentity = nullptr;
			m_state->m_loanReader = nullptr;
		}
		return ret;
	}

	::dds_ReturnCode_t prepare_loan(
	    std::function<::dds_ReturnCode_t()> cleanup, const void* identity = nullptr, const void* reader = nullptr) {
		if(!get())
			return dds_RETCODE_BAD_PARAMETER;
		const auto ret = finish_loan();
		if(ret != dds_RETCODE_OK)
			return ret;
		m_state->m_beforeDelete = std::move(cleanup);
		m_state->m_loanIdentity = identity;
		m_state->m_loanReader = reader;
		return dds_RETCODE_OK;
	}

	const void* loan_identity() const noexcept {
		return m_state ? m_state->m_loanIdentity : nullptr;
	}

	const void* loan_reader() const noexcept {
		return m_state ? m_state->m_loanReader : nullptr;
	}

	TValue* release() {
		if(!m_state)
			return nullptr;
		if(m_state->m_beforeDelete)
			throw std::logic_error("Return the sequence loan before release()");
		if(m_state.use_count() != 1)
			throw std::logic_error("DDS resource is still in use");
		TValue* raw = m_state->m_raw;
		m_state->m_raw = nullptr;
		m_state.reset();
		return raw;
	}

	::dds_ReturnCode_t close() noexcept {
		if(!get())
			return dds_RETCODE_OK;
		if(m_state.use_count() != 1)
			return dds_RETCODE_PRECONDITION_NOT_MET;
		const auto loanRet = finish_loan();
		if(loanRet != dds_RETCODE_OK)
			return loanRet;
		const auto ret = m_state->m_deleter(m_state->m_raw);
		if(ret == dds_RETCODE_OK) {
			m_state->m_raw = nullptr;
			m_state.reset();
		}
		return ret;
	}

	template <typename TAllocate>
	static Resource create(TAllocate allocate, TDeleter deleter = TDeleter()) {
		Resource result;
		result.m_state = std::make_shared<State>(std::move(deleter));
		result.m_state->m_raw = allocate(); // No throwing C++ work after allocation.
		if(!result.m_state->m_raw)
			result.m_state.reset();
		return result;
	}
};

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
		return Type::create([]() { return ::TypeSupport##_alloc(); });                                                 \
	}                                                                                                                  \
	inline void TypeSupport##_free(Type& sample) noexcept {                                                            \
		sample.reset();                                                                                                \
	}                                                                                                                  \
	inline Type TypeSupport##_deserialize(void* input, size_t input_size) {                                            \
		return Type::create([&]() { return ::TypeSupport##_deserialize(input, input_size); });                         \
	}                                                                                                                  \
	}

#define RAII_SEQUENCE_DEFINE(seq_type)                                                                                 \
	struct seq_type##_deleter {                                                                                        \
		::dds_ReturnCode_t operator()(::seq_type* raw) const noexcept {                                                \
			::seq_type##_delete(raw);                                                                                  \
			return dds_RETCODE_OK;                                                                                     \
		}                                                                                                              \
	};                                                                                                                 \
	using seq_type = Resource<::seq_type, seq_type##_deleter>;                                                         \
	inline seq_type seq_type##_create(uint32_t capacity) {                                                             \
		return seq_type::create([=]() { return ::seq_type##_create(capacity); });                                      \
	}                                                                                                                  \
	inline seq_type seq_type##_clone(::seq_type* sequence) {                                                           \
		return seq_type::create([=]() { return ::seq_type##_clone(sequence); });                                       \
	}                                                                                                                  \
	inline void seq_type##_delete(seq_type& sequence) noexcept {                                                       \
		sequence.reset();                                                                                              \
	}

RAII_SEQUENCE_DEFINE(dds_InstanceHandleSeq)
RAII_SEQUENCE_DEFINE(dds_QosPolicyCountSeq)
RAII_SEQUENCE_DEFINE(dds_DataReaderSeq)
RAII_SEQUENCE_DEFINE(dds_DataWriterSeq)
RAII_SEQUENCE_DEFINE(dds_ConditionSeq)
RAII_SEQUENCE_DEFINE(dds_SampleInfoSeq)
RAII_SEQUENCE_DEFINE(dds_DataSeq)
RAII_SEQUENCE_DEFINE(dds_DataRepresentationIdSeq)
RAII_SEQUENCE_DEFINE(dds_DynamicDataSeq)
RAII_SEQUENCE_DEFINE(dds_PropertySeq)
RAII_SEQUENCE_DEFINE(dds_TagSeq)
RAII_SEQUENCE_DEFINE(dds_BinaryPropertySeq)
RAII_SEQUENCE_DEFINE(dds_CharSeq)
RAII_SEQUENCE_DEFINE(dds_StringSeq)
RAII_SEQUENCE_DEFINE(dds_WcharSeq)
RAII_SEQUENCE_DEFINE(dds_WstringSeq)
RAII_SEQUENCE_DEFINE(dds_OctetSeq)
RAII_SEQUENCE_DEFINE(dds_ShortSeq)
RAII_SEQUENCE_DEFINE(dds_UnsignedShortSeq)
RAII_SEQUENCE_DEFINE(dds_LongSeq)
RAII_SEQUENCE_DEFINE(dds_UnsignedLongSeq)
RAII_SEQUENCE_DEFINE(dds_LongLongSeq)
RAII_SEQUENCE_DEFINE(dds_UnsignedLongLongSeq)
RAII_SEQUENCE_DEFINE(dds_FloatSeq)
RAII_SEQUENCE_DEFINE(dds_DoubleSeq)
RAII_SEQUENCE_DEFINE(dds_BooleanSeq)
RAII_SEQUENCE_DEFINE(dds_KeyedStringSeq)
RAII_SEQUENCE_DEFINE(dds_BytesSeq)
RAII_SEQUENCE_DEFINE(dds_KeyedBytesSeq)
#undef RAII_SEQUENCE_DEFINE

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
	inline ::dds_ReturnCode_t qos_type##_copy(qos_type& dst, const qos_type& src) noexcept {                           \
		if(&dst == &src) {                                                                                             \
			return dds_RETCODE_OK;                                                                                     \
		}                                                                                                              \
		(void)::qos_type##_finalize(dst);                                                                              \
		return ::qos_type##_copy(dst, src);                                                                            \
	}                                                                                                                  \
                                                                                                                       \
	inline ::dds_ReturnCode_t qos_type##_copy(qos_type& dst, const typename qos_type::native_type* src) noexcept {     \
		if(src == nullptr) {                                                                                           \
			return dds_RETCODE_BAD_PARAMETER;                                                                          \
		}                                                                                                              \
                                                                                                                       \
		const typename qos_type::native_type* dst_ptr = dst;                                                           \
		if(src == dst_ptr) {                                                                                           \
			return dds_RETCODE_OK;                                                                                     \
		}                                                                                                              \
                                                                                                                       \
		(void)::qos_type##_finalize(dst);                                                                              \
		return ::qos_type##_copy(dst, src);                                                                            \
	}

// RAII Qos 매크로 확장
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
		(void)::qos_type##_finalize(qos);                                                                              \
		return ::c_api_name(self, qos);                                                                                \
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

// The C copy_from_topic_qos replaces only shared policies and frees the old
// representation sequence itself; retain endpoint-only QoS policies.
inline ::dds_ReturnCode_t dds_Publisher_copy_from_topic_qos(
    ::dds_Publisher* self, dds_DataWriterQos& datawriterQos, const dds_TopicQos& topicQos) noexcept {
	return ::dds_Publisher_copy_from_topic_qos(self, datawriterQos, topicQos);
}

inline ::dds_ReturnCode_t dds_Publisher_copy_from_topic_qos(
    ::dds_Publisher* self, dds_DataWriterQos& datawriterQos, const ::dds_TopicQos* topicQos) noexcept {
	if(topicQos == nullptr) {
		return dds_RETCODE_BAD_PARAMETER;
	}

	return ::dds_Publisher_copy_from_topic_qos(self, datawriterQos, topicQos);
}

inline ::dds_ReturnCode_t dds_Subscriber_copy_from_topic_qos(
    ::dds_Subscriber* self, dds_DataReaderQos& datareaderQos, const dds_TopicQos& topicQos) noexcept {
	return ::dds_Subscriber_copy_from_topic_qos(self, datareaderQos, topicQos);
}

inline ::dds_ReturnCode_t dds_Subscriber_copy_from_topic_qos(
    ::dds_Subscriber* self, dds_DataReaderQos& datareaderQos, const ::dds_TopicQos* topicQos) noexcept {
	if(topicQos == nullptr) {
		return dds_RETCODE_BAD_PARAMETER;
	}

	return ::dds_Subscriber_copy_from_topic_qos(self, datareaderQos, topicQos);
}

inline ::dds_ReturnCode_t dds_DomainParticipantFactory_get_participant_qos_from_profile(
    const char* profileName, dds_DomainParticipantQos& qos) noexcept {
	(void)::dds_DomainParticipantQos_finalize(qos);
	return ::dds_DomainParticipantFactory_get_participant_qos_from_profile(profileName, qos);
}

inline ::dds_ReturnCode_t dds_DomainParticipantFactory_get_datawriter_qos_from_profile(
    const char* profileName, dds_DataWriterQos& qos) noexcept {
	(void)::dds_DataWriterQos_finalize(qos);
	return ::dds_DomainParticipantFactory_get_datawriter_qos_from_profile(profileName, qos);
}

inline ::dds_ReturnCode_t dds_DomainParticipantFactory_get_datareader_qos_from_profile(
    const char* profileName, dds_DataReaderQos& qos) noexcept {
	(void)::dds_DataReaderQos_finalize(qos);
	return ::dds_DomainParticipantFactory_get_datareader_qos_from_profile(profileName, qos);
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
		(void)::type##_finalize(dst);                                                                                  \
		return ::type##_copy_from(dst, src);                                                                           \
	}                                                                                                                  \
	inline ::dds_ReturnCode_t type##_copy_from(type& dst, const type& src) noexcept {                                  \
		return raii::type##_copy_from(dst, static_cast<const ::type*>(src));                                           \
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
	::dds_DynamicTypeBuilderFactory* factory = nullptr;

	explicit dds_DynamicType_deleter(::dds_DynamicTypeBuilderFactory* f = nullptr)
	    : factory(f) {
	}

	::dds_ReturnCode_t operator()(::dds_DynamicType* raw) const noexcept {
		return ::dds_DynamicTypeBuilderFactory_delete_type(
		    factory ? factory : ::dds_DynamicTypeBuilderFactory_get_instance(), raw);
	}
};

using dds_DynamicType = Resource<::dds_DynamicType, dds_DynamicType_deleter>;

inline ::dds_ReturnCode_t dds_DynamicTypeBuilderFactory_delete_type(
    ::dds_DynamicTypeBuilderFactory* self, dds_DynamicType& type) noexcept {
	if(!type)
		return dds_RETCODE_OK;
	if(type.owner().use_count() > 2)
		return dds_RETCODE_PRECONDITION_NOT_MET;
	const auto ret = ::dds_DynamicTypeBuilderFactory_delete_type(self, type.get());
	if(ret == dds_RETCODE_OK)
		(void)type.release();
	return ret;
}

struct dds_DynamicData_deleter {
	::dds_DynamicDataFactory* factory = nullptr;
	::dds_DynamicData* parent = nullptr;
	std::shared_ptr<void> parent_owner;
	std::shared_ptr<void> type_owner;
	dds_DynamicData_deleter() = default;

	explicit dds_DynamicData_deleter(::dds_DynamicDataFactory* f)
	    : factory(f) {
	}

	::dds_ReturnCode_t operator()(::dds_DynamicData* raw) const noexcept {
		return parent ? ::dds_DynamicData_return_loaned_value(parent, raw)
		              : ::dds_DynamicDataFactory_delete_data(
		                    factory ? factory : ::dds_DynamicDataFactory_get_instance(), raw);
	}
};

using dds_DynamicData = Resource<::dds_DynamicData, dds_DynamicData_deleter>;

inline ::dds_ReturnCode_t dds_DynamicDataFactory_delete_data(
    ::dds_DynamicDataFactory* self, dds_DynamicData& data) noexcept {
	if(!data)
		return dds_RETCODE_BAD_PARAMETER;
	if(data.deleter()->parent)
		return dds_RETCODE_PRECONDITION_NOT_MET;
	if(data.owner().use_count() > 2)
		return dds_RETCODE_PRECONDITION_NOT_MET;
	const auto ret = ::dds_DynamicDataFactory_delete_data(self, data.get());
	if(ret == dds_RETCODE_OK)
		(void)data.release();
	return ret;
}

inline dds_DynamicData dds_DynamicData_loan_value(const dds_DynamicData& self, ::dds_MemberId_t id) {
	dds_DynamicData_deleter cleanup;
	cleanup.parent = self.get();
	cleanup.parent_owner = self.owner();
	return dds_DynamicData::create([&]() { return ::dds_DynamicData_loan_value(self, id); }, cleanup);
}

inline ::dds_ReturnCode_t dds_DynamicData_return_loaned_value(
    const dds_DynamicData& self, dds_DynamicData& value) noexcept {
	if(!self || !value)
		return dds_RETCODE_BAD_PARAMETER;
	if(value.deleter()->parent != self.get())
		return dds_RETCODE_PRECONDITION_NOT_MET;
	if(value.owner().use_count() > 2)
		return dds_RETCODE_PRECONDITION_NOT_MET;
	const auto ret = ::dds_DynamicData_return_loaned_value(self, value.get());
	if(ret == dds_RETCODE_OK)
		(void)value.release();
	return ret;
}

struct dds_Memory_deleter {
	::dds_ReturnCode_t operator()(void* raw) const noexcept {
		::dds_free(raw);
		return dds_RETCODE_OK;
	}
};

using dds_Memory = Resource<void, dds_Memory_deleter>;
using dds_String = Resource<char, dds_Memory_deleter>;

inline void dds_free(dds_Memory& value) noexcept {
	value.reset();
}

inline void dds_free(dds_String& value) noexcept {
	value.reset();
}

inline dds_Memory dds_malloc(size_t size) {
	return dds_Memory::create([=]() { return ::dds_malloc(size); });
}

inline dds_Memory dds_calloc(size_t count, size_t size) {
	return dds_Memory::create([=]() { return ::dds_calloc(count, size); });
}

inline dds_String dds_strdup(const char* str) {
	return dds_String::create([=]() { return ::dds_strdup(str); });
}

inline dds_DynamicType dds_DynamicTypeBuilder_build(::dds_DynamicTypeBuilder* self) {
	return dds_DynamicType::create([&]() { return ::dds_DynamicTypeBuilder_build(self); });
}

inline dds_DynamicTypeMember dds_DynamicTypeMember_alloc() {
	return dds_DynamicTypeMember::create([&]() { return ::dds_DynamicTypeMember_alloc(); });
}

inline dds_DynamicTypeSupport dds_DynamicTypeSupport_create_type_support(::dds_DynamicType* type) {
	return dds_DynamicTypeSupport::create([&]() { return ::dds_DynamicTypeSupport_create_type_support(type); });
}

inline dds_DynamicData dds_DynamicDataFactory_create_data(
    ::dds_DynamicDataFactory* self, const ::dds_DynamicType* type) {
	return dds_DynamicData::create(
	    [&]() { return ::dds_DynamicDataFactory_create_data(self, type); }, dds_DynamicData_deleter(self));
}

inline dds_DynamicData dds_DynamicData_clone(::dds_DynamicData* self) {
	return dds_DynamicData::create([&]() { return ::dds_DynamicData_clone(self); });
}

// The C DynamicData implementation borrows its type, including in clones.
inline dds_DynamicData dds_DynamicDataFactory_create_data(::dds_DynamicDataFactory* self, const dds_DynamicType& type) {
	dds_DynamicData_deleter cleanup(self);
	cleanup.type_owner = type.owner();

	return dds_DynamicData::create([&]() { return ::dds_DynamicDataFactory_create_data(self, type); }, cleanup);
}

inline dds_DynamicData dds_DynamicData_clone(const dds_DynamicData& self) {
	dds_DynamicData_deleter cleanup;
	cleanup.type_owner = self.owner();
	return dds_DynamicData::create([&]() { return ::dds_DynamicData_clone(self); }, cleanup);
}

inline dds_TypeSupport dds_TypeSupport_create(const char* metaStr) {
	return dds_TypeSupport::create([&]() { return ::dds_TypeSupport_create(metaStr); });
}

inline dds_TypeSupport dds_TypeSupport_create2(const char** metaStrArray, uint32_t metaStrArrayLen) {
	return dds_TypeSupport::create([&]() { return ::dds_TypeSupport_create2(metaStrArray, metaStrArrayLen); });
}

inline dds_TypeSupport dds_TypeSupport_early_create(const char* metaStr) {
	return dds_TypeSupport::create([&]() { return ::dds_TypeSupport_early_create(metaStr); });
}

inline dds_TypeSupport dds_TypeSupport_early_create2(const char** metaStrArray, uint32_t metaStrArrayLen) {
	return dds_TypeSupport::create([&]() { return ::dds_TypeSupport_early_create2(metaStrArray, metaStrArrayLen); });
}

inline dds_Memory dds_TypeSupport_serialize(::dds_TypeSupport* self, void* data, size_t* outputSize) {
	return dds_Memory::create([&]() { return ::dds_TypeSupport_serialize(self, data, outputSize); });
}

inline dds_Memory dds_TypeSupport_serialize_w_repr_id(
    ::dds_TypeSupport* self, void* data, size_t* outputSize, dds_DataRepresentationId_t reprId) {
	return dds_Memory::create([&]() { return ::dds_TypeSupport_serialize_w_repr_id(self, data, outputSize, reprId); });
}

inline dds_Memory dds_StringTypeSupport_serialize(::dds_String data, size_t* outputSize) {
	return dds_Memory::create([&]() { return ::dds_StringTypeSupport_serialize(data, outputSize); });
}

inline dds_KeyedString dds_KeyedStringTypeSupport_alloc() {
	return dds_KeyedString::create([&]() { return ::dds_KeyedStringTypeSupport_alloc(); });
}

inline dds_Memory dds_KeyedStringTypeSupport_serialize(::dds_KeyedString* data, size_t* outputSize) {
	return dds_Memory::create([&]() { return ::dds_KeyedStringTypeSupport_serialize(data, outputSize); });
}

inline dds_KeyedString dds_KeyedStringTypeSupport_deserialize(void* input, size_t inputSize) {
	return dds_KeyedString::create([&]() { return ::dds_KeyedStringTypeSupport_deserialize(input, inputSize); });
}

inline dds_Bytes dds_BytesTypeSupport_alloc() {
	return dds_Bytes::create([&]() { return ::dds_BytesTypeSupport_alloc(); });
}

inline dds_Memory dds_BytesTypeSupport_serialize(::dds_Bytes* data, size_t* outputSize) {
	return dds_Memory::create([&]() { return ::dds_BytesTypeSupport_serialize(data, outputSize); });
}

inline dds_Bytes dds_BytesTypeSupport_deserialize(void* input, size_t inputSize) {
	return dds_Bytes::create([&]() { return ::dds_BytesTypeSupport_deserialize(input, inputSize); });
}

inline dds_KeyedBytes dds_KeyedBytesTypeSupport_alloc() {
	return dds_KeyedBytes::create([&]() { return ::dds_KeyedBytesTypeSupport_alloc(); });
}

inline dds_Memory dds_KeyedBytesTypeSupport_serialize(::dds_KeyedBytes* data, size_t* outputSize) {
	return dds_Memory::create([&]() { return ::dds_KeyedBytesTypeSupport_serialize(data, outputSize); });
}

inline dds_KeyedBytes dds_KeyedBytesTypeSupport_deserialize(void* input, size_t inputSize) {
	return dds_KeyedBytes::create([&]() { return ::dds_KeyedBytesTypeSupport_deserialize(input, inputSize); });
}

inline dds_String dds_DomainParticipantFactory_dump_as_string(::dds_DomainParticipantFactory* self) {
	return dds_String::create([&]() { return ::dds_DomainParticipantFactory_dump_as_string(self); });
}

inline dds_TypeSupport dds_TypeSupport_raw_create() {
	return dds_TypeSupport::create([&]() { return ::dds_TypeSupport_raw_create(); });
}

inline dds_TypeSupport dds_TypeSupport_raw_create_w_key() {
	return dds_TypeSupport::create([&]() { return ::dds_TypeSupport_raw_create_w_key(); });
}

inline dds_TypeSupport dds_TypeSupport_raw_create_w_typehash(void* typehash, bool hasKey) {
	return dds_TypeSupport::create([&]() { return ::dds_TypeSupport_raw_create_w_typehash(typehash, hasKey); });
}

inline dds_Memory dds_TypeSupport_serialize_be(::dds_TypeSupport* self, void* data, size_t* outputSize) {
	return dds_Memory::create([&]() { return ::dds_TypeSupport_serialize_be(self, data, outputSize); });
}

inline dds_Memory dds_TypeSupport_serialize_le(::dds_TypeSupport* self, void* data, size_t* outputSize) {
	return dds_Memory::create([&]() { return ::dds_TypeSupport_serialize_le(self, data, outputSize); });
}

inline dds_String dds_TypeSupport_extract_metastring(::dds_TypeSupport* self) {
	return dds_String::create([&]() { return ::dds_TypeSupport_extract_metastring(self); });
}

inline dds_DynamicTypeSupport dds_DynamicTypeSupport_create_type_support_with_metastring(char* metastring) {
	return dds_DynamicTypeSupport::create(
	    [&]() { return ::dds_DynamicTypeSupport_create_type_support_with_metastring(metastring); });
}

inline dds_String dds_DomainParticipantQos_dump_as_string(::dds_DomainParticipantQos* qos) {
	return dds_String::create([&]() { return ::dds_DomainParticipantQos_dump_as_string(qos); });
}

inline dds_String dds_TopicQos_dump_as_string(::dds_TopicQos* qos) {
	return dds_String::create([&]() { return ::dds_TopicQos_dump_as_string(qos); });
}

inline dds_String dds_PublisherQos_dump_as_string(::dds_PublisherQos* qos) {
	return dds_String::create([&]() { return ::dds_PublisherQos_dump_as_string(qos); });
}

inline dds_String dds_SubscriberQos_dump_as_string(::dds_SubscriberQos* qos) {
	return dds_String::create([&]() { return ::dds_SubscriberQos_dump_as_string(qos); });
}

inline dds_String dds_DataWriterQos_dump_as_string(::dds_DataWriterQos* qos) {
	return dds_String::create([&]() { return ::dds_DataWriterQos_dump_as_string(qos); });
}

inline dds_String dds_DataReaderQos_dump_as_string(::dds_DataReaderQos* qos) {
	return dds_String::create([&]() { return ::dds_DataReaderQos_dump_as_string(qos); });
}

struct dds_TypeSupportData_deleter {
	::dds_TypeSupport* support = nullptr;
	bool initialized = true;
	std::shared_ptr<void> support_owner;

	::dds_ReturnCode_t operator()(void* raw) const noexcept {
		if(initialized)
			::dds_TypeSupport_delete_data(support, raw);
		else
			::dds_TypeSupport_free(support, raw);
		return dds_RETCODE_OK;
	}
};

using dds_TypeSupportData = Resource<void, dds_TypeSupportData_deleter>;

inline dds_TypeSupportData dds_TypeSupport_create_data(const dds_TypeSupport& self) {
	dds_TypeSupportData_deleter cleanup;
	cleanup.support = self.get();
	cleanup.support_owner = self.owner();
	cleanup.initialized = true;
	return dds_TypeSupportData::create([&]() { return ::dds_TypeSupport_create_data(self); }, cleanup);
}

inline dds_TypeSupportData dds_TypeSupport_alloc(const dds_TypeSupport& self) {
	dds_TypeSupportData_deleter cleanup;
	cleanup.support = self.get();
	cleanup.support_owner = self.owner();
	cleanup.initialized = false;
	return dds_TypeSupportData::create([&]() { return ::dds_TypeSupport_alloc(self); }, cleanup);
}

inline dds_TypeSupportData dds_TypeSupport_clone_data(const dds_TypeSupport& self, void* data) {
	dds_TypeSupportData_deleter cleanup;
	cleanup.support = self.get();
	cleanup.support_owner = self.owner();
	cleanup.initialized = true;
	return dds_TypeSupportData::create([&]() { return ::dds_TypeSupport_clone_data(self, data); }, cleanup);
}

inline dds_TypeSupportData dds_TypeSupport_deserialize(const dds_TypeSupport& self, void* input, size_t inputSize) {
	dds_TypeSupportData_deleter cleanup;
	cleanup.support = self.get();
	cleanup.support_owner = self.owner();
	cleanup.initialized = true;
	return dds_TypeSupportData::create(
	    [&]() { return ::dds_TypeSupport_deserialize(self, input, inputSize); }, cleanup);
}

inline void dds_TypeSupport_delete_data(const dds_TypeSupport& self, dds_TypeSupportData& data) {
	if(!data)
		return;
	if(data.deleter()->support != self.get())
		throw std::invalid_argument("TypeSupport does not own this sample");
	if(data.owner().use_count() > 2)
		throw std::logic_error("DDS sample is still in use");
	::dds_TypeSupport_delete_data(self, data.get());
	(void)data.release();
}

inline void dds_TypeSupport_free(const dds_TypeSupport& self, dds_TypeSupportData& data) {
	if(!data)
		return;
	if(data.deleter()->support != self.get())
		throw std::invalid_argument("TypeSupport does not own this sample");
	if(data.owner().use_count() > 2)
		throw std::logic_error("DDS sample is still in use");
	::dds_TypeSupport_free(self, data.get());
	(void)data.release();
}

namespace detail {
// Shared by the participating sequences, without owning those sequences. The
// first sequence destruction/reuse returns the entire loan before any C sequence
// is deleted; later cleanup calls are no-ops. Retains its reader while active.
struct SequenceLoan {
	std::shared_ptr<DataReader> reader;
	::dds_DataSeq* data = nullptr;
	::dds_DynamicDataSeq* dynamic_data = nullptr;
	::dds_SampleInfoSeq* infos = nullptr;
	::dds_UnsignedLongSeq* sizes = nullptr;
	bool active = false;

	::dds_ReturnCode_t finish() noexcept {
		if(!active)
			return dds_RETCODE_OK;
		::dds_ReturnCode_t ret;
		if(sizes)
			ret = ::dds_DataReader_raw_return_loan(**reader, data, infos, sizes);
		else if(dynamic_data)
			ret = ::dds_DynamicDataReader_return_loan(**reader, dynamic_data, infos);
		else
			ret = ::dds_DataReader_return_loan(**reader, data, infos);
		if(ret == dds_RETCODE_OK) {
			active = false;
			reader.reset();
		}
		return ret;
	}
};
} // namespace detail

inline ::dds_ReturnCode_t dds_DataReader_read(const dds_DataReader& self,
    dds_DataSeq& dataValues,
    dds_SampleInfoSeq& sampleInfos,
    int32_t maxSamples,
    ::dds_SampleStateMask sampleStates,
    ::dds_ViewStateMask viewStates,
    ::dds_InstanceStateMask instanceStates) {
	if(!self || !dataValues || !sampleInfos)
		return dds_RETCODE_BAD_PARAMETER;
	auto loan = std::make_shared<detail::SequenceLoan>();
	loan->reader = detail::SharedImpl(self);
	loan->data = dataValues;
	loan->infos = sampleInfos;
	std::function<::dds_ReturnCode_t()> cleanup = [loan]() { return loan->finish(); };
	auto ret = dataValues.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = sampleInfos.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = ::dds_DataReader_read(self, dataValues, sampleInfos, maxSamples, sampleStates, viewStates, instanceStates);
	loan->active = (ret == dds_RETCODE_OK);
	if(!loan->active)
		loan->reader.reset();
	return ret;
}

inline ::dds_ReturnCode_t dds_DataReader_take(const dds_DataReader& self,
    dds_DataSeq& dataValues,
    dds_SampleInfoSeq& sampleInfos,
    int32_t maxSamples,
    ::dds_SampleStateMask sampleStates,
    ::dds_ViewStateMask viewStates,
    ::dds_InstanceStateMask instanceStates) {
	if(!self || !dataValues || !sampleInfos)
		return dds_RETCODE_BAD_PARAMETER;
	auto loan = std::make_shared<detail::SequenceLoan>();
	loan->reader = detail::SharedImpl(self);
	loan->data = dataValues;
	loan->infos = sampleInfos;
	std::function<::dds_ReturnCode_t()> cleanup = [loan]() { return loan->finish(); };
	auto ret = dataValues.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = sampleInfos.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = ::dds_DataReader_take(self, dataValues, sampleInfos, maxSamples, sampleStates, viewStates, instanceStates);
	loan->active = (ret == dds_RETCODE_OK);
	if(!loan->active)
		loan->reader.reset();
	return ret;
}

inline ::dds_ReturnCode_t dds_DataReader_read_w_condition(const dds_DataReader& self,
    dds_DataSeq& dataValues,
    dds_SampleInfoSeq& sampleInfos,
    int32_t maxSamples,
    const ::dds_ReadCondition* aCondition) {
	if(!self || !dataValues || !sampleInfos)
		return dds_RETCODE_BAD_PARAMETER;
	auto loan = std::make_shared<detail::SequenceLoan>();
	loan->reader = detail::SharedImpl(self);
	loan->data = dataValues;
	loan->infos = sampleInfos;
	std::function<::dds_ReturnCode_t()> cleanup = [loan]() { return loan->finish(); };
	auto ret = dataValues.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = sampleInfos.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = ::dds_DataReader_read_w_condition(self, dataValues, sampleInfos, maxSamples, aCondition);
	loan->active = (ret == dds_RETCODE_OK);
	if(!loan->active)
		loan->reader.reset();
	return ret;
}

inline ::dds_ReturnCode_t dds_DataReader_take_w_condition(const dds_DataReader& self,
    dds_DataSeq& dataValues,
    dds_SampleInfoSeq& sampleInfos,
    int32_t maxSamples,
    const ::dds_ReadCondition* aCondition) {
	if(!self || !dataValues || !sampleInfos)
		return dds_RETCODE_BAD_PARAMETER;
	auto loan = std::make_shared<detail::SequenceLoan>();
	loan->reader = detail::SharedImpl(self);
	loan->data = dataValues;
	loan->infos = sampleInfos;
	std::function<::dds_ReturnCode_t()> cleanup = [loan]() { return loan->finish(); };
	auto ret = dataValues.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = sampleInfos.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = ::dds_DataReader_take_w_condition(self, dataValues, sampleInfos, maxSamples, aCondition);
	loan->active = (ret == dds_RETCODE_OK);
	if(!loan->active)
		loan->reader.reset();
	return ret;
}

inline ::dds_ReturnCode_t dds_DataReader_read_instance(const dds_DataReader& self,
    dds_DataSeq& dataValues,
    dds_SampleInfoSeq& sampleInfos,
    int32_t maxSamples,
    ::dds_InstanceHandle_t aHandle,
    ::dds_SampleStateMask sampleStates,
    ::dds_ViewStateMask viewStates,
    ::dds_InstanceStateMask instanceStates) {
	if(!self || !dataValues || !sampleInfos)
		return dds_RETCODE_BAD_PARAMETER;
	auto loan = std::make_shared<detail::SequenceLoan>();
	loan->reader = detail::SharedImpl(self);
	loan->data = dataValues;
	loan->infos = sampleInfos;
	std::function<::dds_ReturnCode_t()> cleanup = [loan]() { return loan->finish(); };
	auto ret = dataValues.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = sampleInfos.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = ::dds_DataReader_read_instance(
	    self, dataValues, sampleInfos, maxSamples, aHandle, sampleStates, viewStates, instanceStates);
	loan->active = (ret == dds_RETCODE_OK);
	if(!loan->active)
		loan->reader.reset();
	return ret;
}

inline ::dds_ReturnCode_t dds_DataReader_take_instance(const dds_DataReader& self,
    dds_DataSeq& dataValues,
    dds_SampleInfoSeq& sampleInfos,
    int32_t maxSamples,
    ::dds_InstanceHandle_t aHandle,
    ::dds_SampleStateMask sampleStates,
    ::dds_ViewStateMask viewStates,
    ::dds_InstanceStateMask instanceStates) {
	if(!self || !dataValues || !sampleInfos)
		return dds_RETCODE_BAD_PARAMETER;
	auto loan = std::make_shared<detail::SequenceLoan>();
	loan->reader = detail::SharedImpl(self);
	loan->data = dataValues;
	loan->infos = sampleInfos;
	std::function<::dds_ReturnCode_t()> cleanup = [loan]() { return loan->finish(); };
	auto ret = dataValues.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = sampleInfos.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = ::dds_DataReader_take_instance(
	    self, dataValues, sampleInfos, maxSamples, aHandle, sampleStates, viewStates, instanceStates);
	loan->active = (ret == dds_RETCODE_OK);
	if(!loan->active)
		loan->reader.reset();
	return ret;
}

inline ::dds_ReturnCode_t dds_DataReader_read_next_instance(const dds_DataReader& self,
    dds_DataSeq& dataValues,
    dds_SampleInfoSeq& sampleInfos,
    int32_t maxSamples,
    ::dds_InstanceHandle_t previousHandle,
    ::dds_SampleStateMask sampleStates,
    ::dds_ViewStateMask viewStates,
    ::dds_InstanceStateMask instanceStates) {
	if(!self || !dataValues || !sampleInfos)
		return dds_RETCODE_BAD_PARAMETER;
	auto loan = std::make_shared<detail::SequenceLoan>();
	loan->reader = detail::SharedImpl(self);
	loan->data = dataValues;
	loan->infos = sampleInfos;
	std::function<::dds_ReturnCode_t()> cleanup = [loan]() { return loan->finish(); };
	auto ret = dataValues.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = sampleInfos.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = ::dds_DataReader_read_next_instance(
	    self, dataValues, sampleInfos, maxSamples, previousHandle, sampleStates, viewStates, instanceStates);
	loan->active = (ret == dds_RETCODE_OK);
	if(!loan->active)
		loan->reader.reset();
	return ret;
}

inline ::dds_ReturnCode_t dds_DataReader_take_next_instance(const dds_DataReader& self,
    dds_DataSeq& dataValues,
    dds_SampleInfoSeq& sampleInfos,
    int32_t maxSamples,
    ::dds_InstanceHandle_t previousHandle,
    ::dds_SampleStateMask sampleStates,
    ::dds_ViewStateMask viewStates,
    ::dds_InstanceStateMask instanceStates) {
	if(!self || !dataValues || !sampleInfos)
		return dds_RETCODE_BAD_PARAMETER;
	auto loan = std::make_shared<detail::SequenceLoan>();
	loan->reader = detail::SharedImpl(self);
	loan->data = dataValues;
	loan->infos = sampleInfos;
	std::function<::dds_ReturnCode_t()> cleanup = [loan]() { return loan->finish(); };
	auto ret = dataValues.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = sampleInfos.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = ::dds_DataReader_take_next_instance(
	    self, dataValues, sampleInfos, maxSamples, previousHandle, sampleStates, viewStates, instanceStates);
	loan->active = (ret == dds_RETCODE_OK);
	if(!loan->active)
		loan->reader.reset();
	return ret;
}

inline ::dds_ReturnCode_t dds_DataReader_read_next_instance_w_condition(const dds_DataReader& self,
    dds_DataSeq& dataValues,
    dds_SampleInfoSeq& sampleInfos,
    int32_t maxSamples,
    ::dds_InstanceHandle_t previousHandle,
    const ::dds_ReadCondition* aCondition) {
	if(!self || !dataValues || !sampleInfos)
		return dds_RETCODE_BAD_PARAMETER;
	auto loan = std::make_shared<detail::SequenceLoan>();
	loan->reader = detail::SharedImpl(self);
	loan->data = dataValues;
	loan->infos = sampleInfos;
	std::function<::dds_ReturnCode_t()> cleanup = [loan]() { return loan->finish(); };
	auto ret = dataValues.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = sampleInfos.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = ::dds_DataReader_read_next_instance_w_condition(
	    self, dataValues, sampleInfos, maxSamples, previousHandle, aCondition);
	loan->active = (ret == dds_RETCODE_OK);
	if(!loan->active)
		loan->reader.reset();
	return ret;
}

inline ::dds_ReturnCode_t dds_DataReader_take_next_instance_w_condition(const dds_DataReader& self,
    dds_DataSeq& dataValues,
    dds_SampleInfoSeq& sampleInfos,
    int32_t maxSamples,
    ::dds_InstanceHandle_t previousHandle,
    const ::dds_ReadCondition* aCondition) {
	if(!self || !dataValues || !sampleInfos)
		return dds_RETCODE_BAD_PARAMETER;
	auto loan = std::make_shared<detail::SequenceLoan>();
	loan->reader = detail::SharedImpl(self);
	loan->data = dataValues;
	loan->infos = sampleInfos;
	std::function<::dds_ReturnCode_t()> cleanup = [loan]() { return loan->finish(); };
	auto ret = dataValues.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = sampleInfos.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = ::dds_DataReader_take_next_instance_w_condition(
	    self, dataValues, sampleInfos, maxSamples, previousHandle, aCondition);
	loan->active = (ret == dds_RETCODE_OK);
	if(!loan->active)
		loan->reader.reset();
	return ret;
}

inline ::dds_ReturnCode_t dds_DynamicDataReader_read(const dds_DataReader& self,
    dds_DynamicDataSeq& dataValues,
    dds_SampleInfoSeq& sampleInfos,
    int32_t maxSamples,
    ::dds_SampleStateMask sampleStates,
    ::dds_ViewStateMask viewStates,
    ::dds_InstanceStateMask instanceStates) {
	if(!self || !dataValues || !sampleInfos)
		return dds_RETCODE_BAD_PARAMETER;
	auto loan = std::make_shared<detail::SequenceLoan>();
	loan->reader = detail::SharedImpl(self);
	loan->dynamic_data = dataValues;
	loan->infos = sampleInfos;
	std::function<::dds_ReturnCode_t()> cleanup = [loan]() { return loan->finish(); };
	auto ret = dataValues.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = sampleInfos.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = ::dds_DynamicDataReader_read(
	    self, dataValues, sampleInfos, maxSamples, sampleStates, viewStates, instanceStates);
	loan->active = (ret == dds_RETCODE_OK);
	if(!loan->active)
		loan->reader.reset();
	return ret;
}

inline ::dds_ReturnCode_t dds_DynamicDataReader_take(const dds_DataReader& self,
    dds_DynamicDataSeq& dataValues,
    dds_SampleInfoSeq& sampleInfos,
    int32_t maxSamples,
    ::dds_SampleStateMask sampleStates,
    ::dds_ViewStateMask viewStates,
    ::dds_InstanceStateMask instanceStates) {
	if(!self || !dataValues || !sampleInfos)
		return dds_RETCODE_BAD_PARAMETER;
	auto loan = std::make_shared<detail::SequenceLoan>();
	loan->reader = detail::SharedImpl(self);
	loan->dynamic_data = dataValues;
	loan->infos = sampleInfos;
	std::function<::dds_ReturnCode_t()> cleanup = [loan]() { return loan->finish(); };
	auto ret = dataValues.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = sampleInfos.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = ::dds_DynamicDataReader_take(
	    self, dataValues, sampleInfos, maxSamples, sampleStates, viewStates, instanceStates);
	loan->active = (ret == dds_RETCODE_OK);
	if(!loan->active)
		loan->reader.reset();
	return ret;
}

inline ::dds_ReturnCode_t dds_DynamicDataReader_read_w_condition(const dds_DataReader& self,
    dds_DynamicDataSeq& dataValues,
    dds_SampleInfoSeq& sampleInfos,
    int32_t maxSamples,
    const ::dds_ReadCondition* aCondition) {
	if(!self || !dataValues || !sampleInfos)
		return dds_RETCODE_BAD_PARAMETER;
	auto loan = std::make_shared<detail::SequenceLoan>();
	loan->reader = detail::SharedImpl(self);
	loan->dynamic_data = dataValues;
	loan->infos = sampleInfos;
	std::function<::dds_ReturnCode_t()> cleanup = [loan]() { return loan->finish(); };
	auto ret = dataValues.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = sampleInfos.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = ::dds_DynamicDataReader_read_w_condition(self, dataValues, sampleInfos, maxSamples, aCondition);
	loan->active = (ret == dds_RETCODE_OK);
	if(!loan->active)
		loan->reader.reset();
	return ret;
}

inline ::dds_ReturnCode_t dds_DynamicDataReader_take_w_condition(const dds_DataReader& self,
    dds_DynamicDataSeq& dataValues,
    dds_SampleInfoSeq& sampleInfos,
    int32_t maxSamples,
    const ::dds_ReadCondition* aCondition) {
	if(!self || !dataValues || !sampleInfos)
		return dds_RETCODE_BAD_PARAMETER;
	auto loan = std::make_shared<detail::SequenceLoan>();
	loan->reader = detail::SharedImpl(self);
	loan->dynamic_data = dataValues;
	loan->infos = sampleInfos;
	std::function<::dds_ReturnCode_t()> cleanup = [loan]() { return loan->finish(); };
	auto ret = dataValues.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = sampleInfos.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = ::dds_DynamicDataReader_take_w_condition(self, dataValues, sampleInfos, maxSamples, aCondition);
	loan->active = (ret == dds_RETCODE_OK);
	if(!loan->active)
		loan->reader.reset();
	return ret;
}

inline ::dds_ReturnCode_t dds_DynamicDataReader_read_instance(const dds_DataReader& self,
    dds_DynamicDataSeq& dataValues,
    dds_SampleInfoSeq& sampleInfos,
    int32_t maxSamples,
    ::dds_InstanceHandle_t aHandle,
    ::dds_SampleStateMask sampleStates,
    ::dds_ViewStateMask viewStates,
    ::dds_InstanceStateMask instanceStates) {
	if(!self || !dataValues || !sampleInfos)
		return dds_RETCODE_BAD_PARAMETER;
	auto loan = std::make_shared<detail::SequenceLoan>();
	loan->reader = detail::SharedImpl(self);
	loan->dynamic_data = dataValues;
	loan->infos = sampleInfos;
	std::function<::dds_ReturnCode_t()> cleanup = [loan]() { return loan->finish(); };
	auto ret = dataValues.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = sampleInfos.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = ::dds_DynamicDataReader_read_instance(
	    self, dataValues, sampleInfos, maxSamples, aHandle, sampleStates, viewStates, instanceStates);
	loan->active = (ret == dds_RETCODE_OK);
	if(!loan->active)
		loan->reader.reset();
	return ret;
}

inline ::dds_ReturnCode_t dds_DynamicDataReader_take_instance(const dds_DataReader& self,
    dds_DynamicDataSeq& dataValues,
    dds_SampleInfoSeq& sampleInfos,
    int32_t maxSamples,
    ::dds_InstanceHandle_t aHandle,
    ::dds_SampleStateMask sampleStates,
    ::dds_ViewStateMask viewStates,
    ::dds_InstanceStateMask instanceStates) {
	if(!self || !dataValues || !sampleInfos)
		return dds_RETCODE_BAD_PARAMETER;
	auto loan = std::make_shared<detail::SequenceLoan>();
	loan->reader = detail::SharedImpl(self);
	loan->dynamic_data = dataValues;
	loan->infos = sampleInfos;
	std::function<::dds_ReturnCode_t()> cleanup = [loan]() { return loan->finish(); };
	auto ret = dataValues.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = sampleInfos.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = ::dds_DynamicDataReader_take_instance(
	    self, dataValues, sampleInfos, maxSamples, aHandle, sampleStates, viewStates, instanceStates);
	loan->active = (ret == dds_RETCODE_OK);
	if(!loan->active)
		loan->reader.reset();
	return ret;
}

inline ::dds_ReturnCode_t dds_DynamicDataReader_read_next_instance(const dds_DataReader& self,
    dds_DynamicDataSeq& dataValues,
    dds_SampleInfoSeq& sampleInfos,
    int32_t maxSamples,
    ::dds_InstanceHandle_t previousHandle,
    ::dds_SampleStateMask sampleStates,
    ::dds_ViewStateMask viewStates,
    ::dds_InstanceStateMask instanceStates) {
	if(!self || !dataValues || !sampleInfos)
		return dds_RETCODE_BAD_PARAMETER;
	auto loan = std::make_shared<detail::SequenceLoan>();
	loan->reader = detail::SharedImpl(self);
	loan->dynamic_data = dataValues;
	loan->infos = sampleInfos;
	std::function<::dds_ReturnCode_t()> cleanup = [loan]() { return loan->finish(); };
	auto ret = dataValues.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = sampleInfos.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = ::dds_DynamicDataReader_read_next_instance(
	    self, dataValues, sampleInfos, maxSamples, previousHandle, sampleStates, viewStates, instanceStates);
	loan->active = (ret == dds_RETCODE_OK);
	if(!loan->active)
		loan->reader.reset();
	return ret;
}

inline ::dds_ReturnCode_t dds_DynamicDataReader_take_next_instance(const dds_DataReader& self,
    dds_DynamicDataSeq& dataValues,
    dds_SampleInfoSeq& sampleInfos,
    int32_t maxSamples,
    ::dds_InstanceHandle_t previousHandle,
    ::dds_SampleStateMask sampleStates,
    ::dds_ViewStateMask viewStates,
    ::dds_InstanceStateMask instanceStates) {
	if(!self || !dataValues || !sampleInfos)
		return dds_RETCODE_BAD_PARAMETER;
	auto loan = std::make_shared<detail::SequenceLoan>();
	loan->reader = detail::SharedImpl(self);
	loan->dynamic_data = dataValues;
	loan->infos = sampleInfos;
	std::function<::dds_ReturnCode_t()> cleanup = [loan]() { return loan->finish(); };
	auto ret = dataValues.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = sampleInfos.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = ::dds_DynamicDataReader_take_next_instance(
	    self, dataValues, sampleInfos, maxSamples, previousHandle, sampleStates, viewStates, instanceStates);
	loan->active = (ret == dds_RETCODE_OK);
	if(!loan->active)
		loan->reader.reset();
	return ret;
}

inline ::dds_ReturnCode_t dds_DynamicDataReader_read_next_instance_w_condition(const dds_DataReader& self,
    dds_DynamicDataSeq& dataValues,
    dds_SampleInfoSeq& sampleInfos,
    int32_t maxSamples,
    ::dds_InstanceHandle_t previousHandle,
    const ::dds_ReadCondition* aCondition) {
	if(!self || !dataValues || !sampleInfos)
		return dds_RETCODE_BAD_PARAMETER;
	auto loan = std::make_shared<detail::SequenceLoan>();
	loan->reader = detail::SharedImpl(self);
	loan->dynamic_data = dataValues;
	loan->infos = sampleInfos;
	std::function<::dds_ReturnCode_t()> cleanup = [loan]() { return loan->finish(); };
	auto ret = dataValues.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = sampleInfos.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = ::dds_DynamicDataReader_read_next_instance_w_condition(
	    self, dataValues, sampleInfos, maxSamples, previousHandle, aCondition);
	loan->active = (ret == dds_RETCODE_OK);
	if(!loan->active)
		loan->reader.reset();
	return ret;
}

inline ::dds_ReturnCode_t dds_DynamicDataReader_take_next_instance_w_condition(const dds_DataReader& self,
    dds_DynamicDataSeq& dataValues,
    dds_SampleInfoSeq& sampleInfos,
    int32_t maxSamples,
    ::dds_InstanceHandle_t previousHandle,
    const ::dds_ReadCondition* aCondition) {
	if(!self || !dataValues || !sampleInfos)
		return dds_RETCODE_BAD_PARAMETER;
	auto loan = std::make_shared<detail::SequenceLoan>();
	loan->reader = detail::SharedImpl(self);
	loan->dynamic_data = dataValues;
	loan->infos = sampleInfos;
	std::function<::dds_ReturnCode_t()> cleanup = [loan]() { return loan->finish(); };
	auto ret = dataValues.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = sampleInfos.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = ::dds_DynamicDataReader_take_next_instance_w_condition(
	    self, dataValues, sampleInfos, maxSamples, previousHandle, aCondition);
	loan->active = (ret == dds_RETCODE_OK);
	if(!loan->active)
		loan->reader.reset();
	return ret;
}

inline ::dds_ReturnCode_t dds_DataReader_read_w_sampleinfoex(const dds_DataReader& self,
    dds_DataSeq& dataValues,
    dds_SampleInfoSeq& sampleInfos,
    int32_t maxSamples,
    ::dds_SampleStateMask sampleStates,
    ::dds_ViewStateMask viewStates,
    ::dds_InstanceStateMask instanceStates) {
	if(!self || !dataValues || !sampleInfos)
		return dds_RETCODE_BAD_PARAMETER;
	auto loan = std::make_shared<detail::SequenceLoan>();
	loan->reader = detail::SharedImpl(self);
	loan->data = dataValues;
	loan->infos = sampleInfos;
	std::function<::dds_ReturnCode_t()> cleanup = [loan]() { return loan->finish(); };
	auto ret = dataValues.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = sampleInfos.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = ::dds_DataReader_read_w_sampleinfoex(
	    self, dataValues, sampleInfos, maxSamples, sampleStates, viewStates, instanceStates);
	loan->active = (ret == dds_RETCODE_OK);
	if(!loan->active)
		loan->reader.reset();
	return ret;
}

inline ::dds_ReturnCode_t dds_DataReader_take_w_sampleinfoex(const dds_DataReader& self,
    dds_DataSeq& dataValues,
    dds_SampleInfoSeq& sampleInfos,
    int32_t maxSamples,
    ::dds_SampleStateMask sampleStates,
    ::dds_ViewStateMask viewStates,
    ::dds_InstanceStateMask instanceStates) {
	if(!self || !dataValues || !sampleInfos)
		return dds_RETCODE_BAD_PARAMETER;
	auto loan = std::make_shared<detail::SequenceLoan>();
	loan->reader = detail::SharedImpl(self);
	loan->data = dataValues;
	loan->infos = sampleInfos;
	std::function<::dds_ReturnCode_t()> cleanup = [loan]() { return loan->finish(); };
	auto ret = dataValues.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = sampleInfos.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = ::dds_DataReader_take_w_sampleinfoex(
	    self, dataValues, sampleInfos, maxSamples, sampleStates, viewStates, instanceStates);
	loan->active = (ret == dds_RETCODE_OK);
	if(!loan->active)
		loan->reader.reset();
	return ret;
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
	if(!self || !rawDataValues || !sampleInfos || !rawDataSizes)
		return dds_RETCODE_BAD_PARAMETER;
	auto loan = std::make_shared<detail::SequenceLoan>();
	loan->reader = detail::SharedImpl(self);
	loan->data = rawDataValues;
	loan->infos = sampleInfos;
	loan->sizes = rawDataSizes;
	std::function<::dds_ReturnCode_t()> cleanup = [loan]() { return loan->finish(); };
	auto ret = rawDataValues.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = sampleInfos.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = rawDataSizes.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = ::dds_DataReader_raw_read(
	    self, handle, rawDataValues, sampleInfos, rawDataSizes, maxSamples, sampleStates, viewStates, instanceStates);
	loan->active = (ret == dds_RETCODE_OK);
	if(!loan->active)
		loan->reader.reset();
	return ret;
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
	if(!self || !rawDataValues || !sampleInfoexs || !rawDataSizes)
		return dds_RETCODE_BAD_PARAMETER;
	auto loan = std::make_shared<detail::SequenceLoan>();
	loan->reader = detail::SharedImpl(self);
	loan->data = rawDataValues;
	loan->infos = sampleInfoexs;
	loan->sizes = rawDataSizes;
	std::function<::dds_ReturnCode_t()> cleanup = [loan]() { return loan->finish(); };
	auto ret = rawDataValues.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = sampleInfoexs.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = rawDataSizes.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = ::dds_DataReader_raw_read_w_sampleinfoex(
	    self, handle, rawDataValues, sampleInfoexs, rawDataSizes, maxSamples, sampleStates, viewStates, instanceStates);
	loan->active = (ret == dds_RETCODE_OK);
	if(!loan->active)
		loan->reader.reset();
	return ret;
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
	if(!self || !rawDataValues || !sampleInfos || !rawDataSizes)
		return dds_RETCODE_BAD_PARAMETER;
	auto loan = std::make_shared<detail::SequenceLoan>();
	loan->reader = detail::SharedImpl(self);
	loan->data = rawDataValues;
	loan->infos = sampleInfos;
	loan->sizes = rawDataSizes;
	std::function<::dds_ReturnCode_t()> cleanup = [loan]() { return loan->finish(); };
	auto ret = rawDataValues.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = sampleInfos.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = rawDataSizes.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = ::dds_DataReader_raw_take(
	    self, handle, rawDataValues, sampleInfos, rawDataSizes, maxSamples, sampleStates, viewStates, instanceStates);
	loan->active = (ret == dds_RETCODE_OK);
	if(!loan->active)
		loan->reader.reset();
	return ret;
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
	if(!self || !rawDataValues || !sampleInfoexs || !rawDataSizes)
		return dds_RETCODE_BAD_PARAMETER;
	auto loan = std::make_shared<detail::SequenceLoan>();
	loan->reader = detail::SharedImpl(self);
	loan->data = rawDataValues;
	loan->infos = sampleInfoexs;
	loan->sizes = rawDataSizes;
	std::function<::dds_ReturnCode_t()> cleanup = [loan]() { return loan->finish(); };
	auto ret = rawDataValues.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = sampleInfoexs.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = rawDataSizes.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = ::dds_DataReader_raw_take_w_sampleinfoex(
	    self, handle, rawDataValues, sampleInfoexs, rawDataSizes, maxSamples, sampleStates, viewStates, instanceStates);
	loan->active = (ret == dds_RETCODE_OK);
	if(!loan->active)
		loan->reader.reset();
	return ret;
}

inline ::dds_ReturnCode_t dds_DataReader_return_loan(
    const dds_DataReader& self, dds_DataSeq& data, dds_SampleInfoSeq& infos) noexcept {
	if(!self || !data || !infos)
		return dds_RETCODE_BAD_PARAMETER;
	if(data.loan_identity()) {
		if(data.loan_identity() != infos.loan_identity() || data.loan_reader() != self.get())
			return dds_RETCODE_PRECONDITION_NOT_MET;
		auto ret = data.finish_loan();
		if(ret != dds_RETCODE_OK)
			return ret;
		(void)infos.finish_loan();

		return ret;
	}
	if(infos.loan_identity())
		return dds_RETCODE_PRECONDITION_NOT_MET;
	return ::dds_DataReader_return_loan(self, data, infos);
}

inline ::dds_ReturnCode_t dds_DataReader_raw_return_loan(
    const dds_DataReader& self, dds_DataSeq& data, dds_SampleInfoSeq& infos, dds_UnsignedLongSeq& sizes) noexcept {
	if(!self || !data || !infos || !sizes)
		return dds_RETCODE_BAD_PARAMETER;
	if(data.loan_identity()) {
		if(data.loan_identity() != infos.loan_identity() || data.loan_reader() != self.get() ||
		    data.loan_identity() != sizes.loan_identity())
			return dds_RETCODE_PRECONDITION_NOT_MET;
		auto ret = data.finish_loan();
		if(ret != dds_RETCODE_OK)
			return ret;
		(void)infos.finish_loan();
		(void)sizes.finish_loan();
		return ret;
	}
	if(infos.loan_identity() || sizes.loan_identity())
		return dds_RETCODE_PRECONDITION_NOT_MET;
	return ::dds_DataReader_raw_return_loan(self, data, infos, sizes);
}

inline ::dds_ReturnCode_t dds_DynamicDataReader_return_loan(
    const dds_DataReader& self, dds_DynamicDataSeq& data, dds_SampleInfoSeq& infos) noexcept {
	if(!self || !data || !infos)
		return dds_RETCODE_BAD_PARAMETER;
	if(data.loan_identity()) {
		if(data.loan_identity() != infos.loan_identity() || data.loan_reader() != self.get())
			return dds_RETCODE_PRECONDITION_NOT_MET;
		auto ret = data.finish_loan();
		if(ret != dds_RETCODE_OK)
			return ret;
		(void)infos.finish_loan();

		return ret;
	}
	if(infos.loan_identity())
		return dds_RETCODE_PRECONDITION_NOT_MET;
	return ::dds_DynamicDataReader_return_loan(self, data, infos);
}

struct dds_ReaderLoan_deleter {
	std::shared_ptr<DataReader> reader;

	::dds_ReturnCode_t operator()(void* raw) const noexcept {
		return ::dds_DataReader_return_loaned_sample(**reader, raw);
	}
};

using dds_ReaderLoan = Resource<void, dds_ReaderLoan_deleter>;

inline ::dds_ReturnCode_t dds_DataReader_return_loaned_sample(
    const dds_DataReader& self, dds_ReaderLoan& sample) noexcept {
	if(!self || !sample)
		return dds_RETCODE_BAD_PARAMETER;
	if(sample.deleter()->reader != detail::SharedImpl(self))
		return dds_RETCODE_PRECONDITION_NOT_MET;
	return sample.close();
}

struct dds_WriterLoan_deleter {
	std::shared_ptr<DataWriter> writer;

	::dds_ReturnCode_t operator()(void* raw) const noexcept {
		::dds_DataWriter_return_loaned_sample(**writer, raw);
		return dds_RETCODE_OK;
	}
};

using dds_WriterLoan = Resource<void, dds_WriterLoan_deleter>;

inline void dds_DataWriter_return_loaned_sample(const dds_DataWriter& self, dds_WriterLoan& sample) {
	if(sample && sample.deleter()->writer != detail::SharedImpl(self))
		throw std::invalid_argument("DataWriter does not own this loan");
	sample.reset();
}

inline ::dds_ReturnCode_t dds_DataWriter_get_loan(const dds_DataWriter& self, dds_WriterLoan& sample, size_t* size) {
	if(!self || !size)
		return dds_RETCODE_BAD_PARAMETER;
	auto ret = sample.close();
	if(ret != dds_RETCODE_OK)
		return ret;
	dds_WriterLoan_deleter cleanup;
	cleanup.writer = detail::SharedImpl(self);
	sample = dds_WriterLoan::create(
	    [&]() {
		    void* raw = nullptr;
		    ret = ::dds_DataWriter_get_loan(self, &raw, size);
		    return ret == dds_RETCODE_OK ? raw : nullptr;
	    },
	    cleanup);
	return ret;
}

inline ::dds_ReturnCode_t dds_DataReader_read_next_loaned_sample(
    const dds_DataReader& self, dds_ReaderLoan& sample, ::dds_SampleInfoEx* info) {
	if(!self || !info)
		return dds_RETCODE_BAD_PARAMETER;
	auto ret = sample.close();
	if(ret != dds_RETCODE_OK)
		return ret;
	dds_ReaderLoan_deleter cleanup;
	cleanup.reader = detail::SharedImpl(self);
	sample = dds_ReaderLoan::create(
	    [&]() {
		    void* raw = nullptr;
		    ret = ::dds_DataReader_read_next_loaned_sample(self, &raw, info);
		    return ret == dds_RETCODE_OK ? raw : nullptr;
	    },
	    cleanup);
	return ret;
}

inline ::dds_ReturnCode_t dds_DataReader_take_next_loaned_sample(
    const dds_DataReader& self, dds_ReaderLoan& sample, ::dds_SampleInfoEx* info) {
	if(!self || !info)
		return dds_RETCODE_BAD_PARAMETER;
	auto ret = sample.close();
	if(ret != dds_RETCODE_OK)
		return ret;
	dds_ReaderLoan_deleter cleanup;
	cleanup.reader = detail::SharedImpl(self);
	sample = dds_ReaderLoan::create(
	    [&]() {
		    void* raw = nullptr;
		    ret = ::dds_DataReader_take_next_loaned_sample(self, &raw, info);
		    return ret == dds_RETCODE_OK ? raw : nullptr;
	    },
	    cleanup);
	return ret;
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
	return detail::SharedImpl(self)->attach(condition);
}

inline ::dds_ReturnCode_t dds_WaitSet_detach_condition(const dds_WaitSet& self, const dds_StatusCondition& condition) {
	if(!self || !condition)
		return dds_RETCODE_BAD_PARAMETER;
	return detail::SharedImpl(self)->detach(condition);
}

// DynamicTypeBuilder objects and singleton factories are factory-owned in this
// C API: no public per-builder delete exists. Keep those as native C handles.
// Likewise get/lookup/find results remain borrowed and must not be adopted.

using dds_Wstring = Resource<uint16_t, dds_Memory_deleter>;
using dds_WideString = Resource<wchar_t, dds_Memory_deleter>;

inline void dds_free(dds_Wstring& value) noexcept {
	value.reset();
}

inline void dds_free(dds_WideString& value) noexcept {
	value.reset();
}

inline dds_Wstring dds_wcsdup(const uint16_t* value) {
	return dds_Wstring::create([=]() { return ::dds_wcsdup(value); });
}

inline dds_Wstring dds_wide_to_utf16(const wchar_t* value) {
	return dds_Wstring::create([=]() { return ::dds_wide_to_utf16(value); });
}

inline dds_WideString dds_utf16_to_wide(const ::dds_Wchar* value) {
	return dds_WideString::create([=]() { return ::dds_utf16_to_wide(value); });
}

inline dds_String dds_StringTypeSupport_deserialize(void* input, size_t inputSize) {
	return dds_String::create([=]() { return ::dds_StringTypeSupport_deserialize(input, inputSize); });
}

struct dds_md5_ctx_deleter {
	::dds_ReturnCode_t operator()(::dds_md5_ctx* raw) const noexcept {
		::dds_md5_ctx_free(raw);
		return dds_RETCODE_OK;
	}
};

using dds_md5_ctx = Resource<::dds_md5_ctx, dds_md5_ctx_deleter>;

inline dds_md5_ctx dds_md5_ctx_alloc() {
	return dds_md5_ctx::create([]() { return static_cast<::dds_md5_ctx*>(::dds_md5_ctx_alloc()); });
}

inline void dds_md5_ctx_free(dds_md5_ctx& context) noexcept {
	context.reset();
}

inline ::dds_ReturnCode_t dds_DynamicType_get_descriptor(::dds_DynamicType* self, dds_TypeDescriptor& value) noexcept {
	if(!self)
		return dds_RETCODE_BAD_PARAMETER;
	(void)::dds_TypeDescriptor_finalize(value);
	return ::dds_DynamicType_get_descriptor(self, value);
}

inline ::dds_ReturnCode_t dds_DynamicTypeMember_get_descriptor(
    ::dds_DynamicTypeMember* self, dds_MemberDescriptor& value) noexcept {
	if(!self)
		return dds_RETCODE_BAD_PARAMETER;
	(void)::dds_MemberDescriptor_finalize(value);
	return ::dds_DynamicTypeMember_get_descriptor(self, value);
}

inline ::dds_ReturnCode_t dds_DynamicData_get_descriptor(
    ::dds_DynamicData* self, dds_MemberDescriptor& value, ::dds_MemberId_t id) noexcept {
	if(!self)
		return dds_RETCODE_BAD_PARAMETER;
	(void)::dds_MemberDescriptor_finalize(value);
	return ::dds_DynamicData_get_descriptor(self, value, id);
}

// Instance handles are values, not owned DDS entities.
inline ::dds_InstanceHandle_t dds_DataWriter_lookup_instance(const dds_DataWriter& self, const void* data) {
	return self ? ::dds_DataWriter_lookup_instance(self, data) : dds_HANDLE_NIL;
}

inline ::dds_InstanceHandle_t dds_DataReader_lookup_instance(const dds_DataReader& self, const void* data) {
	return self ? ::dds_DataReader_lookup_instance(self, data) : dds_HANDLE_NIL;
}

inline ::dds_InstanceHandle_t dds_DynamicDataWriter_lookup_instance(
    const dds_DataWriter& self, const ::dds_DynamicData* data) {
	return self ? ::dds_DynamicDataWriter_lookup_instance(self, data) : dds_HANDLE_NIL;
}

inline ::dds_InstanceHandle_t dds_DynamicDataReader_lookup_instance(
    const dds_DataReader& self, const ::dds_DynamicData* data) {
	return self ? ::dds_DynamicDataReader_lookup_instance(self, data) : dds_HANDLE_NIL;
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
		result.push_back(dds_DynamicTypeSupport::create(
		    [&]() { return static_cast<::dds_DynamicTypeSupport*>(::dds_DataSeq_remove(sequence.get(), 0)); }));
	}
	return result;
}

} // namespace raii

#endif // RAII_HPP
