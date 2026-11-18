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
std::shared_ptr<TImpl>& shared_impl(DDSEntity<TImpl>& h) noexcept;

template <typename TImpl>
const std::shared_ptr<TImpl>& shared_impl(const DDSEntity<TImpl>& h) noexcept;

inline void terminate_on_error(::dds_ReturnCode_t ret, const char* func) noexcept {
	if(ret != dds_RETCODE_OK) {
		std::cerr << "fail " << func << "() ret=" << ret << '\n';
		std::terminate();
	}
}

inline ::dds_ReturnCode_t bad_parameter() noexcept {
	return dds_RETCODE_BAD_PARAMETER;
}

inline void clear_listener(::dds_DomainParticipant* p) noexcept {
	if(p == nullptr) {
		return;
	}
	(void)::dds_DomainParticipant_set_listener(p, nullptr, 0);
	(void)::dds_DomainParticipant_set_listener_context(p, nullptr);
}

inline void clear_listener(::dds_Topic* p) noexcept {
	if(p == nullptr) {
		return;
	}
	(void)::dds_Topic_set_listener(p, nullptr, 0);
	(void)::dds_Topic_set_listener_context(p, nullptr);
}

inline void clear_listener(::dds_Publisher* p) noexcept {
	if(p == nullptr) {
		return;
	}
	(void)::dds_Publisher_set_listener(p, nullptr, 0);
	(void)::dds_Publisher_set_listener_context(p, nullptr);
}

inline void clear_listener(::dds_Subscriber* p) noexcept {
	if(p == nullptr) {
		return;
	}
	(void)::dds_Subscriber_set_listener(p, nullptr, 0);
	(void)::dds_Subscriber_set_listener_context(p, nullptr);
}

inline void clear_listener(::dds_DataWriter* p) noexcept {
	if(p == nullptr) {
		return;
	}
	(void)::dds_DataWriter_set_listener(p, nullptr, 0);
	(void)::dds_DataWriter_set_listener_context(p, nullptr);
}

inline void clear_listener(::dds_DataReader* p) noexcept {
	if(p == nullptr) {
		return;
	}
	(void)::dds_DataReader_set_listener(p, nullptr, 0);
	(void)::dds_DataReader_set_listener_context(p, nullptr);
}

template <typename T>
inline T* condition_cast(T* p) noexcept {
	return p;
}

inline ::dds_Condition* condition_cast(::dds_GuardCondition* p) noexcept {
	return reinterpret_cast<::dds_Condition*>(p);
}

inline ::dds_Condition* condition_cast(::dds_ReadCondition* p) noexcept {
	return reinterpret_cast<::dds_Condition*>(p);
}

inline ::dds_Condition* condition_cast(::dds_QueryCondition* p) noexcept {
	return reinterpret_cast<::dds_Condition*>(p);
}

inline ::dds_Condition* condition_cast(::dds_StatusCondition* p) noexcept {
	return reinterpret_cast<::dds_Condition*>(p);
}

inline const ::dds_TopicDescription* topic_description_cast(const ::dds_Topic* p) noexcept {
	return reinterpret_cast<const ::dds_TopicDescription*>(p);
}

inline const ::dds_TopicDescription* topic_description_cast(const ::dds_ContentFilteredTopic* p) noexcept {
	return reinterpret_cast<const ::dds_TopicDescription*>(p);
}

inline const ::dds_TopicDescription* topic_description_cast(const ::dds_MultiTopic* p) noexcept {
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

	friend std::shared_ptr<TImpl>& detail::shared_impl<TImpl>(DDSEntity<TImpl>& h) noexcept;
	friend const std::shared_ptr<TImpl>& detail::shared_impl<TImpl>(const DDSEntity<TImpl>& h) noexcept;
};

namespace detail {

template <typename TImpl>
std::shared_ptr<TImpl>& shared_impl(DDSEntity<TImpl>& h) noexcept {
	return h.m_sharedptr;
}

template <typename TImpl>
const std::shared_ptr<TImpl>& shared_impl(const DDSEntity<TImpl>& h) noexcept {
	return h.m_sharedptr;
}

template <typename Impl, typename Raw, typename... Args>
std::shared_ptr<Impl> make_owned(Raw* raw, const Args&... args) {
	try {
		return std::make_shared<Impl>(raw, args...);
	} catch(...) {
		Impl cleanup(raw, args...);
		throw;
	}
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
		detail::clear_listener(m_entity);
		::dds_DomainParticipantFactory* dpf = ::dds_DomainParticipantFactory_get_instance();
		::dds_ReturnCode_t ret = ::dds_DomainParticipantFactory_delete_participant(dpf, m_entity);
		detail::terminate_on_error(ret, "dds_DomainParticipantFactory_delete_participant");
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
		detail::clear_listener(m_entity);
		if(m_parent && **m_parent != nullptr) {
			::dds_ReturnCode_t ret = ::dds_DomainParticipant_delete_topic(**m_parent, m_entity);
			detail::terminate_on_error(ret, "dds_DomainParticipant_delete_topic");
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
	    std::shared_ptr<Topic> related_topic) noexcept
	    : m_parent(std::move(parent))
	    , m_related_topic(std::move(related_topic))
	    , m_entity(entity) {
	}

	~ContentFilteredTopic() noexcept {
		if(m_entity == nullptr) {
			return;
		}
		if(m_parent && **m_parent != nullptr) {
			::dds_ReturnCode_t ret = ::dds_DomainParticipant_delete_contentfilteredtopic(**m_parent, m_entity);
			detail::terminate_on_error(ret, "dds_DomainParticipant_delete_contentfilteredtopic");
		}
		m_entity = nullptr;
	}

private:
	std::shared_ptr<DomainParticipant> m_parent;
	std::shared_ptr<Topic> m_related_topic;
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
			detail::terminate_on_error(ret, "dds_DomainParticipant_delete_multitopic");
		}
		m_entity = nullptr;
	}

private:
	std::shared_ptr<DomainParticipant> m_parent;
	::dds_MultiTopic* m_entity{nullptr};
};

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
		detail::clear_listener(m_entity);
		if(m_parent && **m_parent != nullptr) {
			::dds_ReturnCode_t ret = ::dds_DomainParticipant_delete_publisher(**m_parent, m_entity);
			detail::terminate_on_error(ret, "dds_DomainParticipant_delete_publisher");
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
		detail::clear_listener(m_entity);
		if(m_parent && **m_parent != nullptr) {
			::dds_ReturnCode_t ret = ::dds_DomainParticipant_delete_subscriber(**m_parent, m_entity);
			detail::terminate_on_error(ret, "dds_DomainParticipant_delete_subscriber");
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
		detail::clear_listener(m_entity);
		if(m_parent && **m_parent != nullptr) {
			::dds_ReturnCode_t ret = ::dds_Publisher_delete_datawriter(**m_parent, m_entity);
			detail::terminate_on_error(ret, "dds_Publisher_delete_datawriter");
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
	    std::shared_ptr<void> topic_description_owner) noexcept
	    : m_parent(std::move(parent))
	    , m_topic_description_owner(std::move(topic_description_owner))
	    , m_entity(entity) {
	}

	~DataReader() noexcept {
		if(m_entity == nullptr) {
			return;
		}
		detail::clear_listener(m_entity);
		if(m_parent && **m_parent != nullptr) {
			::dds_ReturnCode_t ret = ::dds_Subscriber_delete_datareader(**m_parent, m_entity);
			detail::terminate_on_error(ret, "dds_Subscriber_delete_datareader");
		}
		m_entity = nullptr;
	}

private:
	std::shared_ptr<Subscriber> m_parent;
	std::shared_ptr<void> m_topic_description_owner;
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
			detail::terminate_on_error(ret, "dds_DataReader_delete_readcondition");
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
			detail::terminate_on_error(ret, "dds_DataReader_delete_readcondition");
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
		detail::terminate_on_error(ret, "dds_WaitSet_delete");
		m_entity = nullptr;
	}

	struct Attachment {
		::dds_Condition* raw;
		std::shared_ptr<void> owner;
	};

	template <typename C>
	::dds_ReturnCode_t attach(const C& condition) {
		auto raw = detail::condition_cast(condition.get());
		for(const auto& item : m_attached)
			if(item.raw == raw)
				return ::dds_WaitSet_attach_condition(m_entity, raw);
		m_attached.push_back({raw, detail::shared_impl(condition)});
		const auto ret = ::dds_WaitSet_attach_condition(m_entity, raw);
		if(ret != dds_RETCODE_OK)
			m_attached.pop_back();
		return ret;
	}

	template <typename C>
	::dds_ReturnCode_t detach(const C& condition) {
		auto raw = detail::condition_cast(condition.get());
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
			detail::terminate_on_error(::dds_DynamicDataFactory_delete_instance(), "dds_DynamicDataFactory_delete_instance");
			detail::terminate_on_error(
			    ::dds_DynamicTypeBuilderFactory_delete_instance(), "dds_DynamicTypeBuilderFactory_delete_instance");
			::dds_DomainParticipantFactory_shutdown();
		}
	};

	auto* factory = ::dds_DomainParticipantFactory_get_instance();
	if(factory != nullptr) {
		static ShutdownGuard guard;
		(void)guard;
	}
	return factory;
}

inline dds_DomainParticipant dds_DomainParticipantFactory_create_participant(::dds_DomainParticipantFactory* self,
    ::dds_DomainId_t domain_id,
    const ::dds_DomainParticipantQos* qos,
    const ::dds_DomainParticipantListener* a_listener,
    ::dds_StatusMask mask) {
	::dds_DomainParticipant* raw =
	    ::dds_DomainParticipantFactory_create_participant(self, domain_id, qos, a_listener, mask);
	if(raw == nullptr) {
		return {};
	}
	return dds_DomainParticipant(detail::make_owned<DomainParticipant>(raw));
}

inline ::dds_ReturnCode_t dds_DomainParticipantFactory_delete_participant(
    ::dds_DomainParticipantFactory* self, dds_DomainParticipant& a_participant) {
	if(detail::shared_impl(a_participant).use_count() > 1)
		return dds_RETCODE_PRECONDITION_NOT_MET;
	if(!a_participant) {
		return detail::bad_parameter();
	}
	detail::clear_listener(static_cast<::dds_DomainParticipant*>(a_participant));
	::dds_ReturnCode_t ret = ::dds_DomainParticipantFactory_delete_participant(self, a_participant);
	if(ret == dds_RETCODE_OK) {
		*detail::shared_impl(a_participant) = nullptr;
		a_participant = nullptr;
	}
	return ret;
}

inline dds_DomainParticipant dds_DomainParticipantFactory_create_participant_w_id(::dds_DomainParticipantFactory* self,
    ::dds_DomainId_t domain_id,
    uint16_t participant_id,
    const ::dds_DomainParticipantQos* qos,
    const ::dds_DomainParticipantListener* a_listener,
    ::dds_StatusMask mask) {
	::dds_DomainParticipant* raw =
	    ::dds_DomainParticipantFactory_create_participant_w_id(self, domain_id, participant_id, qos, a_listener, mask);
	if(raw == nullptr) {
		return {};
	}
	return dds_DomainParticipant(detail::make_owned<DomainParticipant>(raw));
}

inline dds_DomainParticipant dds_DomainParticipantFactory_create_participant_w_props(
    ::dds_DomainParticipantFactory* self,
    ::dds_DomainId_t domain_id,
    const ::dds_DomainParticipantQos* qos,
    const ::dds_DomainParticipantListener* a_listener,
    ::dds_StatusMask mask,
    ::dds_StringProperty* props) {
	::dds_DomainParticipant* raw =
	    ::dds_DomainParticipantFactory_create_participant_w_props(self, domain_id, qos, a_listener, mask, props);
	if(raw == nullptr) {
		return {};
	}
	return dds_DomainParticipant(detail::make_owned<DomainParticipant>(raw));
}

inline dds_DomainParticipant dds_DomainParticipantFactory_create_participant_w_profile(
    ::dds_DomainParticipantFactory* self,
    ::dds_DomainId_t domain_id,
    const char* profile_name,
    const ::dds_DomainParticipantListener* a_listener,
    ::dds_StatusMask mask) {
	::dds_DomainParticipant* raw =
	    ::dds_DomainParticipantFactory_create_participant_w_profile(self, domain_id, profile_name, a_listener, mask);
	if(raw == nullptr) {
		return {};
	}
	return dds_DomainParticipant(detail::make_owned<DomainParticipant>(raw));
}

// -----------------------------------------------------------------------------
// DomainParticipant-owned entities
// -----------------------------------------------------------------------------
inline dds_Publisher dds_DomainParticipant_create_publisher(const dds_DomainParticipant& self,
    const ::dds_PublisherQos* qos,
    const ::dds_PublisherListener* a_listener,
    ::dds_StatusMask mask) {
	::dds_Publisher* raw =
	    ::dds_DomainParticipant_create_publisher(static_cast<::dds_DomainParticipant*>(self), qos, a_listener, mask);
	if(raw == nullptr) {
		return {};
	}
	return dds_Publisher(detail::make_owned<Publisher>(raw, detail::shared_impl(self)));
}

inline ::dds_ReturnCode_t dds_DomainParticipant_delete_publisher(
    const dds_DomainParticipant& self, dds_Publisher& publisher) {
	if(!self || !publisher) {
		return detail::bad_parameter();
	}
	if(detail::shared_impl(publisher).use_count() > 1)
		return dds_RETCODE_PRECONDITION_NOT_MET;
	detail::clear_listener(static_cast<::dds_Publisher*>(publisher));
	::dds_ReturnCode_t ret =
	    ::dds_DomainParticipant_delete_publisher(static_cast<::dds_DomainParticipant*>(self), publisher);
	if(ret == dds_RETCODE_OK) {
		*detail::shared_impl(publisher) = nullptr;
		publisher = nullptr;
	}
	return ret;
}

inline dds_Subscriber dds_DomainParticipant_create_subscriber(const dds_DomainParticipant& self,
    const ::dds_SubscriberQos* qos,
    const ::dds_SubscriberListener* a_listener,
    ::dds_StatusMask mask) {
	::dds_Subscriber* raw =
	    ::dds_DomainParticipant_create_subscriber(static_cast<::dds_DomainParticipant*>(self), qos, a_listener, mask);
	if(raw == nullptr) {
		return {};
	}
	return dds_Subscriber(detail::make_owned<Subscriber>(raw, detail::shared_impl(self)));
}

inline ::dds_ReturnCode_t dds_DomainParticipant_delete_subscriber(
    const dds_DomainParticipant& self, dds_Subscriber& subscriber) {
	if(!self || !subscriber) {
		return detail::bad_parameter();
	}
	if(detail::shared_impl(subscriber).use_count() > 1)
		return dds_RETCODE_PRECONDITION_NOT_MET;
	detail::clear_listener(static_cast<::dds_Subscriber*>(subscriber));
	::dds_ReturnCode_t ret =
	    ::dds_DomainParticipant_delete_subscriber(static_cast<::dds_DomainParticipant*>(self), subscriber);
	if(ret == dds_RETCODE_OK) {
		*detail::shared_impl(subscriber) = nullptr;
		subscriber = nullptr;
	}
	return ret;
}

inline dds_Topic dds_DomainParticipant_create_topic(const dds_DomainParticipant& self,
    const char* topic_name,
    const char* type_name,
    const ::dds_TopicQos* qos,
    const ::dds_TopicListener* a_listener,
    ::dds_StatusMask mask) {
	::dds_Topic* raw = ::dds_DomainParticipant_create_topic(
	    static_cast<::dds_DomainParticipant*>(self), topic_name, type_name, qos, a_listener, mask);
	if(raw == nullptr) {
		return {};
	}
	return dds_Topic(detail::make_owned<Topic>(raw, detail::shared_impl(self)));
}

inline ::dds_ReturnCode_t dds_DomainParticipant_delete_topic(const dds_DomainParticipant& self, dds_Topic& topic) {
	if(!self || !topic) {
		return detail::bad_parameter();
	}
	if(detail::shared_impl(topic).use_count() > 1)
		return dds_RETCODE_PRECONDITION_NOT_MET;
	detail::clear_listener(static_cast<::dds_Topic*>(topic));
	::dds_ReturnCode_t ret = ::dds_DomainParticipant_delete_topic(static_cast<::dds_DomainParticipant*>(self), topic);
	if(ret == dds_RETCODE_OK) {
		*detail::shared_impl(topic) = nullptr;
		topic = nullptr;
	}
	return ret;
}

inline dds_ContentFilteredTopic dds_DomainParticipant_create_contentfilteredtopic(const dds_DomainParticipant& self,
    const char* name,
    const dds_Topic& related_topic,
    const char* filter_expression,
    const ::dds_StringSeq* expression_parameters) {
	::dds_ContentFilteredTopic* raw =
	    ::dds_DomainParticipant_create_contentfilteredtopic(static_cast<::dds_DomainParticipant*>(self),
	        name,
	        static_cast<::dds_Topic*>(related_topic),
	        filter_expression,
	        expression_parameters);
	if(raw == nullptr) {
		return {};
	}
	return dds_ContentFilteredTopic(
	    detail::make_owned<ContentFilteredTopic>(raw, detail::shared_impl(self), detail::shared_impl(related_topic)));
}

inline ::dds_ReturnCode_t dds_DomainParticipant_delete_contentfilteredtopic(
    const dds_DomainParticipant& self, dds_ContentFilteredTopic& topic) {
	if(!self || !topic) {
		return detail::bad_parameter();
	}
	if(detail::shared_impl(topic).use_count() > 1)
		return dds_RETCODE_PRECONDITION_NOT_MET;
	::dds_ReturnCode_t ret =
	    ::dds_DomainParticipant_delete_contentfilteredtopic(static_cast<::dds_DomainParticipant*>(self), topic);
	if(ret == dds_RETCODE_OK) {
		*detail::shared_impl(topic) = nullptr;
		topic = nullptr;
	}
	return ret;
}

inline dds_MultiTopic dds_DomainParticipant_create_multitopic(const dds_DomainParticipant& self,
    const char* name,
    const char* type_name,
    const char* subscription_expression,
    const ::dds_StringSeq* expression_parameters) {
	::dds_MultiTopic* raw = ::dds_DomainParticipant_create_multitopic(
	    static_cast<::dds_DomainParticipant*>(self), name, type_name, subscription_expression, expression_parameters);
	if(raw == nullptr) {
		return {};
	}
	return dds_MultiTopic(detail::make_owned<MultiTopic>(raw, detail::shared_impl(self)));
}

inline ::dds_ReturnCode_t dds_DomainParticipant_delete_multitopic(
    const dds_DomainParticipant& self, dds_MultiTopic& topic) {
	if(!self || !topic) {
		return detail::bad_parameter();
	}
	if(detail::shared_impl(topic).use_count() > 1)
		return dds_RETCODE_PRECONDITION_NOT_MET;
	::dds_ReturnCode_t ret =
	    ::dds_DomainParticipant_delete_multitopic(static_cast<::dds_DomainParticipant*>(self), topic);
	if(ret == dds_RETCODE_OK) {
		*detail::shared_impl(topic) = nullptr;
		topic = nullptr;
	}
	return ret;
}

// -----------------------------------------------------------------------------
// Publisher-owned entities
// -----------------------------------------------------------------------------
inline dds_DataWriter dds_Publisher_create_datawriter(const dds_Publisher& self,
    const dds_Topic& a_topic,
    const ::dds_DataWriterQos* qos,
    const ::dds_DataWriterListener* a_listener,
    ::dds_StatusMask mask) {
	::dds_DataWriter* raw = ::dds_Publisher_create_datawriter(
	    static_cast<::dds_Publisher*>(self), static_cast<::dds_Topic*>(a_topic), qos, a_listener, mask);
	if(raw == nullptr) {
		return {};
	}
	return dds_DataWriter(detail::make_owned<DataWriter>(raw, detail::shared_impl(self), detail::shared_impl(a_topic)));
}

inline dds_DataWriter dds_Publisher_create_datawriter_w_props(const dds_Publisher& self,
    const dds_Topic& a_topic,
    const ::dds_DataWriterQos* qos,
    const ::dds_DataWriterListener* a_listener,
    ::dds_StatusMask mask,
    ::dds_StringProperty* props) {
	::dds_DataWriter* raw = ::dds_Publisher_create_datawriter_w_props(
	    static_cast<::dds_Publisher*>(self), static_cast<::dds_Topic*>(a_topic), qos, a_listener, mask, props);
	if(raw == nullptr) {
		return {};
	}
	return dds_DataWriter(detail::make_owned<DataWriter>(raw, detail::shared_impl(self), detail::shared_impl(a_topic)));
}

inline dds_DataWriter dds_Publisher_create_datawriter_w_profile(const dds_Publisher& self,
    const dds_Topic& a_topic,
    const char* profile_name,
    const ::dds_DataWriterListener* a_listener,
    ::dds_StatusMask mask) {
	::dds_DataWriter* raw = ::dds_Publisher_create_datawriter_w_profile(
	    static_cast<::dds_Publisher*>(self), static_cast<::dds_Topic*>(a_topic), profile_name, a_listener, mask);
	if(raw == nullptr) {
		return {};
	}
	return dds_DataWriter(detail::make_owned<DataWriter>(raw, detail::shared_impl(self), detail::shared_impl(a_topic)));
}

// Raw topic pointers are borrowed. The caller must keep the topic alive until
// the endpoint is deleted; only its publisher/subscriber is retained.
inline dds_DataWriter dds_Publisher_create_datawriter(const dds_Publisher& self,
    const ::dds_Topic* a_topic,
    const ::dds_DataWriterQos* qos,
    const ::dds_DataWriterListener* a_listener,
    ::dds_StatusMask mask) {
	::dds_DataWriter* raw =
	    ::dds_Publisher_create_datawriter(static_cast<::dds_Publisher*>(self), a_topic, qos, a_listener, mask);
	if(raw == nullptr) {
		return {};
	}
	return dds_DataWriter(detail::make_owned<DataWriter>(raw, detail::shared_impl(self), std::shared_ptr<Topic>{}));
}

inline dds_DataWriter dds_Publisher_create_datawriter_w_props(const dds_Publisher& self,
    const ::dds_Topic* a_topic,
    const ::dds_DataWriterQos* qos,
    const ::dds_DataWriterListener* a_listener,
    ::dds_StatusMask mask,
    ::dds_StringProperty* props) {
	::dds_DataWriter* raw = ::dds_Publisher_create_datawriter_w_props(
	    static_cast<::dds_Publisher*>(self), a_topic, qos, a_listener, mask, props);
	if(raw == nullptr) {
		return {};
	}
	return dds_DataWriter(detail::make_owned<DataWriter>(raw, detail::shared_impl(self), std::shared_ptr<Topic>{}));
}

inline dds_DataWriter dds_Publisher_create_datawriter_w_profile(const dds_Publisher& self,
    const ::dds_Topic* a_topic,
    const char* profile_name,
    const ::dds_DataWriterListener* a_listener,
    ::dds_StatusMask mask) {
	::dds_DataWriter* raw = ::dds_Publisher_create_datawriter_w_profile(
	    static_cast<::dds_Publisher*>(self), a_topic, profile_name, a_listener, mask);
	if(raw == nullptr) {
		return {};
	}
	return dds_DataWriter(detail::make_owned<DataWriter>(raw, detail::shared_impl(self), std::shared_ptr<Topic>{}));
}

inline ::dds_ReturnCode_t dds_Publisher_delete_datawriter(const dds_Publisher& self, dds_DataWriter& a_datawriter) {
	if(!self || !a_datawriter) {
		return detail::bad_parameter();
	}
	if(detail::shared_impl(a_datawriter).use_count() > 1)
		return dds_RETCODE_PRECONDITION_NOT_MET;
	detail::clear_listener(static_cast<::dds_DataWriter*>(a_datawriter));
	::dds_ReturnCode_t ret = ::dds_Publisher_delete_datawriter(static_cast<::dds_Publisher*>(self), a_datawriter);
	if(ret == dds_RETCODE_OK) {
		*detail::shared_impl(a_datawriter) = nullptr;
		a_datawriter = nullptr;
	}
	return ret;
}

// -----------------------------------------------------------------------------
// Subscriber-owned entities
// -----------------------------------------------------------------------------
inline dds_DataReader dds_Subscriber_create_datareader(const dds_Subscriber& self,
    const dds_Topic& a_topic,
    const ::dds_DataReaderQos* qos,
    const ::dds_DataReaderListener* a_listener,
    ::dds_StatusMask mask) {
	::dds_DataReader* raw = ::dds_Subscriber_create_datareader(static_cast<::dds_Subscriber*>(self),
	    detail::topic_description_cast(static_cast<::dds_Topic*>(a_topic)),
	    qos,
	    a_listener,
	    mask);
	if(raw == nullptr) {
		return {};
	}
	return dds_DataReader(detail::make_owned<DataReader>(raw, detail::shared_impl(self), detail::shared_impl(a_topic)));
}

inline dds_DataReader dds_Subscriber_create_datareader(const dds_Subscriber& self,
    const dds_ContentFilteredTopic& a_topic,
    const ::dds_DataReaderQos* qos,
    const ::dds_DataReaderListener* a_listener,
    ::dds_StatusMask mask) {
	::dds_DataReader* raw = ::dds_Subscriber_create_datareader(static_cast<::dds_Subscriber*>(self),
	    detail::topic_description_cast(static_cast<::dds_ContentFilteredTopic*>(a_topic)),
	    qos,
	    a_listener,
	    mask);
	if(raw == nullptr) {
		return {};
	}
	return dds_DataReader(detail::make_owned<DataReader>(raw, detail::shared_impl(self), detail::shared_impl(a_topic)));
}

inline dds_DataReader dds_Subscriber_create_datareader(const dds_Subscriber& self,
    const dds_MultiTopic& a_topic,
    const ::dds_DataReaderQos* qos,
    const ::dds_DataReaderListener* a_listener,
    ::dds_StatusMask mask) {
	::dds_DataReader* raw = ::dds_Subscriber_create_datareader(static_cast<::dds_Subscriber*>(self),
	    detail::topic_description_cast(static_cast<::dds_MultiTopic*>(a_topic)),
	    qos,
	    a_listener,
	    mask);
	if(raw == nullptr) {
		return {};
	}
	return dds_DataReader(detail::make_owned<DataReader>(raw, detail::shared_impl(self), detail::shared_impl(a_topic)));
}

inline dds_DataReader dds_Subscriber_create_datareader_w_profile(const dds_Subscriber& self,
    const dds_Topic& a_topic,
    const char* profile_name,
    const ::dds_DataReaderListener* a_listener,
    ::dds_StatusMask mask) {
	::dds_DataReader* raw = ::dds_Subscriber_create_datareader_w_profile(
	    static_cast<::dds_Subscriber*>(self), static_cast<::dds_Topic*>(a_topic), profile_name, a_listener, mask);
	if(raw == nullptr) {
		return {};
	}
	return dds_DataReader(detail::make_owned<DataReader>(raw, detail::shared_impl(self), detail::shared_impl(a_topic)));
}

// Raw topic descriptions are borrowed; no C++ topic owner is retained.
inline dds_DataReader dds_Subscriber_create_datareader(const dds_Subscriber& self,
    const ::dds_TopicDescription* a_topic,
    const ::dds_DataReaderQos* qos,
    const ::dds_DataReaderListener* a_listener,
    ::dds_StatusMask mask) {
	::dds_DataReader* raw =
	    ::dds_Subscriber_create_datareader(static_cast<::dds_Subscriber*>(self), a_topic, qos, a_listener, mask);
	if(raw == nullptr) {
		return {};
	}
	return dds_DataReader(detail::make_owned<DataReader>(raw, detail::shared_impl(self), std::shared_ptr<void>{}));
}

// Accept the native topic types as well as the common TopicDescription pointer.
// Restrict deduction to native types so existing RAII overloads keep ownership.
template <typename NativeTopic>
inline typename std::enable_if<std::is_same<NativeTopic, ::dds_Topic>::value ||
                                   std::is_same<NativeTopic, ::dds_ContentFilteredTopic>::value ||
                                   std::is_same<NativeTopic, ::dds_MultiTopic>::value,
    dds_DataReader>::type
dds_Subscriber_create_datareader(const dds_Subscriber& self,
    const NativeTopic* a_topic,
    const ::dds_DataReaderQos* qos,
    const ::dds_DataReaderListener* a_listener,
    ::dds_StatusMask mask) {
	return raii::dds_Subscriber_create_datareader(self, detail::topic_description_cast(a_topic), qos, a_listener, mask);
}

inline dds_DataReader dds_Subscriber_create_datareader_w_profile(const dds_Subscriber& self,
    const ::dds_Topic* a_topic,
    const char* profile_name,
    const ::dds_DataReaderListener* a_listener,
    ::dds_StatusMask mask) {
	::dds_DataReader* raw = ::dds_Subscriber_create_datareader_w_profile(
	    static_cast<::dds_Subscriber*>(self), a_topic, profile_name, a_listener, mask);
	if(raw == nullptr) {
		return {};
	}
	return dds_DataReader(detail::make_owned<DataReader>(raw, detail::shared_impl(self), std::shared_ptr<void>{}));
}

inline ::dds_ReturnCode_t dds_Subscriber_delete_datareader(const dds_Subscriber& self, dds_DataReader& a_datareader) {
	if(!self || !a_datareader) {
		return detail::bad_parameter();
	}
	if(detail::shared_impl(a_datareader).use_count() > 1)
		return dds_RETCODE_PRECONDITION_NOT_MET;
	detail::clear_listener(static_cast<::dds_DataReader*>(a_datareader));
	::dds_ReturnCode_t ret = ::dds_Subscriber_delete_datareader(static_cast<::dds_Subscriber*>(self), a_datareader);
	if(ret == dds_RETCODE_OK) {
		*detail::shared_impl(a_datareader) = nullptr;
		a_datareader = nullptr;
	}
	return ret;
}

// -----------------------------------------------------------------------------
// DataReader-owned conditions
// -----------------------------------------------------------------------------
inline dds_ReadCondition dds_DataReader_create_readcondition(const dds_DataReader& self,
    ::dds_SampleStateMask sample_states,
    ::dds_ViewStateMask view_states,
    ::dds_InstanceStateMask instance_states) {
	::dds_ReadCondition* raw = ::dds_DataReader_create_readcondition(
	    static_cast<::dds_DataReader*>(self), sample_states, view_states, instance_states);
	if(raw == nullptr) {
		return {};
	}
	return dds_ReadCondition(detail::make_owned<ReadCondition>(raw, detail::shared_impl(self)));
}

inline dds_QueryCondition dds_DataReader_create_querycondition(const dds_DataReader& self,
    ::dds_SampleStateMask sample_states,
    ::dds_ViewStateMask view_states,
    ::dds_InstanceStateMask instance_states,
    const char* query_expression,
    const ::dds_StringSeq* query_parameters) {
	::dds_QueryCondition* raw = ::dds_DataReader_create_querycondition(static_cast<::dds_DataReader*>(self),
	    sample_states,
	    view_states,
	    instance_states,
	    query_expression,
	    query_parameters);
	if(raw == nullptr) {
		return {};
	}
	return dds_QueryCondition(detail::make_owned<QueryCondition>(raw, detail::shared_impl(self)));
}

inline ::dds_ReturnCode_t dds_DataReader_delete_readcondition(
    const dds_DataReader& self, dds_ReadCondition& a_condition) {
	if(!self || !a_condition) {
		return detail::bad_parameter();
	}
	if(detail::shared_impl(a_condition).use_count() > 1)
		return dds_RETCODE_PRECONDITION_NOT_MET;
	::dds_ReturnCode_t ret = ::dds_DataReader_delete_readcondition(static_cast<::dds_DataReader*>(self), a_condition);
	if(ret == dds_RETCODE_OK) {
		*detail::shared_impl(a_condition) = nullptr;
		a_condition = nullptr;
	}
	return ret;
}

inline ::dds_ReturnCode_t dds_DataReader_delete_readcondition(
    const dds_DataReader& self, dds_QueryCondition& a_condition) {
	if(!self || !a_condition) {
		return detail::bad_parameter();
	}
	if(detail::shared_impl(a_condition).use_count() > 1)
		return dds_RETCODE_PRECONDITION_NOT_MET;
	::dds_ReturnCode_t ret = ::dds_DataReader_delete_readcondition(static_cast<::dds_DataReader*>(self),
	    reinterpret_cast<::dds_ReadCondition*>(static_cast<::dds_QueryCondition*>(a_condition)));
	if(ret == dds_RETCODE_OK) {
		*detail::shared_impl(a_condition) = nullptr;
		a_condition = nullptr;
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
	return dds_WaitSet(detail::make_owned<WaitSet>(raw));
}

inline ::dds_ReturnCode_t dds_WaitSet_delete(dds_WaitSet& self) {
	if(!self) {
		return detail::bad_parameter();
	}
	::dds_ReturnCode_t ret = ::dds_WaitSet_delete(self);
	if(ret == dds_RETCODE_OK) {
		*detail::shared_impl(self) = nullptr;
		detail::shared_impl(self)->clear_attachments();
		self = nullptr;
	}
	return ret;
}

inline dds_GuardCondition dds_GuardCondition_create() {
	::dds_GuardCondition* raw = ::dds_GuardCondition_create();
	if(raw == nullptr) {
		return {};
	}
	return dds_GuardCondition(detail::make_owned<GuardCondition>(raw));
}

inline void dds_GuardCondition_delete(dds_GuardCondition& self) noexcept {
	if(!self) {
		return;
	}
	if(detail::shared_impl(self).use_count() > 1) {
		self = nullptr;
		return;
	}
	::dds_GuardCondition_delete(self);
	*detail::shared_impl(self) = nullptr;
	self = nullptr;
}

inline ::dds_ReturnCode_t dds_WaitSet_attach_condition(const dds_WaitSet& self, const dds_GuardCondition& cond) {
	if(!self || !cond) {
		return detail::bad_parameter();
	}
	return detail::shared_impl(self)->attach(cond);
}

inline ::dds_ReturnCode_t dds_WaitSet_attach_condition(const dds_WaitSet& self, const dds_ReadCondition& cond) {
	if(!self || !cond) {
		return detail::bad_parameter();
	}
	return detail::shared_impl(self)->attach(cond);
}

inline ::dds_ReturnCode_t dds_WaitSet_attach_condition(const dds_WaitSet& self, const dds_QueryCondition& cond) {
	if(!self || !cond) {
		return detail::bad_parameter();
	}
	return detail::shared_impl(self)->attach(cond);
}

inline ::dds_ReturnCode_t dds_WaitSet_detach_condition(const dds_WaitSet& self, const dds_GuardCondition& cond) {
	if(!self || !cond) {
		return detail::bad_parameter();
	}
	return detail::shared_impl(self)->detach(cond);
}

inline ::dds_ReturnCode_t dds_WaitSet_detach_condition(const dds_WaitSet& self, const dds_ReadCondition& cond) {
	if(!self || !cond) {
		return detail::bad_parameter();
	}
	return detail::shared_impl(self)->detach(cond);
}

inline ::dds_ReturnCode_t dds_WaitSet_detach_condition(const dds_WaitSet& self, const dds_QueryCondition& cond) {
	if(!self || !cond) {
		return detail::bad_parameter();
	}
	return detail::shared_impl(self)->detach(cond);
}

// Move-only owners for sequences, TypeSupport, samples, and dynamic types.
// Deleters return the native return code. Destructor failures terminate, matching
// the entity wrappers; use close()/the explicit delete wrapper to inspect errors.
template <typename T, typename Deleter>
class Resource {
	struct State {
		T* raw = nullptr;
		Deleter deleter;
		std::function<::dds_ReturnCode_t()> before_delete;
		const void* loan_identity = nullptr;
		const void* loan_reader = nullptr;

		explicit State(Deleter d)
		    : deleter(std::move(d)) {
		}

		~State() noexcept {
			if(raw) {
				if(before_delete)
					detail::terminate_on_error(before_delete(), "return sequence loan");
				detail::terminate_on_error(deleter(raw), "RAII resource cleanup");
			}
		}
	};

	std::shared_ptr<State> state_;

public:
	using native_type = T;
	Resource() noexcept = default;
	Resource(const Resource&) = delete;
	Resource& operator=(const Resource&) = delete;
	Resource(Resource&&) noexcept = default;
	Resource& operator=(Resource&&) noexcept = default;

	// Adopts an owning C pointer, never a borrowed get/lookup result.
	Resource(T* raw, Deleter deleter = Deleter()) {
		if(!raw)
			return;
		try {
			state_ = std::make_shared<State>(deleter);
		} catch(...) {
			detail::terminate_on_error(deleter(raw), "adoption cleanup");
			throw;
		}
		state_->raw = raw;
	}

	Resource& operator=(T* raw) {
		if(get() != raw) {
			Resource next(raw);
			*this = std::move(next);
		}
		return *this;
	}

	Resource& operator=(std::nullptr_t) noexcept {
		state_.reset();
		return *this;
	}

	T* get() const noexcept {
		return state_ ? state_->raw : nullptr;
	}

	operator T*() const noexcept {
		return get();
	}

	T* operator->() const noexcept {
		return get();
	}

	T* operator*() const noexcept {
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
		return state_;
	}

	const Deleter* deleter() const noexcept {
		return state_ ? &state_->deleter : nullptr;
	}

	void reset() noexcept {
		state_.reset();
	}

	::dds_ReturnCode_t finish_loan() noexcept {
		if(!state_ || !state_->before_delete)
			return dds_RETCODE_OK;
		const auto ret = state_->before_delete();
		if(ret == dds_RETCODE_OK) {
			state_->before_delete = {};
			state_->loan_identity = nullptr;
			state_->loan_reader = nullptr;
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
		state_->before_delete = std::move(cleanup);
		state_->loan_identity = identity;
		state_->loan_reader = reader;
		return dds_RETCODE_OK;
	}

	const void* loan_identity() const noexcept {
		return state_ ? state_->loan_identity : nullptr;
	}

	const void* loan_reader() const noexcept {
		return state_ ? state_->loan_reader : nullptr;
	}

	T* release() {
		if(!state_)
			return nullptr;
		if(state_->before_delete)
			throw std::logic_error("Return the sequence loan before release()");
		if(state_.use_count() != 1)
			throw std::logic_error("DDS resource is still in use");
		T* raw = state_->raw;
		state_->raw = nullptr;
		state_.reset();
		return raw;
	}

	::dds_ReturnCode_t close() noexcept {
		if(!get())
			return dds_RETCODE_OK;
		if(state_.use_count() != 1)
			return dds_RETCODE_PRECONDITION_NOT_MET;
		const auto loan_ret = finish_loan();
		if(loan_ret != dds_RETCODE_OK)
			return loan_ret;
		const auto ret = state_->deleter(state_->raw);
		if(ret == dds_RETCODE_OK) {
			state_->raw = nullptr;
			state_.reset();
		}
		return ret;
	}

	template <typename Allocate>
	static Resource create(Allocate allocate, Deleter deleter = Deleter()) {
		Resource result;
		result.state_ = std::make_shared<State>(std::move(deleter));
		result.state_->raw = allocate(); // No throwing C++ work after allocation.
		if(!result.state_->raw)
			result.state_.reset();
		return result;
	}
};

// Expand at global scope after including the generated TypeSupport header.
// Only sample allocation, deserialization and release are wrapped. The generated
// TypeSupport singleton remains borrowed and the generated C files stay intact.
#define RAII_TYPESUPPORT_DEFINE(TypeSupport, Type)                                    \
    namespace raii {                                                                \
    struct TypeSupport##_sample_deleter {                                            \
        ::dds_ReturnCode_t operator()(::Type* raw) const noexcept {                   \
            ::TypeSupport##_free(raw);                                              \
            return dds_RETCODE_OK;                                                  \
        }                                                                           \
    };                                                                              \
    using Type = ::raii::Resource<::Type, TypeSupport##_sample_deleter>;               \
    inline Type TypeSupport##_alloc() {                                             \
        return Type::create([]() { return ::TypeSupport##_alloc(); });               \
    }                                                                               \
    inline void TypeSupport##_free(Type& sample) noexcept {                          \
        sample.reset();                                                             \
    }                                                                               \
    inline Type TypeSupport##_deserialize(void* input, size_t input_size) {          \
        return Type::create([&]() {                                                 \
            return ::TypeSupport##_deserialize(input, input_size);                   \
        });                                                                         \
    }                                                                               \
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
		    : qos_entity_(other.qos_entity_) {                                                                         \
			other.qos_entity_ = native_type{};                                                                         \
		}                                                                                                              \
		qos_type& operator=(qos_type&& other) noexcept {                                                               \
			if(this != &other) {                                                                                       \
				detail::terminate_on_error(::qos_type##_finalize(&qos_entity_), "move assignment cleanup");            \
				qos_entity_ = other.qos_entity_;                                                                       \
				other.qos_entity_ = native_type{};                                                                     \
			}                                                                                                          \
			return *this;                                                                                              \
		}                                                                                                              \
                                                                                                                       \
		operator native_type*() noexcept {                                                                             \
			return &qos_entity_;                                                                                       \
		}                                                                                                              \
                                                                                                                       \
		operator const native_type*() const noexcept {                                                                 \
			return &qos_entity_;                                                                                       \
		}                                                                                                              \
                                                                                                                       \
		native_type* operator->() noexcept {                                                                           \
			return &qos_entity_;                                                                                       \
		}                                                                                                              \
                                                                                                                       \
		const native_type* operator->() const noexcept {                                                               \
			return &qos_entity_;                                                                                       \
		}                                                                                                              \
                                                                                                                       \
		~qos_type() noexcept {                                                                                         \
			(void)::qos_type##_finalize(&qos_entity_);                                                                 \
		}                                                                                                              \
                                                                                                                       \
	private:                                                                                                           \
		native_type qos_entity_{};                                                                                     \
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
    ::dds_Publisher* self, dds_DataWriterQos& datawriter_qos, const dds_TopicQos& topic_qos) noexcept {
	return ::dds_Publisher_copy_from_topic_qos(self, datawriter_qos, topic_qos);
}

inline ::dds_ReturnCode_t dds_Publisher_copy_from_topic_qos(
    ::dds_Publisher* self, dds_DataWriterQos& datawriter_qos, const ::dds_TopicQos* topic_qos) noexcept {
	if(topic_qos == nullptr) {
		return dds_RETCODE_BAD_PARAMETER;
	}

	return ::dds_Publisher_copy_from_topic_qos(self, datawriter_qos, topic_qos);
}

inline ::dds_ReturnCode_t dds_Subscriber_copy_from_topic_qos(
    ::dds_Subscriber* self, dds_DataReaderQos& datareader_qos, const dds_TopicQos& topic_qos) noexcept {
	return ::dds_Subscriber_copy_from_topic_qos(self, datareader_qos, topic_qos);
}

inline ::dds_ReturnCode_t dds_Subscriber_copy_from_topic_qos(
    ::dds_Subscriber* self, dds_DataReaderQos& datareader_qos, const ::dds_TopicQos* topic_qos) noexcept {
	if(topic_qos == nullptr) {
		return dds_RETCODE_BAD_PARAMETER;
	}

	return ::dds_Subscriber_copy_from_topic_qos(self, datareader_qos, topic_qos);
}

inline ::dds_ReturnCode_t dds_DomainParticipantFactory_get_participant_qos_from_profile(
    const char* profile_name, dds_DomainParticipantQos& qos) noexcept {
	(void)::dds_DomainParticipantQos_finalize(qos);
	return ::dds_DomainParticipantFactory_get_participant_qos_from_profile(profile_name, qos);
}

inline ::dds_ReturnCode_t dds_DomainParticipantFactory_get_datawriter_qos_from_profile(
    const char* profile_name, dds_DataWriterQos& qos) noexcept {
	(void)::dds_DataWriterQos_finalize(qos);
	return ::dds_DomainParticipantFactory_get_datawriter_qos_from_profile(profile_name, qos);
}

inline ::dds_ReturnCode_t dds_DomainParticipantFactory_get_datareader_qos_from_profile(
    const char* profile_name, dds_DataReaderQos& qos) noexcept {
	(void)::dds_DataReaderQos_finalize(qos);
	return ::dds_DomainParticipantFactory_get_datareader_qos_from_profile(profile_name, qos);
}

#define RAII_DESCRIPTOR_DEFINE(type)                                                                                   \
	class type {                                                                                                       \
		::type value_{};                                                                                               \
                                                                                                                       \
	public:                                                                                                            \
		using native_type = ::type;                                                                                    \
		type() noexcept = default;                                                                                     \
		type(const type&) = delete;                                                                                    \
		type& operator=(const type&) = delete;                                                                         \
		/* Transfer ownership; leave the source safe to finalize or reuse. */                                          \
		type(type&& other) noexcept                                                                                    \
		    : value_(other.value_) {                                                                                   \
			other.value_ = native_type{};                                                                              \
		}                                                                                                              \
		type& operator=(type&& other) noexcept {                                                                       \
			if(this != &other) {                                                                                       \
				detail::terminate_on_error(::type##_finalize(&value_), "move assignment cleanup");                     \
				value_ = other.value_;                                                                                 \
				other.value_ = native_type{};                                                                          \
			}                                                                                                          \
			return *this;                                                                                              \
		}                                                                                                              \
		operator ::type*() noexcept {                                                                                  \
			return &value_;                                                                                            \
		}                                                                                                              \
		operator const ::type*() const noexcept {                                                                      \
			return &value_;                                                                                            \
		}                                                                                                              \
		::type* operator->() noexcept {                                                                                \
			return &value_;                                                                                            \
		}                                                                                                              \
		const ::type* operator->() const noexcept {                                                                    \
			return &value_;                                                                                            \
		}                                                                                                              \
		~type() noexcept {                                                                                             \
			(void)::type##_finalize(&value_);                                                                          \
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

inline dds_TypeSupport dds_TypeSupport_create(const char* meta_str) {
	return dds_TypeSupport::create([&]() { return ::dds_TypeSupport_create(meta_str); });
}

inline dds_TypeSupport dds_TypeSupport_create2(const char** meta_str_array, uint32_t meta_str_array_len) {
	return dds_TypeSupport::create([&]() { return ::dds_TypeSupport_create2(meta_str_array, meta_str_array_len); });
}

inline dds_TypeSupport dds_TypeSupport_early_create(const char* meta_str) {
	return dds_TypeSupport::create([&]() { return ::dds_TypeSupport_early_create(meta_str); });
}

inline dds_TypeSupport dds_TypeSupport_early_create2(const char** meta_str_array, uint32_t meta_str_array_len) {
	return dds_TypeSupport::create(
	    [&]() { return ::dds_TypeSupport_early_create2(meta_str_array, meta_str_array_len); });
}

inline dds_Memory dds_TypeSupport_serialize(::dds_TypeSupport* self, void* data, size_t* output_size) {
	return dds_Memory::create([&]() { return ::dds_TypeSupport_serialize(self, data, output_size); });
}

inline dds_Memory dds_TypeSupport_serialize_w_repr_id(
    ::dds_TypeSupport* self, void* data, size_t* output_size, dds_DataRepresentationId_t repr_id) {
	return dds_Memory::create(
	    [&]() { return ::dds_TypeSupport_serialize_w_repr_id(self, data, output_size, repr_id); });
}

inline dds_Memory dds_StringTypeSupport_serialize(::dds_String data, size_t* output_size) {
	return dds_Memory::create([&]() { return ::dds_StringTypeSupport_serialize(data, output_size); });
}

inline dds_KeyedString dds_KeyedStringTypeSupport_alloc() {
	return dds_KeyedString::create([&]() { return ::dds_KeyedStringTypeSupport_alloc(); });
}

inline dds_Memory dds_KeyedStringTypeSupport_serialize(::dds_KeyedString* data, size_t* output_size) {
	return dds_Memory::create([&]() { return ::dds_KeyedStringTypeSupport_serialize(data, output_size); });
}

inline dds_KeyedString dds_KeyedStringTypeSupport_deserialize(void* input, size_t input_size) {
	return dds_KeyedString::create([&]() { return ::dds_KeyedStringTypeSupport_deserialize(input, input_size); });
}

inline dds_Bytes dds_BytesTypeSupport_alloc() {
	return dds_Bytes::create([&]() { return ::dds_BytesTypeSupport_alloc(); });
}

inline dds_Memory dds_BytesTypeSupport_serialize(::dds_Bytes* data, size_t* output_size) {
	return dds_Memory::create([&]() { return ::dds_BytesTypeSupport_serialize(data, output_size); });
}

inline dds_Bytes dds_BytesTypeSupport_deserialize(void* input, size_t input_size) {
	return dds_Bytes::create([&]() { return ::dds_BytesTypeSupport_deserialize(input, input_size); });
}

inline dds_KeyedBytes dds_KeyedBytesTypeSupport_alloc() {
	return dds_KeyedBytes::create([&]() { return ::dds_KeyedBytesTypeSupport_alloc(); });
}

inline dds_Memory dds_KeyedBytesTypeSupport_serialize(::dds_KeyedBytes* data, size_t* output_size) {
	return dds_Memory::create([&]() { return ::dds_KeyedBytesTypeSupport_serialize(data, output_size); });
}

inline dds_KeyedBytes dds_KeyedBytesTypeSupport_deserialize(void* input, size_t input_size) {
	return dds_KeyedBytes::create([&]() { return ::dds_KeyedBytesTypeSupport_deserialize(input, input_size); });
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

inline dds_TypeSupport dds_TypeSupport_raw_create_w_typehash(void* typehash, bool has_key) {
	return dds_TypeSupport::create([&]() { return ::dds_TypeSupport_raw_create_w_typehash(typehash, has_key); });
}

inline dds_Memory dds_TypeSupport_serialize_be(::dds_TypeSupport* self, void* data, size_t* output_size) {
	return dds_Memory::create([&]() { return ::dds_TypeSupport_serialize_be(self, data, output_size); });
}

inline dds_Memory dds_TypeSupport_serialize_le(::dds_TypeSupport* self, void* data, size_t* output_size) {
	return dds_Memory::create([&]() { return ::dds_TypeSupport_serialize_le(self, data, output_size); });
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

inline dds_TypeSupportData dds_TypeSupport_deserialize(const dds_TypeSupport& self, void* input, size_t input_size) {
	dds_TypeSupportData_deleter cleanup;
	cleanup.support = self.get();
	cleanup.support_owner = self.owner();
	cleanup.initialized = true;
	return dds_TypeSupportData::create(
	    [&]() { return ::dds_TypeSupport_deserialize(self, input, input_size); }, cleanup);
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
    dds_DataSeq& data_values,
    dds_SampleInfoSeq& sample_infos,
    int32_t max_samples,
    ::dds_SampleStateMask sample_states,
    ::dds_ViewStateMask view_states,
    ::dds_InstanceStateMask instance_states) {
	if(!self || !data_values || !sample_infos)
		return dds_RETCODE_BAD_PARAMETER;
	auto loan = std::make_shared<detail::SequenceLoan>();
	loan->reader = detail::shared_impl(self);
	loan->data = data_values;
	loan->infos = sample_infos;
	std::function<::dds_ReturnCode_t()> cleanup = [loan]() { return loan->finish(); };
	auto ret = data_values.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = sample_infos.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = ::dds_DataReader_read(
	    self, data_values, sample_infos, max_samples, sample_states, view_states, instance_states);
	loan->active = (ret == dds_RETCODE_OK);
	if(!loan->active)
		loan->reader.reset();
	return ret;
}

inline ::dds_ReturnCode_t dds_DataReader_take(const dds_DataReader& self,
    dds_DataSeq& data_values,
    dds_SampleInfoSeq& sample_infos,
    int32_t max_samples,
    ::dds_SampleStateMask sample_states,
    ::dds_ViewStateMask view_states,
    ::dds_InstanceStateMask instance_states) {
	if(!self || !data_values || !sample_infos)
		return dds_RETCODE_BAD_PARAMETER;
	auto loan = std::make_shared<detail::SequenceLoan>();
	loan->reader = detail::shared_impl(self);
	loan->data = data_values;
	loan->infos = sample_infos;
	std::function<::dds_ReturnCode_t()> cleanup = [loan]() { return loan->finish(); };
	auto ret = data_values.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = sample_infos.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = ::dds_DataReader_take(
	    self, data_values, sample_infos, max_samples, sample_states, view_states, instance_states);
	loan->active = (ret == dds_RETCODE_OK);
	if(!loan->active)
		loan->reader.reset();
	return ret;
}

inline ::dds_ReturnCode_t dds_DataReader_read_w_condition(const dds_DataReader& self,
    dds_DataSeq& data_values,
    dds_SampleInfoSeq& sample_infos,
    int32_t max_samples,
    const ::dds_ReadCondition* a_condition) {
	if(!self || !data_values || !sample_infos)
		return dds_RETCODE_BAD_PARAMETER;
	auto loan = std::make_shared<detail::SequenceLoan>();
	loan->reader = detail::shared_impl(self);
	loan->data = data_values;
	loan->infos = sample_infos;
	std::function<::dds_ReturnCode_t()> cleanup = [loan]() { return loan->finish(); };
	auto ret = data_values.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = sample_infos.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = ::dds_DataReader_read_w_condition(self, data_values, sample_infos, max_samples, a_condition);
	loan->active = (ret == dds_RETCODE_OK);
	if(!loan->active)
		loan->reader.reset();
	return ret;
}

inline ::dds_ReturnCode_t dds_DataReader_take_w_condition(const dds_DataReader& self,
    dds_DataSeq& data_values,
    dds_SampleInfoSeq& sample_infos,
    int32_t max_samples,
    const ::dds_ReadCondition* a_condition) {
	if(!self || !data_values || !sample_infos)
		return dds_RETCODE_BAD_PARAMETER;
	auto loan = std::make_shared<detail::SequenceLoan>();
	loan->reader = detail::shared_impl(self);
	loan->data = data_values;
	loan->infos = sample_infos;
	std::function<::dds_ReturnCode_t()> cleanup = [loan]() { return loan->finish(); };
	auto ret = data_values.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = sample_infos.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = ::dds_DataReader_take_w_condition(self, data_values, sample_infos, max_samples, a_condition);
	loan->active = (ret == dds_RETCODE_OK);
	if(!loan->active)
		loan->reader.reset();
	return ret;
}

inline ::dds_ReturnCode_t dds_DataReader_read_instance(const dds_DataReader& self,
    dds_DataSeq& data_values,
    dds_SampleInfoSeq& sample_infos,
    int32_t max_samples,
    ::dds_InstanceHandle_t a_handle,
    ::dds_SampleStateMask sample_states,
    ::dds_ViewStateMask view_states,
    ::dds_InstanceStateMask instance_states) {
	if(!self || !data_values || !sample_infos)
		return dds_RETCODE_BAD_PARAMETER;
	auto loan = std::make_shared<detail::SequenceLoan>();
	loan->reader = detail::shared_impl(self);
	loan->data = data_values;
	loan->infos = sample_infos;
	std::function<::dds_ReturnCode_t()> cleanup = [loan]() { return loan->finish(); };
	auto ret = data_values.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = sample_infos.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = ::dds_DataReader_read_instance(
	    self, data_values, sample_infos, max_samples, a_handle, sample_states, view_states, instance_states);
	loan->active = (ret == dds_RETCODE_OK);
	if(!loan->active)
		loan->reader.reset();
	return ret;
}

inline ::dds_ReturnCode_t dds_DataReader_take_instance(const dds_DataReader& self,
    dds_DataSeq& data_values,
    dds_SampleInfoSeq& sample_infos,
    int32_t max_samples,
    ::dds_InstanceHandle_t a_handle,
    ::dds_SampleStateMask sample_states,
    ::dds_ViewStateMask view_states,
    ::dds_InstanceStateMask instance_states) {
	if(!self || !data_values || !sample_infos)
		return dds_RETCODE_BAD_PARAMETER;
	auto loan = std::make_shared<detail::SequenceLoan>();
	loan->reader = detail::shared_impl(self);
	loan->data = data_values;
	loan->infos = sample_infos;
	std::function<::dds_ReturnCode_t()> cleanup = [loan]() { return loan->finish(); };
	auto ret = data_values.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = sample_infos.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = ::dds_DataReader_take_instance(
	    self, data_values, sample_infos, max_samples, a_handle, sample_states, view_states, instance_states);
	loan->active = (ret == dds_RETCODE_OK);
	if(!loan->active)
		loan->reader.reset();
	return ret;
}

inline ::dds_ReturnCode_t dds_DataReader_read_next_instance(const dds_DataReader& self,
    dds_DataSeq& data_values,
    dds_SampleInfoSeq& sample_infos,
    int32_t max_samples,
    ::dds_InstanceHandle_t previous_handle,
    ::dds_SampleStateMask sample_states,
    ::dds_ViewStateMask view_states,
    ::dds_InstanceStateMask instance_states) {
	if(!self || !data_values || !sample_infos)
		return dds_RETCODE_BAD_PARAMETER;
	auto loan = std::make_shared<detail::SequenceLoan>();
	loan->reader = detail::shared_impl(self);
	loan->data = data_values;
	loan->infos = sample_infos;
	std::function<::dds_ReturnCode_t()> cleanup = [loan]() { return loan->finish(); };
	auto ret = data_values.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = sample_infos.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = ::dds_DataReader_read_next_instance(
	    self, data_values, sample_infos, max_samples, previous_handle, sample_states, view_states, instance_states);
	loan->active = (ret == dds_RETCODE_OK);
	if(!loan->active)
		loan->reader.reset();
	return ret;
}

inline ::dds_ReturnCode_t dds_DataReader_take_next_instance(const dds_DataReader& self,
    dds_DataSeq& data_values,
    dds_SampleInfoSeq& sample_infos,
    int32_t max_samples,
    ::dds_InstanceHandle_t previous_handle,
    ::dds_SampleStateMask sample_states,
    ::dds_ViewStateMask view_states,
    ::dds_InstanceStateMask instance_states) {
	if(!self || !data_values || !sample_infos)
		return dds_RETCODE_BAD_PARAMETER;
	auto loan = std::make_shared<detail::SequenceLoan>();
	loan->reader = detail::shared_impl(self);
	loan->data = data_values;
	loan->infos = sample_infos;
	std::function<::dds_ReturnCode_t()> cleanup = [loan]() { return loan->finish(); };
	auto ret = data_values.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = sample_infos.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = ::dds_DataReader_take_next_instance(
	    self, data_values, sample_infos, max_samples, previous_handle, sample_states, view_states, instance_states);
	loan->active = (ret == dds_RETCODE_OK);
	if(!loan->active)
		loan->reader.reset();
	return ret;
}

inline ::dds_ReturnCode_t dds_DataReader_read_next_instance_w_condition(const dds_DataReader& self,
    dds_DataSeq& data_values,
    dds_SampleInfoSeq& sample_infos,
    int32_t max_samples,
    ::dds_InstanceHandle_t previous_handle,
    const ::dds_ReadCondition* a_condition) {
	if(!self || !data_values || !sample_infos)
		return dds_RETCODE_BAD_PARAMETER;
	auto loan = std::make_shared<detail::SequenceLoan>();
	loan->reader = detail::shared_impl(self);
	loan->data = data_values;
	loan->infos = sample_infos;
	std::function<::dds_ReturnCode_t()> cleanup = [loan]() { return loan->finish(); };
	auto ret = data_values.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = sample_infos.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = ::dds_DataReader_read_next_instance_w_condition(
	    self, data_values, sample_infos, max_samples, previous_handle, a_condition);
	loan->active = (ret == dds_RETCODE_OK);
	if(!loan->active)
		loan->reader.reset();
	return ret;
}

inline ::dds_ReturnCode_t dds_DataReader_take_next_instance_w_condition(const dds_DataReader& self,
    dds_DataSeq& data_values,
    dds_SampleInfoSeq& sample_infos,
    int32_t max_samples,
    ::dds_InstanceHandle_t previous_handle,
    const ::dds_ReadCondition* a_condition) {
	if(!self || !data_values || !sample_infos)
		return dds_RETCODE_BAD_PARAMETER;
	auto loan = std::make_shared<detail::SequenceLoan>();
	loan->reader = detail::shared_impl(self);
	loan->data = data_values;
	loan->infos = sample_infos;
	std::function<::dds_ReturnCode_t()> cleanup = [loan]() { return loan->finish(); };
	auto ret = data_values.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = sample_infos.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = ::dds_DataReader_take_next_instance_w_condition(
	    self, data_values, sample_infos, max_samples, previous_handle, a_condition);
	loan->active = (ret == dds_RETCODE_OK);
	if(!loan->active)
		loan->reader.reset();
	return ret;
}

inline ::dds_ReturnCode_t dds_DynamicDataReader_read(const dds_DataReader& self,
    dds_DynamicDataSeq& data_values,
    dds_SampleInfoSeq& sample_infos,
    int32_t max_samples,
    ::dds_SampleStateMask sample_states,
    ::dds_ViewStateMask view_states,
    ::dds_InstanceStateMask instance_states) {
	if(!self || !data_values || !sample_infos)
		return dds_RETCODE_BAD_PARAMETER;
	auto loan = std::make_shared<detail::SequenceLoan>();
	loan->reader = detail::shared_impl(self);
	loan->dynamic_data = data_values;
	loan->infos = sample_infos;
	std::function<::dds_ReturnCode_t()> cleanup = [loan]() { return loan->finish(); };
	auto ret = data_values.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = sample_infos.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = ::dds_DynamicDataReader_read(
	    self, data_values, sample_infos, max_samples, sample_states, view_states, instance_states);
	loan->active = (ret == dds_RETCODE_OK);
	if(!loan->active)
		loan->reader.reset();
	return ret;
}

inline ::dds_ReturnCode_t dds_DynamicDataReader_take(const dds_DataReader& self,
    dds_DynamicDataSeq& data_values,
    dds_SampleInfoSeq& sample_infos,
    int32_t max_samples,
    ::dds_SampleStateMask sample_states,
    ::dds_ViewStateMask view_states,
    ::dds_InstanceStateMask instance_states) {
	if(!self || !data_values || !sample_infos)
		return dds_RETCODE_BAD_PARAMETER;
	auto loan = std::make_shared<detail::SequenceLoan>();
	loan->reader = detail::shared_impl(self);
	loan->dynamic_data = data_values;
	loan->infos = sample_infos;
	std::function<::dds_ReturnCode_t()> cleanup = [loan]() { return loan->finish(); };
	auto ret = data_values.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = sample_infos.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = ::dds_DynamicDataReader_take(
	    self, data_values, sample_infos, max_samples, sample_states, view_states, instance_states);
	loan->active = (ret == dds_RETCODE_OK);
	if(!loan->active)
		loan->reader.reset();
	return ret;
}

inline ::dds_ReturnCode_t dds_DynamicDataReader_read_w_condition(const dds_DataReader& self,
    dds_DynamicDataSeq& data_values,
    dds_SampleInfoSeq& sample_infos,
    int32_t max_samples,
    const ::dds_ReadCondition* a_condition) {
	if(!self || !data_values || !sample_infos)
		return dds_RETCODE_BAD_PARAMETER;
	auto loan = std::make_shared<detail::SequenceLoan>();
	loan->reader = detail::shared_impl(self);
	loan->dynamic_data = data_values;
	loan->infos = sample_infos;
	std::function<::dds_ReturnCode_t()> cleanup = [loan]() { return loan->finish(); };
	auto ret = data_values.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = sample_infos.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = ::dds_DynamicDataReader_read_w_condition(self, data_values, sample_infos, max_samples, a_condition);
	loan->active = (ret == dds_RETCODE_OK);
	if(!loan->active)
		loan->reader.reset();
	return ret;
}

inline ::dds_ReturnCode_t dds_DynamicDataReader_take_w_condition(const dds_DataReader& self,
    dds_DynamicDataSeq& data_values,
    dds_SampleInfoSeq& sample_infos,
    int32_t max_samples,
    const ::dds_ReadCondition* a_condition) {
	if(!self || !data_values || !sample_infos)
		return dds_RETCODE_BAD_PARAMETER;
	auto loan = std::make_shared<detail::SequenceLoan>();
	loan->reader = detail::shared_impl(self);
	loan->dynamic_data = data_values;
	loan->infos = sample_infos;
	std::function<::dds_ReturnCode_t()> cleanup = [loan]() { return loan->finish(); };
	auto ret = data_values.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = sample_infos.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = ::dds_DynamicDataReader_take_w_condition(self, data_values, sample_infos, max_samples, a_condition);
	loan->active = (ret == dds_RETCODE_OK);
	if(!loan->active)
		loan->reader.reset();
	return ret;
}

inline ::dds_ReturnCode_t dds_DynamicDataReader_read_instance(const dds_DataReader& self,
    dds_DynamicDataSeq& data_values,
    dds_SampleInfoSeq& sample_infos,
    int32_t max_samples,
    ::dds_InstanceHandle_t a_handle,
    ::dds_SampleStateMask sample_states,
    ::dds_ViewStateMask view_states,
    ::dds_InstanceStateMask instance_states) {
	if(!self || !data_values || !sample_infos)
		return dds_RETCODE_BAD_PARAMETER;
	auto loan = std::make_shared<detail::SequenceLoan>();
	loan->reader = detail::shared_impl(self);
	loan->dynamic_data = data_values;
	loan->infos = sample_infos;
	std::function<::dds_ReturnCode_t()> cleanup = [loan]() { return loan->finish(); };
	auto ret = data_values.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = sample_infos.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = ::dds_DynamicDataReader_read_instance(
	    self, data_values, sample_infos, max_samples, a_handle, sample_states, view_states, instance_states);
	loan->active = (ret == dds_RETCODE_OK);
	if(!loan->active)
		loan->reader.reset();
	return ret;
}

inline ::dds_ReturnCode_t dds_DynamicDataReader_take_instance(const dds_DataReader& self,
    dds_DynamicDataSeq& data_values,
    dds_SampleInfoSeq& sample_infos,
    int32_t max_samples,
    ::dds_InstanceHandle_t a_handle,
    ::dds_SampleStateMask sample_states,
    ::dds_ViewStateMask view_states,
    ::dds_InstanceStateMask instance_states) {
	if(!self || !data_values || !sample_infos)
		return dds_RETCODE_BAD_PARAMETER;
	auto loan = std::make_shared<detail::SequenceLoan>();
	loan->reader = detail::shared_impl(self);
	loan->dynamic_data = data_values;
	loan->infos = sample_infos;
	std::function<::dds_ReturnCode_t()> cleanup = [loan]() { return loan->finish(); };
	auto ret = data_values.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = sample_infos.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = ::dds_DynamicDataReader_take_instance(
	    self, data_values, sample_infos, max_samples, a_handle, sample_states, view_states, instance_states);
	loan->active = (ret == dds_RETCODE_OK);
	if(!loan->active)
		loan->reader.reset();
	return ret;
}

inline ::dds_ReturnCode_t dds_DynamicDataReader_read_next_instance(const dds_DataReader& self,
    dds_DynamicDataSeq& data_values,
    dds_SampleInfoSeq& sample_infos,
    int32_t max_samples,
    ::dds_InstanceHandle_t previous_handle,
    ::dds_SampleStateMask sample_states,
    ::dds_ViewStateMask view_states,
    ::dds_InstanceStateMask instance_states) {
	if(!self || !data_values || !sample_infos)
		return dds_RETCODE_BAD_PARAMETER;
	auto loan = std::make_shared<detail::SequenceLoan>();
	loan->reader = detail::shared_impl(self);
	loan->dynamic_data = data_values;
	loan->infos = sample_infos;
	std::function<::dds_ReturnCode_t()> cleanup = [loan]() { return loan->finish(); };
	auto ret = data_values.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = sample_infos.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = ::dds_DynamicDataReader_read_next_instance(
	    self, data_values, sample_infos, max_samples, previous_handle, sample_states, view_states, instance_states);
	loan->active = (ret == dds_RETCODE_OK);
	if(!loan->active)
		loan->reader.reset();
	return ret;
}

inline ::dds_ReturnCode_t dds_DynamicDataReader_take_next_instance(const dds_DataReader& self,
    dds_DynamicDataSeq& data_values,
    dds_SampleInfoSeq& sample_infos,
    int32_t max_samples,
    ::dds_InstanceHandle_t previous_handle,
    ::dds_SampleStateMask sample_states,
    ::dds_ViewStateMask view_states,
    ::dds_InstanceStateMask instance_states) {
	if(!self || !data_values || !sample_infos)
		return dds_RETCODE_BAD_PARAMETER;
	auto loan = std::make_shared<detail::SequenceLoan>();
	loan->reader = detail::shared_impl(self);
	loan->dynamic_data = data_values;
	loan->infos = sample_infos;
	std::function<::dds_ReturnCode_t()> cleanup = [loan]() { return loan->finish(); };
	auto ret = data_values.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = sample_infos.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = ::dds_DynamicDataReader_take_next_instance(
	    self, data_values, sample_infos, max_samples, previous_handle, sample_states, view_states, instance_states);
	loan->active = (ret == dds_RETCODE_OK);
	if(!loan->active)
		loan->reader.reset();
	return ret;
}

inline ::dds_ReturnCode_t dds_DynamicDataReader_read_next_instance_w_condition(const dds_DataReader& self,
    dds_DynamicDataSeq& data_values,
    dds_SampleInfoSeq& sample_infos,
    int32_t max_samples,
    ::dds_InstanceHandle_t previous_handle,
    const ::dds_ReadCondition* a_condition) {
	if(!self || !data_values || !sample_infos)
		return dds_RETCODE_BAD_PARAMETER;
	auto loan = std::make_shared<detail::SequenceLoan>();
	loan->reader = detail::shared_impl(self);
	loan->dynamic_data = data_values;
	loan->infos = sample_infos;
	std::function<::dds_ReturnCode_t()> cleanup = [loan]() { return loan->finish(); };
	auto ret = data_values.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = sample_infos.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = ::dds_DynamicDataReader_read_next_instance_w_condition(
	    self, data_values, sample_infos, max_samples, previous_handle, a_condition);
	loan->active = (ret == dds_RETCODE_OK);
	if(!loan->active)
		loan->reader.reset();
	return ret;
}

inline ::dds_ReturnCode_t dds_DynamicDataReader_take_next_instance_w_condition(const dds_DataReader& self,
    dds_DynamicDataSeq& data_values,
    dds_SampleInfoSeq& sample_infos,
    int32_t max_samples,
    ::dds_InstanceHandle_t previous_handle,
    const ::dds_ReadCondition* a_condition) {
	if(!self || !data_values || !sample_infos)
		return dds_RETCODE_BAD_PARAMETER;
	auto loan = std::make_shared<detail::SequenceLoan>();
	loan->reader = detail::shared_impl(self);
	loan->dynamic_data = data_values;
	loan->infos = sample_infos;
	std::function<::dds_ReturnCode_t()> cleanup = [loan]() { return loan->finish(); };
	auto ret = data_values.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = sample_infos.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = ::dds_DynamicDataReader_take_next_instance_w_condition(
	    self, data_values, sample_infos, max_samples, previous_handle, a_condition);
	loan->active = (ret == dds_RETCODE_OK);
	if(!loan->active)
		loan->reader.reset();
	return ret;
}

inline ::dds_ReturnCode_t dds_DataReader_read_w_sampleinfoex(const dds_DataReader& self,
    dds_DataSeq& data_values,
    dds_SampleInfoSeq& sample_infos,
    int32_t max_samples,
    ::dds_SampleStateMask sample_states,
    ::dds_ViewStateMask view_states,
    ::dds_InstanceStateMask instance_states) {
	if(!self || !data_values || !sample_infos)
		return dds_RETCODE_BAD_PARAMETER;
	auto loan = std::make_shared<detail::SequenceLoan>();
	loan->reader = detail::shared_impl(self);
	loan->data = data_values;
	loan->infos = sample_infos;
	std::function<::dds_ReturnCode_t()> cleanup = [loan]() { return loan->finish(); };
	auto ret = data_values.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = sample_infos.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = ::dds_DataReader_read_w_sampleinfoex(
	    self, data_values, sample_infos, max_samples, sample_states, view_states, instance_states);
	loan->active = (ret == dds_RETCODE_OK);
	if(!loan->active)
		loan->reader.reset();
	return ret;
}

inline ::dds_ReturnCode_t dds_DataReader_take_w_sampleinfoex(const dds_DataReader& self,
    dds_DataSeq& data_values,
    dds_SampleInfoSeq& sample_infos,
    int32_t max_samples,
    ::dds_SampleStateMask sample_states,
    ::dds_ViewStateMask view_states,
    ::dds_InstanceStateMask instance_states) {
	if(!self || !data_values || !sample_infos)
		return dds_RETCODE_BAD_PARAMETER;
	auto loan = std::make_shared<detail::SequenceLoan>();
	loan->reader = detail::shared_impl(self);
	loan->data = data_values;
	loan->infos = sample_infos;
	std::function<::dds_ReturnCode_t()> cleanup = [loan]() { return loan->finish(); };
	auto ret = data_values.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = sample_infos.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = ::dds_DataReader_take_w_sampleinfoex(
	    self, data_values, sample_infos, max_samples, sample_states, view_states, instance_states);
	loan->active = (ret == dds_RETCODE_OK);
	if(!loan->active)
		loan->reader.reset();
	return ret;
}

inline ::dds_ReturnCode_t dds_DataReader_raw_read(const dds_DataReader& self,
    ::dds_InstanceHandle_t handle,
    dds_DataSeq& raw_data_values,
    dds_SampleInfoSeq& sample_infos,
    dds_UnsignedLongSeq& raw_data_sizes,
    int32_t max_samples,
    ::dds_SampleStateMask sample_states,
    ::dds_ViewStateMask view_states,
    ::dds_InstanceStateMask instance_states) {
	if(!self || !raw_data_values || !sample_infos || !raw_data_sizes)
		return dds_RETCODE_BAD_PARAMETER;
	auto loan = std::make_shared<detail::SequenceLoan>();
	loan->reader = detail::shared_impl(self);
	loan->data = raw_data_values;
	loan->infos = sample_infos;
	loan->sizes = raw_data_sizes;
	std::function<::dds_ReturnCode_t()> cleanup = [loan]() { return loan->finish(); };
	auto ret = raw_data_values.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = sample_infos.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = raw_data_sizes.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = ::dds_DataReader_raw_read(self,
	    handle,
	    raw_data_values,
	    sample_infos,
	    raw_data_sizes,
	    max_samples,
	    sample_states,
	    view_states,
	    instance_states);
	loan->active = (ret == dds_RETCODE_OK);
	if(!loan->active)
		loan->reader.reset();
	return ret;
}

inline ::dds_ReturnCode_t dds_DataReader_raw_read_w_sampleinfoex(const dds_DataReader& self,
    ::dds_InstanceHandle_t handle,
    dds_DataSeq& raw_data_values,
    dds_SampleInfoSeq& sample_infoexs,
    dds_UnsignedLongSeq& raw_data_sizes,
    int32_t max_samples,
    ::dds_SampleStateMask sample_states,
    ::dds_ViewStateMask view_states,
    ::dds_InstanceStateMask instance_states) {
	if(!self || !raw_data_values || !sample_infoexs || !raw_data_sizes)
		return dds_RETCODE_BAD_PARAMETER;
	auto loan = std::make_shared<detail::SequenceLoan>();
	loan->reader = detail::shared_impl(self);
	loan->data = raw_data_values;
	loan->infos = sample_infoexs;
	loan->sizes = raw_data_sizes;
	std::function<::dds_ReturnCode_t()> cleanup = [loan]() { return loan->finish(); };
	auto ret = raw_data_values.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = sample_infoexs.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = raw_data_sizes.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = ::dds_DataReader_raw_read_w_sampleinfoex(self,
	    handle,
	    raw_data_values,
	    sample_infoexs,
	    raw_data_sizes,
	    max_samples,
	    sample_states,
	    view_states,
	    instance_states);
	loan->active = (ret == dds_RETCODE_OK);
	if(!loan->active)
		loan->reader.reset();
	return ret;
}

inline ::dds_ReturnCode_t dds_DataReader_raw_take(const dds_DataReader& self,
    ::dds_InstanceHandle_t handle,
    dds_DataSeq& raw_data_values,
    dds_SampleInfoSeq& sample_infos,
    dds_UnsignedLongSeq& raw_data_sizes,
    int32_t max_samples,
    ::dds_SampleStateMask sample_states,
    ::dds_ViewStateMask view_states,
    ::dds_InstanceStateMask instance_states) {
	if(!self || !raw_data_values || !sample_infos || !raw_data_sizes)
		return dds_RETCODE_BAD_PARAMETER;
	auto loan = std::make_shared<detail::SequenceLoan>();
	loan->reader = detail::shared_impl(self);
	loan->data = raw_data_values;
	loan->infos = sample_infos;
	loan->sizes = raw_data_sizes;
	std::function<::dds_ReturnCode_t()> cleanup = [loan]() { return loan->finish(); };
	auto ret = raw_data_values.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = sample_infos.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = raw_data_sizes.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = ::dds_DataReader_raw_take(self,
	    handle,
	    raw_data_values,
	    sample_infos,
	    raw_data_sizes,
	    max_samples,
	    sample_states,
	    view_states,
	    instance_states);
	loan->active = (ret == dds_RETCODE_OK);
	if(!loan->active)
		loan->reader.reset();
	return ret;
}

inline ::dds_ReturnCode_t dds_DataReader_raw_take_w_sampleinfoex(const dds_DataReader& self,
    ::dds_InstanceHandle_t handle,
    dds_DataSeq& raw_data_values,
    dds_SampleInfoSeq& sample_infoexs,
    dds_UnsignedLongSeq& raw_data_sizes,
    int32_t max_samples,
    ::dds_SampleStateMask sample_states,
    ::dds_ViewStateMask view_states,
    ::dds_InstanceStateMask instance_states) {
	if(!self || !raw_data_values || !sample_infoexs || !raw_data_sizes)
		return dds_RETCODE_BAD_PARAMETER;
	auto loan = std::make_shared<detail::SequenceLoan>();
	loan->reader = detail::shared_impl(self);
	loan->data = raw_data_values;
	loan->infos = sample_infoexs;
	loan->sizes = raw_data_sizes;
	std::function<::dds_ReturnCode_t()> cleanup = [loan]() { return loan->finish(); };
	auto ret = raw_data_values.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = sample_infoexs.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = raw_data_sizes.prepare_loan(cleanup, loan.get(), self.get());
	if(ret != dds_RETCODE_OK)
		return ret;
	ret = ::dds_DataReader_raw_take_w_sampleinfoex(self,
	    handle,
	    raw_data_values,
	    sample_infoexs,
	    raw_data_sizes,
	    max_samples,
	    sample_states,
	    view_states,
	    instance_states);
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
	if(sample.deleter()->reader != detail::shared_impl(self))
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
	if(sample && sample.deleter()->writer != detail::shared_impl(self))
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
	cleanup.writer = detail::shared_impl(self);
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
	cleanup.reader = detail::shared_impl(self);
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
	cleanup.reader = detail::shared_impl(self);
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
	    : parent_(std::move(parent))
	    , raw_(raw) {
	}

	StatusCondition(const StatusCondition&) = delete;
	StatusCondition& operator=(const StatusCondition&) = delete;

	::dds_StatusCondition* operator*() noexcept {
		return raw_;
	}

	const ::dds_StatusCondition* operator*() const noexcept {
		return raw_;
	}

	StatusCondition& operator=(std::nullptr_t) noexcept {
		raw_ = nullptr;
		return *this;
	}

private:
	std::shared_ptr<void> parent_;
	::dds_StatusCondition* raw_;
};

using dds_StatusCondition = DDSEntity<StatusCondition>;

inline dds_StatusCondition dds_DomainParticipant_get_statuscondition(const dds_DomainParticipant& self) {
	if(!self)
		return {};
	auto raw = ::dds_DomainParticipant_get_statuscondition(self);
	if(!raw)
		return {};
	return dds_StatusCondition(detail::make_owned<StatusCondition>(raw, detail::shared_impl(self)));
}

inline dds_StatusCondition dds_Topic_get_statuscondition(const dds_Topic& self) {
	if(!self)
		return {};
	auto raw = ::dds_Topic_get_statuscondition(self);
	if(!raw)
		return {};
	return dds_StatusCondition(detail::make_owned<StatusCondition>(raw, detail::shared_impl(self)));
}

inline dds_StatusCondition dds_Publisher_get_statuscondition(const dds_Publisher& self) {
	if(!self)
		return {};
	auto raw = ::dds_Publisher_get_statuscondition(self);
	if(!raw)
		return {};
	return dds_StatusCondition(detail::make_owned<StatusCondition>(raw, detail::shared_impl(self)));
}

inline dds_StatusCondition dds_Subscriber_get_statuscondition(const dds_Subscriber& self) {
	if(!self)
		return {};
	auto raw = ::dds_Subscriber_get_statuscondition(self);
	if(!raw)
		return {};
	return dds_StatusCondition(detail::make_owned<StatusCondition>(raw, detail::shared_impl(self)));
}

inline dds_StatusCondition dds_DataWriter_get_statuscondition(const dds_DataWriter& self) {
	if(!self)
		return {};
	auto raw = ::dds_DataWriter_get_statuscondition(self);
	if(!raw)
		return {};
	return dds_StatusCondition(detail::make_owned<StatusCondition>(raw, detail::shared_impl(self)));
}

inline dds_StatusCondition dds_DataReader_get_statuscondition(const dds_DataReader& self) {
	if(!self)
		return {};
	auto raw = ::dds_DataReader_get_statuscondition(self);
	if(!raw)
		return {};
	return dds_StatusCondition(detail::make_owned<StatusCondition>(raw, detail::shared_impl(self)));
}

inline ::dds_ReturnCode_t dds_WaitSet_attach_condition(const dds_WaitSet& self, const dds_StatusCondition& condition) {
	if(!self || !condition)
		return dds_RETCODE_BAD_PARAMETER;
	return detail::shared_impl(self)->attach(condition);
}

inline ::dds_ReturnCode_t dds_WaitSet_detach_condition(const dds_WaitSet& self, const dds_StatusCondition& condition) {
	if(!self || !condition)
		return dds_RETCODE_BAD_PARAMETER;
	return detail::shared_impl(self)->detach(condition);
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

inline dds_String dds_StringTypeSupport_deserialize(void* input, size_t input_size) {
	return dds_String::create([=]() { return ::dds_StringTypeSupport_deserialize(input, input_size); });
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

} // namespace raii

#endif // RAII_HPP
