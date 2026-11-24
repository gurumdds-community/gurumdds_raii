# GurumDDS RAII

[English](README.md)

`raii.hpp`는 GurumDDS C API 자원을 자동으로 정리해 주는 C++11 헤더입니다. 자원 생성·교체·해제에는 `raii::dds_*` 함수를 사용하세요. 자원을 조회하거나 사용하는 `::dds_*` 함수에는 래퍼 핸들을 그대로 넘길 수 있습니다.

## 설치

`raii.hpp`를 프로젝트의 헤더 검색 경로에 추가하고 GurumDDS C 라이브러리를 링크하세요. 빌드에는 예외 처리를 켠 C++11 이상 컴파일러와 래퍼에서 사용하는 C API를 제공하는 GurumDDS SDK가 필요합니다.

SDK가 시스템 검색 경로에 있으면 다음 명령으로 예제를 빌드할 수 있습니다. 다른 경로에 설치했다면 `-I`와 `-L`로 SDK 경로를 지정하세요.

```sh
c++ -std=c++11 -I/path/to/raii main.cpp -lgurumdds -o example
```

## 빠른 시작

아래 코드를 `main.cpp`로 저장하세요. RAII 래퍼로 Participant를 만든 뒤 C API로 도메인 ID를 조회합니다.

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

## 소유권과 수명

| 핸들 | 소유권 |
| --- | --- |
| Participant, Topic, DataReader 등의 DDS 엔티티 | 핸들을 복사하면 같은 엔티티를 공유합니다. DataReader처럼 다른 관리 엔티티를 쓰는 객체는 그 엔티티도 함께 유지합니다. |
| 시퀀스, 샘플, DynamicType 등의 자원 | 이동 전용 핸들입니다. `get()`으로 포인터를 빌려 쓸 수 있고, `close()`를 호출하면 자원 해제 결과를 반환합니다. |
| QoS와 디스크립터 | 이동 전용 래퍼가 소멸할 때 내부 값을 정리합니다. 깊은 복사에는 전용 복사 함수를 사용하세요. QoS 초기값은 해당 기본 QoS 조회 함수로 가져옵니다. |

`get()`으로 받은 포인터는 자원을 소유한 핸들이 살아 있는 동안에만 사용하세요. 래퍼 생성자에는 소유권을 넘길 수 있는 포인터만 전달하세요. `release()`는 다른 자원에 의존하지 않고 소유권을 넘길 수 있을 때 사용합니다.

엔티티를 직접 삭제하려면 `raii::dds_*_delete_*`를 호출하고 반환 코드를 확인하세요. 다른 핸들이 엔티티를 소유하고 있으면 `dds_RETCODE_PRECONDITION_NOT_MET`가 반환될 수 있습니다. C API의 삭제 함수나 `delete_contained_entities`를 호출하면 래퍼에 삭제가 반영되지 않습니다.

리스너와 컨텍스트의 수명은 등록한 쪽에서 관리합니다. Reader를 정리할 때 래퍼가 실행 중인 콜백의 종료를 기다려 주지 않으므로, 콜백이 끝날 때까지 컨텍스트와 콜백이 참조하는 객체를 살려 두세요. 여러 스레드에서 같은 핸들이나 DDS 자원을 사용할 때도 호출하는 쪽에서 동기화해야 합니다.

C API의 생성 함수가 `nullptr`을 반환하면 래퍼는 빈 핸들을 반환합니다. C++ 메모리 할당에 실패하면 예외가 발생할 수 있습니다. 비어 있거나 이미 닫힌 엔티티를 C 포인터로 변환해도 예외가 발생하므로, C 함수에 넘기기 전에 핸들을 확인하세요. 소멸자에서 확인한 정리 작업이 실패하면 `std::terminate()`를 호출합니다.

`raii::dds_DomainParticipantFactory_get_instance()`로 팩토리를 가져오면 프로그램이 정상 종료될 때 DDS 팩토리를 자동으로 정리합니다. 그 전에 DDS 자원을 해제하고 DDS를 사용하는 스레드를 종료하세요.

## Loan과 WaitSet

- Reader에서 시퀀스로 데이터를 빌리면 loan을 반환할 때까지 Reader가 살아 있습니다. 이 loan에 속한 시퀀스 중 하나를 재사용하거나 소멸시키면 loan이 자동으로 반환됩니다. 반환 결과를 확인하려면 해당 래퍼의 loan 반환 함수를 직접 호출하세요.
- Writer 버퍼는 `raii::dds_DataWriter_get_loan(writer, &sample, &capacity)`으로 빌립니다. `sample`에는 `raii::dds_WriterLoan`을 사용하세요. 채운 버퍼를 쓰려면 `raii::dds_DataWriter_raw_write()`, 바로 돌려주려면 `raii::dds_DataWriter_return_loaned_sample()`을 호출합니다. 쓰기 래퍼는 C API를 호출하기 전에 원래 loan을 반환하므로, C API의 쓰기가 실패해도 그 loan은 이미 반환된 상태입니다.
- 래퍼를 통해 WaitSet에 붙인 조건은 조건을 떼거나 WaitSet을 소멸시킬 때까지 살아 있습니다.

## IDL에서 생성한 샘플

IDL로 생성한 TypeSupport 헤더를 포함하고, `extern "C"` 블록 밖의 전역 스코프에서 타입별로 `RAII_TYPESUPPORT_DEFINE`을 한 번씩 선언하세요. 샘플을 할당하거나 역직렬화하고 해제하는 함수와 이동 전용 샘플 핸들이 생깁니다. 타입 등록과 샘플 읽기·쓰기는 생성된 API를 사용합니다.

```cpp
#include "FooTypeSupport.h"
#include "raii.hpp"

RAII_TYPESUPPORT_DEFINE(FooTypeSupport, Foo)
```

`raii::FooTypeSupport_alloc()`으로 받은 `raii::Foo` 핸들이 스코프를 벗어나면 샘플이 자동으로 해제됩니다. 샘플을 쓰기 전에 생성된 API의 규약에 따라 값을 초기화하세요. TypeSupport 싱글턴은 기존 API에서 빌려 씁니다.

## 라이선스

Copyright 2026 GurumNetworks, Inc.

래퍼에는 [Apache License, Version 2.0](LICENSE)이, GurumDDS SDK에는 별도 라이선스가 적용됩니다.
