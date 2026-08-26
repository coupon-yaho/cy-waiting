// 빌드 스크립트의 식별자도 ASCII 영문이다.
//
// **직접 짜지 않는다.** Groovy 는 삼중따옴표·GString·타입 생략까지 있어 정규식으로
// 선언을 고르면 반드시 샌다. CodeNarc 는 Gradle 배포에 들어 있어 새 인프라가 없다.
ruleset {
    ClassName { regex = /^[A-Z][a-zA-Z0-9]*$/ }
    MethodName { regex = /^[a-z][a-zA-Z0-9_]*$/ }
    VariableName { regex = /^[a-z][a-zA-Z0-9]*$/ }
    FieldName { regex = /^[a-z][a-zA-Z0-9]*$/; finalRegex = /^[a-zA-Z][a-zA-Z0-9_]*$/ }
    ParameterName { regex = /^[a-z][a-zA-Z0-9]*$/ }
    PropertyName { regex = /^[a-z][a-zA-Z0-9]*$/ }
}
