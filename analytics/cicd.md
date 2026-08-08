# CI/CD Roadmap для DieselDB: 20% усилий → 80% улучшений

## Контекст проекта

**DieselDB** — экспериментальная СУБД на Java 17:
- 34 файла исходного кода (~8,309 строк)
- 16 тестовых классов
- Maven-проект с двумя профилями тестирования
- Текущий CI: заготовка GitHub Actions (только checkout)
- SonarQube анализ выявил 908 проблем (81 bug, 827 code smells)

---

## Принцип Парето для CI/CD DieselDB

Из всех возможных CI/CD практик выбраны **критические 20%**, которые дадут **80% улучшений** в качестве, надёжности и скорости разработки.

---

## Приоритеты по критичности и effort/reward

### 🔴 P0 — Критично (Неделя 1)
*Минимальные усилия, максимальный эффект*

| # | Инструмент/Workflow | Усилия (ч) | Улучшение (%) | Статус |
|---|---------------------|------------|---------------|--------|
| 1 | **GitHub Actions: Build + Test** | 2 | 40% | ❌ Отсутствует |
| 2 | **Maven Surefire: полный прогон тестов** | 1 | 25% | ⚠️ Частично |
| 3 | **Артефакты: .jar после сборки** | 1 | 15% | ❌ Отсутствует |

**Почему критично:**
- Сейчас CI workflow содержит только `checkout` — сборка и тесты не выполняются
- Без автоматических тестов невозможно гарантировать работоспособность при изменениях
- Нет артефактов для ручного тестирования или развёртывания

---

### 🟠 P1 — Высокий приоритет (Неделя 2)
*Умеренные усилия, значительный эффект*

| # | Инструмент/Workflow | Усилия (ч) | Улучшение (%) | Статус |
|---|---------------------|------------|---------------|--------|
| 4 | **SonarQube Cloud (бесплатно)** | 3 | 30% | ❌ Отсутствует |
| 5 | **Code Coverage (JaCoCo)** | 2 | 20% | ❌ Отсутствует |
| 6 | **Matrix-тестирование: JDK 17, 21** | 2 | 15% | ❌ Отсутствует |

**Почему важно:**
- 81 баг и 116 critical issues уже обнаружены статическим анализом
- Покрытие тестами неизвестно — риск регрессий
- Поддержка LTS-версий Java критична для production-ready проекта

---

### 🟡 P2 — Средний приоритет (Неделя 3-4)
*Умеренные усилия, долгосрочный эффект*

| # | Инструмент/Workflow | Усилия (ч) | Улучшение (%) | Статус |
|---|---------------------|------------|---------------|--------|
| 7 | **Auto-release через GitHub Releases** | 4 | 20% | ❌ Отсутствует |
| 8 | **Dependency Review (security)** | 2 | 15% | ❌ Отсутствует |
| 9 | **Performance benchmarks в CI** | 4 | 25% | ⚠️ Есть PerformanceTest |

**Почему полезно:**
- Автоматизация релизов ускорит delivery
- Security scan предотвратит уязвимости зависимостей
- Контроль производительности критичен для СУБД

---

### 🟢 P3 — Опционально (Месяц 2+)
*Высокие усилия, нишевый эффект*

| # | Инструмент/Workflow | Усилия (ч) | Улучшение (%) | Статус |
|---|---------------------|------------|---------------|--------|
| 10 | Docker-образ для интеграционных тестов | 8 | 10% | ❌ Отсутствует |
| 11 | Slack/Discord уведомления | 2 | 5% | ❌ Отсутствует |
| 12 | Stale PR/issue бот | 1 | 5% | ❌ Отсутствует |

---

## Детальный план внедрения (P0 + P1 = 6 часов)

### Шаг 1: Полный CI Workflow (2 часа)

**Файл:** `.github/workflows/ci.yml`

```yaml
name: CI

on:
  push:
    branches: [ main, develop ]
  pull_request:
    branches: [ main ]

jobs:
  build-and-test:
    runs-on: ubuntu-latest
    
    steps:
      - name: Checkout code
        uses: actions/checkout@v4

      - name: Set up JDK 17
        uses: actions/setup-java@v4
        with:
          java-version: '17'
          distribution: 'temurin'
          cache: maven

      - name: Build with Maven
        run: mvn -B package --file pom.xml

      - name: Run all tests
        run: mvn -Ptest test

      - name: Upload test results
        uses: actions/upload-artifact@v4
        if: always()
        with:
          name: test-results
          path: target/surefire-reports/

      - name: Upload JAR artifact
        uses: actions/upload-artifact@v4
        with:
          name: dieseldb-jar
          path: target/dieseldb-*.jar
```

**Эффект:**
- ✅ Автоматическая сборка при каждом коммите
- ✅ Прогон всех 16 тестовых классов
- ✅ Сохранение результатов тестов и .jar для скачивания

---

### Шаг 2: JaCoCo Code Coverage (2 часа)

**Изменения в `pom.xml`:**

```xml
<build>
    <plugins>
        <!-- Существующий surefire plugin -->
        
        <plugin>
            <groupId>org.jacoco</groupId>
            <artifactId>jacoco-maven-plugin</artifactId>
            <version>0.8.12</version>
            <executions>
                <execution>
                    <goals>
                        <goal>prepare-agent</goal>
                    </goals>
                </execution>
                <execution>
                    <id>report</id>
                    <phase>test</phase>
                    <goals>
                        <goal>report</goal>
                    </goals>
                </execution>
            </executions>
        </plugin>
    </plugins>
</build>
```

**Добавить в CI workflow:**

```yaml
      - name: Upload coverage reports to Codecov
        uses: codecov/codecov-action@v4
        with:
          files: target/site/jacoco/jacoco.xml
          flags: unittests
          fail_ci_if_error: false
```

**Эффект:**
- ✅ Видимость покрытия кода тестами
- ✅ Выявление нетестируемых участков кода
- ✅ Интеграция с Codecov (бесплатно для OSS)

---

### Шаг 3: SonarQube Cloud Integration (3 часа)

**Требования:**
- Бесплатный план для OSS проектов
- Интеграция с GitHub

**Добавить в CI workflow:**

```yaml
      - name: SonarQube Scan
        env:
          SONAR_TOKEN: ${{ secrets.SONAR_TOKEN }}
        run: |
          mvn -B verify org.sonarsource.scanner.maven:sonar-maven-plugin:sonar \
            -Dsonar.projectKey=dieseldb \
            -Dsonar.organization=dieseldb \
            -Dsonar.host.url=https://sonarcloud.io
```

**Настройка:**
1. Зарегистрироваться на https://sonarcloud.io
2. Создать проект `dieseldb`
3. Сгенерировать токен и добавить в GitHub Secrets как `SONAR_TOKEN`

**Эффект:**
- ✅ Автоматический анализ 908 проблем при каждом PR
- ✅ Quality Gate блокирует мерж с новыми багами
- ✅ Тренд качества во времени

---

### Шаг 4: Matrix Testing JDK (2 часа)

**Добавить в CI workflow:**

```yaml
  build-matrix:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        java: [ '17', '21' ]
    
    steps:
      - name: Checkout code
        uses: actions/checkout@v4

      - name: Set up JDK ${{ matrix.java }}
        uses: actions/setup-java@v4
        with:
          java-version: ${{ matrix.java }}
          distribution: 'temurin'
          cache: maven

      - name: Build and Test
        run: mvn -Ptest -B verify
```

**Эффект:**
- ✅ Гарантия совместимости с JDK 17 и 21
- ✅ Раннее обнаружение проблем совместимости

---

## Итоговая матрица Effort vs Impact

```
Impact ▲
   │
80%│         ● SonarQube (P1)
   │         ● Full Tests (P0)
   │    ● JaCoCo (P1)
60%│    ● Build+Test (P0)
   │    ● Artifacts (P0)
   │
40%│              ● Auto-release (P2)
   │              ● Perf Benchmarks (P2)
   │         ● Matrix JDK (P1)
20%│              ● Dependency Review (P2)
   │                       ● Docker (P3)
   │                       ● Notifications (P3)
 0%└──────────────────────────────────────► Effort
    0h   2h   4h   6h   8h   10h  12h  14h
```

**Зона 20/80:** P0 + P1 = **6-8 часов** → **~80% улучшений**

---

## Метрики успеха

| Метрика | До | После (P0+P1) | Цель |
|---------|----|---------------|------|
| Время обнаружения багов | Ручное | < 10 мин после коммита | ✅ |
| Покрытие тестами | Неизвестно | Измеримо (JaCoCo) | >70% |
| Critical bugs в коде | 116 | Снижение на 30% за месяц | ✅ |
| Время сборки | N/A | ~3-5 мин | ✅ |
| Артефакты для тестирования | Нет | .jar после каждого коммита | ✅ |

---

## Рекомендации по порядку внедрения

### Неделя 1 (4 часа) — P0
1. День 1: Настроить полный CI workflow (Шаг 1)
2. День 2: Добавить загрузку артефактов (Шаг 1)
3. День 3: Протестировать на реальном PR

### Неделя 2 (4 часа) — P1
1. День 1: Интегрировать JaCoCo (Шаг 2)
2. День 2-3: Настроить SonarQube Cloud (Шаг 3)
3. День 4: Matrix testing JDK (Шаг 4)

### Неделя 3-4 (8 часов) — P2
- Auto-release workflow
- Dependency review
- Performance benchmarks в CI

---

## Заключение

Для DieselDB **критические 20% усилий** — это настройка:
1. ✅ Полноценного GitHub Actions CI (build + test + artifacts)
2. ✅ JaCoCo code coverage
3. ✅ SonarQube анализа
4. ✅ Matrix testing JDK

**Общие затраты:** 6-8 часов  
**Ожидаемый эффект:** 80% улучшения в надёжности, качестве кода и скорости обнаружения проблем.

Следующие 80% усилий (Docker, уведомления, сложные release workflows) дадут лишь дополнительные 20% улучшений и могут быть отложены до стадии production readiness.

---

## Приложения

### A. Список тестов для полного прогона
```
- AdvancedTest
- AliasesTest
- AllTestsSampleTest
- DatabaseSmokeTest
- GracefulShutdownTest
- GroupByTest
- InTest
- JoinTest
- LikeTest
- OrderByTest
- PerformanceTest
- PersistenceTest
- QuantitativeTest
- ServerConnectionLimitTest
- SocketTimeoutTest
- SubqueriesTest
```

### B. Ссылки
- GitHub Actions Docs: https://docs.github.com/en/actions
- SonarQube Cloud (OSS): https://sonarcloud.io
- JaCoCo Maven Plugin: https://www.eclemma.org/jacoco/trunk/doc/maven.html
- Codecov: https://about.codecov.io

---

*Документ создан: 2026-08-08*  
*Автор: AI Assistant*  
*Проект: DieselDB v0.5.0-SNAPSHOT*
