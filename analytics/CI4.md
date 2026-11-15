# CI4 — Make Release Pipeline Green

> Цель этапа: добиться, чтобы `CI` workflow на `main` стал стабильно зелёным,
> а `Release on Push` workflow создал **первый тег и первый GitHub Release**.
>
> Промпты выполнять строго по порядку. Каждый промпт — отдельный коммит.
> Нельзя начинать P3, пока P1 и P2 локально зелёные.
> Нельзя начинать P4, пока P3 не дал зелёный CI на GitHub.

---

## Контекст проблемы

На 15.11.2026 в репо `Reider85/dieseldb`:
- **0 тегов**, **0 релизов**.
- Последние ~25 запусков `CI` (#737…#761) — все `FAILED`.
- `Release on Push` отработал 22 раза, но тегов не создал: джоб `release`
  защищён условием `github.event.workflow_run.conclusion == 'success'`,
  а CI красный → джоб стабильно **skipped**.

Корневая причина — не release-файл, а красный `compile-gate` после
миграции `java.util.logging → SLF4J` (коммиты `ed371f8`, `bf207aa`,
`b1456ea`, `c7242df`, `9be2ab7`). Релизный YAML при этом корректен,
но не получает шанса отработать.

---

## Prompt 1 — Локально починить компиляцию после миграции JUL → SLF4J

**Goal.** Команда ниже завершается с `BUILD SUCCESS` локально на JDK 21:

```bash
mvn -B -T 4 --no-transfer-progress -Dstyle.color=never \
    compile -P all -DskipTests
```

**Files likely to touch.**
- `src/main/java/**` — классы, где остались вызовы `Logger.warning(...)`,
  `Logger.fine(...)`, `Level.FINEST`, `log.log(Level.SEVERE, ...)`.
- `pom.xml` — убедиться, что `slf4j-api` и `slf4j-simple` (или
  `logback-classic`) объявлены в `<dependencies>` и не конфликтуют с
  остатками `java.util.logging`.

**Steps.**
1. Воспроизвести ошибку локально:
   ```bash
   mvn -B compile -P all -DskipTests 2>&1 | tee /tmp/compile.log
   grep -E 'COMPILATION ERROR|ERROR.*\.java' /tmp/compile.log | head -50
   ```
2. Для каждой ошибки привести сигнатуры к SLF4J:
   - `logger.log(Level.WARNING, "x")`        → `logger.warn("x")`
   - `logger.log(Level.INFO, "x {}", arg)`   → `logger.info("x {}", arg)`
   - `logger.fine("x")`                      → `logger.debug("x")`
   - `logger.finest("x")`                    → `logger.trace("x")`
   - `Level.FINEST`/`Level.FINER`            → `trace`/`debug`
   - `Handler`, `ConsoleHandler`, `FileHandler` → удалить или заменить на
     SLF4J-конфигурацию (`logback.xml` / `simplelogger.properties`).
3. Не оставлять `import java.util.logging.*;` — он тянет за собой
   забытое использование `Level`.
4. Повторять п.1 до `BUILD SUCCESS`.

**Acceptance.**
- `mvn compile -P all -DskipTests` → `BUILD SUCCESS`.
- `grep -rn 'java\.util\.logging' src/main/java | wc -l` → `0`.
- Коммит: `CI4-P1: fix SLF4J migration, restore compilation`.

**Anti-goals.** Не трогать тесты, не рефакторить несвязанный код, не
поднимать версии зависимостей в этом промпте.

---

## Prompt 2 — Починить профиль `fast` (тесты должны быть зелёными)

**Goal.** Локально зелёный fast-профиль:

```bash
mvn -B -T 4 --no-transfer-progress -Dstyle.color=never \
    test -P fast
```

**Files likely to touch.**
- `src/test/java/**` — тесты, завязанные на старое поведение логгера
  (например, проверяющие `handler.publish(...)` или
  `LogManager.getLogManager().getLogger(...)`).
- `src/main/java/**` — только если тест вскрыл реальный регрессию
  миграции SLF4J (например, сообщение пишется на другом уровне).

**Steps.**
1. Прогнать:
   ```bash
   mvn -B test -P fast 2>&1 | tee /tmp/test.log
   grep -E 'FAILURE|ERROR|Tests run:' /tmp/test.log | tail -50
   ```
2. Для каждого падающего теста:
   - прочитать `target/surefire-reports/<TestClass>.txt`;
   - если тест проверял `java.util.logging` инфраструктуру — переписать
     под SLF4J (`LoggerFactory.getLogger(...)`, `ListAppender` из
     `slf4j-test` или собственный `Appender` для logback);
   - если тест падает на функциональной логике — заводить отдельный
     баг-репорт, не чинить в рамках этого промпта.
3. Повторять до `BUILD SUCCESS` с `Tests run: …, Failures: 0, Errors: 0`.

**Acceptance.**
- `mvn test -P fast` → `BUILD SUCCESS`, 0 failures, 0 errors.
- `git diff --stat` — изменения только в `src/test/**` (плюс точечные
  правки `src/main/**` только если они следуют из п.2).
- Коммит: `CI4-P2: fix fast profile tests after SLF4J migration`.

---

## Prompt 3 — Запушить и дождаться зелёного CI на GitHub

**Goal.** На вкладке `github.com/Reider85/dieseldb/actions` для свежего
пуша в `main`:
- джоб `compile-gate` → `success`;
- джоб `fast` → `success`;
- workflow `CI` → `success`.

**Steps.**
1. `git push origin main` (с коммитами P1 и P2).
2. Открыть `https://github.com/Reider85/dieseldb/actions/workflows/ci.yml`.
3. Дождаться завершения обоих джобов.
4. Если `fast` упал на окружении GitHub (но локально был зелёным) —
   скачать артефакт `reports-fast` (шаг `Upload test reports` в `ci.yml`,
   строка 172), разобрать `surefire-reports/`, починить, запушить ещё
   раз.

**Acceptance.**
- Workflow `CI` на последнем коммите в `main` имеет conclusion `success`.
- **Без выполнения P3 все следующие промпты бессмысленны** —
  `Release on Push` будет и дальше пропускать джоб `release`.

**Anti-goals.** Не править `release-on-push.yml`, пока CI не зелёный —
иначе маскируется настоящая причина.

---

## Prompt 4 — Включить read-and-write permissions на уровне репозитория

**Goal.** Шаг `git push origin "$TAG"` в `release-on-push.yml` (строка 111)
не падает с `remote: Permission to the repo denied`.

**Steps.**
1. Открыть `https://github.com/Reider85/dieseldb/settings/actions`.
2. Раздел **Workflow permissions**.
3. Выбрать **«Read and write permissions»**.
4. Сохранить.
5. (Опц.) Включить **«Allow GitHub Actions to create and approve pull requests»**,
   если планируется автоматизация PR.

**Why.** В `release-on-push.yml`:
```yaml
permissions:
  contents: write   # required to push tags and create releases
```
Это декларация работает **только если** настройка репо разрешает
запись. Если репо стоит в «Read repository contents permission» —
`permissions: contents: write` молча игнорируется, и шаг push тега
падает. Это **самая частая причина** «workflow зелёный, а тегов нет».

**Acceptance.**
- Скриншот секции Workflow permissions с выбранным «Read and write».
- В файл изменений не вносится — это настройка репозитория.

---

## Prompt 5 — Переставить шаги в `release-on-push.yml` и убрать `2>/dev/null`

**Goal.** В `.github/workflows/release-on-push.yml`:
- тег пушится **только после** успешной сборки JAR;
- из `mvn help:evaluate` убран `2>/dev/null`, чтобы не маскировать
  ошибки Maven.

**Files to touch.** `.github/workflows/release-on-push.yml`

**Patch (логика).**
1. Шаг `Compute release version` (строки 50–69):
   - убрать `2>/dev/null` из команды `mvn help:evaluate`;
   - `tail -n 1` заменить на `awk 'NF{p=$0} END{print p}'` (последняя
     непустая строка), чтобы не съесть реальное значение, если Maven
     напечатает warning выше.
2. Шаги переупорядочить в строго такую последовательность:
   ```
   1. Checkout
   2. Set up JDK 21
   3. Compute release version
   4. Skip if tag already exists          (guard)
   5. Build JAR                            (if: exists == 'false')
   6. Verify JAR exists                    (if: exists == 'false')
   7. Create and push tag                  (if: exists == 'false' && success)
   8. Generate javadoc link                (if: exists == 'false' && success)
   9. Create GitHub Release with JAR       (if: exists == 'false' && success)
   10. Release summary
   ```
   **Ключевое:** шаги 5–6 выполняются **до** шага 7. Если JAR не собран —
   тег не пушится, и повторный прогон сможет релизить тот же коммит.
3. У шага `Create and push tag` добавить `if: success()` рядом с
   существующим `if: steps.guard.outputs.exists == 'false'`:
   ```yaml
   if: success() && steps.guard.outputs.exists == 'false'
   ```

**Acceptance.**
- Локальный `yamllint` (или `actionlint`) на файле проходит без ошибок.
- В коммите — только `release-on-push.yml`.
- Коммит: `CI4-P5: reorder release steps, unmask mvn help:evaluate errors`.

**Gotcha.** Триггер `workflow_run` использует **копию файла из `main`**,
не из коммита-источника. Правки вступают в силу **только после мержа в
`main`**. Поэтому коммит P5 должен идти в `main` (или через прямой push,
или через fast-forward PR).

---

## Prompt 6 — Синхронизировать версию `pom.xml` с коммитами 3.4.x

**Goal.** В `pom.xml` `<version>` приведена в соответствие с текущей
серийной нумерацией коммитов (`3.4.x`), чтобы первый релиз получил
осмысленный тег `v3.4.<N>-build.<run>`, а не `v3.3.99-build.<run>`.

**Files to touch.** `pom.xml`

**Steps.**
1. Текущее состояние:
   ```bash
   mvn -B help:evaluate -Dexpression=project.version -q -DforceStdout
   ```
   Ожидается: `3.3.99`.
2. Поднять `<version>` (и `<parent>` если есть) до `3.4.44` (или
   следующего за `3.4.43` CI3 completion — берётся из последнего
   коммита `main`).
3. Прогнать:
   ```bash
   mvn -B compile -P all -DskipTests
   mvn -B test -P fast
   ```
   Оба — зелёные.
4. Запушить.

**Acceptance.**
- `mvn help:evaluate -Dexpression=project.version -q -DforceStdout`
  → `3.4.44` (или иное согласованное число).
- CI на этом коммите снова зелёный (P1+P2 не сломаны).
- Коммит: `CI4-P6: bump version to 3.4.44 (sync with commit series)`.

---

## Prompt 7 — Первый релиз: явный `body`, без `generate_release_notes`

**Goal.** Первый GitHub Release создаётся без падения `softprops/action-gh-release@v2`
из-за отсутствия предыдущего тега.

**Files to touch.** `.github/workflows/release-on-push.yml` (шаг
`Create GitHub Release with JAR`, строки 124–141).

**Patch.**
```yaml
      - name: Create GitHub Release with JAR
        if: steps.guard.outputs.exists == 'false'
        uses: softprops/action-gh-release@v2
        with:
          tag_name: ${{ steps.version.outputs.tag }}
          generate_release_notes: false   # ← временно
          files: target/*.jar
          fail_on_unmatched_files: true
          draft: false
          prerelease: false
          body: |
            ## DieselDB ${{ steps.version.outputs.tag }}

            First automated release from CI4 pipeline.

            - Built from commit ${{ github.event.workflow_run.head_sha }}
            - CI run: ${{ github.event.workflow_run.run_number }}
            - Javadoc: ${{ steps.javadoc-link.outputs.javadoc_url }}

            ⭐ Star this repo if you find DieselDB useful.
```

**Why.** `generate_release_notes: true` при нулевых тегах в репо
выводит changelog «с начала истории» (~1100 коммитов) и иногда падает
по таймауту. Для первого релиза проще явный `body:`. После P8 можно
вернуть `generate_release_notes: true`.

**Acceptance.**
- После ближайшего зелёного пуша в `main` в репозитории:
  - вкладка `/tags` показывает **≥1 тег** вида `v3.4.44-build.<N>`;
  - вкладка `/releases` показывает **≥1 release** с приложенным JAR.
- Коммит: `CI4-P7: first release uses explicit body (no auto-notes)`.

---

## Prompt 8 — Включить `generate_release_notes: true` для последующих релизов

**Goal.** Со второго релиза и далее автоматически генерируются release
notes по `git log` между текущим и предыдущим тегом.

**Steps.**
1. Убедиться, что в репозитории уже есть ≥1 тег (результат P7).
2. В `release-on-push.yml`, в шаге `Create GitHub Release with JAR`,
   поменять `generate_release_notes: false` → `true`.
   При желании оставить `body:` как преамбулу — GitHub склеит их.
3. Запушить следующий коммит (любой в `src/`), дождаться зелёного CI и
   автоматического релиза.

**Acceptance.**
- Второй релиз содержит секцию **«What's Changed»** с коммитами между
  предыдущим и новым тегом.
- Коммит: `CI4-P8: enable auto-generated release notes for >=2nd release`.

---

## Definition of Done для всего этапа CI4

- [ ] На `main` последний запуск workflow `CI` — `success`.
- [ ] В репо есть **≥1 тег** вида `v3.4.<N>-build.<run>`.
- [ ] В репо есть **≥1 GitHub Release** с приложенным JAR-файлом.
- [ ] В `.github/workflows/release-on-push.yml` порядок шагов:
      Build JAR → Verify JAR → Push Tag → Create Release.
- [ ] `Settings → Actions → Workflow permissions = Read and write`.
- [ ] В `pom.xml` `<version>` совпадает с серией коммитов `3.4.x`.
- [ ] В `src/main/java` нет `import java.util.logging.*`.
- [ ] После следующего пуша в `main` релиз создаётся **автоматически**
      (без ручного триггера).

## Не делается в рамках CI4

- Не рефакторить архитектуру БД (ARIES, WAL, MVCC, SSI).
- Не поднимать версии Java/Maven/зависимостей, кроме SLF4J-related.
- Не включать профили `core`, `concurrency`, `network`, `perf`, `large`
  в основной `push`-гейт — они остаются в `schedule` / `workflow_dispatch`.
- Не переименовывать workflow-файлы.
