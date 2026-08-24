# Гипотезы и дизайн A/B-тестов

## Принципы экспериментов

- рандомизация — на уровне пользователя, чтобы один человек не видел разные
  варианты в соседних сессиях;
- primary metric должна соответствовать сценарию: user/session booking
  conversion предпочтительнее среднего по event rows;
- минимум два полных недельных цикла, затем остановка только по заранее
  рассчитанному sample size;
- сегмент, метрика, MDE и длительность фиксируются до запуска;
- `booking_value_proxy` используется только как относительная secondary
  metric, не как revenue;
- анализ проводится по intention-to-treat, без удаления «неудобных»
  пользователей после рандомизации.

## Приоритет

| Приоритет | Гипотеза | Почему сейчас |
|---|---|---|
| P1 | Упростить mobile package flow в channel 9 | 869 485 event rows, booking rate 2,48% |
| P2 | Помочь long-lead и long-stay планированию | 7,48 млн rows для lead 91+, booking rate 4,54% |
| P3 | Персонализировать повторное бронирование | 315 681 пользователь имеет ровно один booking |
| P4 | Улучшить выбор в крупных low-conversion markets | разрыв 2,59–14,03% среди markets ≥100 тыс. rows |

## H1. Mobile package comparison и checkout

**Наблюдение.** В channel 9 mobile package имеет row booking rate 2,48% против
4,94% у desktop package и 8,24% у mobile hotel-only. Сегмент большой, а канал
9 даёт основную часть трафика.

**Гипотеза.** Если в mobile package-сценарии сделать единый компактный экран
сравнения, явно показать состав пакета и итоговую цену, закрепить CTA и
сократить повторный ввод данных, то session booking conversion вырастет,
потому что снизится когнитивная и интерфейсная нагрузка.

**Дизайн.**

- аудитория: `channel = 9`, `is_mobile = 1`, `is_package = 1`;
- control: текущий flow;
- treatment: компактные package cards + прозрачный price breakdown + sticky
  CTA + сохранение введённых параметров;
- единица рандомизации: `user_id`;
- primary metric: доля search sessions с booking;
- secondary: time to first booking, переход в checkout, завершение checkout,
  `booking_value_proxy_total` на пользователя;
- guardrails: ошибки формы, latency, доля возврата к выдаче, отмены — после
  подключения соответствующих данных.

## H2. Flexible planning для поездок 91+ дней и длинных stay

**Наблюдение.** Booking rate снижается с 15,48% при lead time 0–1 день до
4,54% при 91+ днях. Для stay 8–14 ночей показатель равен 3,24%, для 15+
ночей — 2,47%.

**Гипотеза.** Если для раннего и длинного планирования показать гибкие даты,
диапазон цен, возможность сохранить поиск и продолжить сравнение позже, то
вырастет доля пользователей, возвращающихся к поиску и завершающих booking.

**Дизайн.**

- аудитория: запросы с `lead_time_bucket = 91_plus` и/или
  `stay_length_bucket in (8_14, 15_plus)`;
- treatment: calendar-flex ±3 дня, price range, save-search и reminder внутри
  продукта;
- primary metric: booking conversion пользователя в течение 30 дней после
  первого eligible search;
- secondary: save-search rate, return-to-search rate, session booking rate;
- guardrails: число бесполезных уведомлений, отключение reminders, latency.

## H3. Персонализированный rebooking после первого booking

**Наблюдение.** 26,33% всех пользователей имеют ровно один booking, а среди
bookers 61,22% бронировали повторно внутри наблюдаемого окна.

**Гипотеза.** Если после первого booking на следующем визите показать
персональный модуль «повторить поездку / похожие направления» с учётом прошлого
destination, party и stay profile, то repeat booking rate увеличится.

**Дизайн.**

- аудитория: пользователи с ровно одним завершённым booking и новым визитом;
- control: стандартная главная/выдача;
- treatment: персонализированный rebooking-модуль;
- primary metric: доля пользователей со вторым booking за 90 или 180 дней;
- secondary: CTR модуля, search start, booking value proxy на пользователя;
- guardrails: скрытие модуля, bounce rate, разнообразие destinations;
- особенность: эксперимент требует длинного окна и анализа зрелых когорт.

## H4. Market-aware выдача в крупных low-conversion markets

**Наблюдение.** Среди markets с ≥100 тыс. event rows booking rate сильно
различается. Крупные low-conversion кандидаты: 1590 (2,59%), 150 (2,79%),
134 (3,03%). Сам по себе разрыв не доказывает проблему ranking.

**Гипотеза.** Если после декомпозиции mix в выбранных markets адаптировать
первые позиции, фильтры и объяснение ценности к локальному intent, то booking
conversion вырастет без снижения разнообразия выбора.

**Дизайн.**

- подготовка: сравнить markets внутри одинаковых channel × mobile × package ×
  lead/stay сегментов;
- аудитория: только markets, где разрыв сохраняется после декомпозиции;
- treatment: market-aware ranking features или преднастроенные релевантные
  фильтры;
- primary metric: session booking conversion;
- secondary: CTR первых результатов, глубина просмотра, time to booking;
- guardrails: diversity/coverage выдачи, zero-result rate, latency;
- стратификация: рандомизация отдельно внутри каждого выбранного market.

## Что необходимо добавить в трекинг

Текущий Expedia dataset не содержит полного funnel. До запуска H1/H2/H4
нужны события:

- показ выдачи и позиция результата;
- открытие карточки;
- изменение фильтра/сортировки;
- начало и шаги checkout;
- validation error;
- выход из шага;
- сохранение поиска и возврат к нему;
- отмена/возврат и фактическая денежная ценность booking.
