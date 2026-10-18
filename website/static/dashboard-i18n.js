/*
 * Английская версия дашборда.
 *
 * Разметка и скрипты дашборда пишут текст по-русски. В режиме EN этот файл
 * переводит видимый текст по словарю: при загрузке и потом всё, что скрипты
 * дорисовывают (таблицы, уведомления, подсказки) — через MutationObserver.
 * Строки, которых нет в словаре, остаются русскими, поэтому новая фраза
 * в дашборде ничего не ломает — её просто нужно добавить сюда.
 *
 * Язык хранится в localStorage('lang') — общий с главной страницей.
 */
(function () {
  const DICT = {
    // Навигация и общее
    'Обзор': 'Overview', 'Настройки': 'Settings', 'Данные': 'Data', 'Общие': 'General',
    'Логирование': 'Logging', 'Модерация': 'Moderation', 'Автоматизация': 'Automation',
    'Безопасность': 'Security', 'Внешний вид': 'Appearance', 'Поиск участника': 'Member lookup',
    'Апелляции': 'Appeals', 'Заявки на бан': 'Ban requests', 'Инвайты': 'Invites',
    'Разделы': 'Sections', 'Меню': 'Menu', 'Выйти': 'Log out', 'Закрыть': 'Close',
    'Сохранить': 'Save', 'Сохраняю...': 'Saving...', 'Обновить': 'Refresh', '⟳ Обновить': '⟳ Refresh',
    'Сбросить': 'Reset', 'Загрузка...': 'Loading...', 'Загрузка серверов...': 'Loading servers...',
    'Поиск...': 'Search...', 'Все': 'All', 'Все →': 'All →', 'Пока пусто': 'Nothing yet',
    'Ничего не найдено': 'Nothing found', 'Никого не найдено': 'No one found', 'Нет данных': 'No data',
    'Не удалось загрузить': 'Could not load', 'Не удалось сохранить': 'Could not save',
    'Не удалось обновить': 'Could not update', 'Не удалось сохранить часть настроек': 'Some settings could not be saved',
    'Ошибка соединения': 'Connection error', 'Ошибка:': 'Error:', '✓ Сохранено': '✓ Saved', '✓ Готово': '✓ Done',
    'Переход по разделам или поиск участника...': 'Jump to a section or find a member...',
    'Бот перезапускается после обновления — переподключимся автоматически, обычно это 1–3 минуты.':
      'The bot is restarting after an update — we will reconnect automatically, usually within 1–3 minutes.',
    'Русский': 'Russian', 'Язык': 'Language',

    // Вход и выбор сервера
    'Войти через Discord': 'Log in with Discord',
    'Войди через Discord чтобы управлять серверами где есть бот': 'Log in with Discord to manage servers that have the bot',
    '← на главную': '← back to home', 'Выбери сервер': 'Choose a server', 'Сервер': 'Server',
    'Нет доступных серверов': 'No servers available',
    'Добавь бота на сервер где у тебя есть права администратора': 'Add the bot to a server where you are an administrator',
    'Модерация и безопасность': 'Moderation and security',

    // Обзор
    'Обзор сервера': 'Server overview', 'Статистика сервера в реальном времени': 'Live server statistics',
    'Участников': 'Members', 'Действий модерации': 'Moderation actions', 'Активных апелляций': 'Open appeals',
    'Последние действия': 'Recent actions', '5 последних записей из mod log': 'Last 5 mod log entries',
    'Действие': 'Action', 'Участник': 'Member', 'Модератор': 'Moderator', 'Причина': 'Reason', 'Когда': 'When',
    'Длительность': 'Duration', 'Mod log пуст': 'Mod log is empty',
    'Последние 100 действий модерации': 'Last 100 moderation actions',

    // Общие настройки
    'Общие настройки': 'General settings', 'Язык, тикеты и базовые каналы': 'Language, tickets and basic channels',
    'Базовое': 'Basics', 'Язык, тикеты, каналы по умолчанию': 'Language, tickets, default channels',
    'Язык бота': 'Bot language', 'Канал для дней рождения': 'Birthday channel',
    'Система тикетов': 'Ticket system', 'Позволяет участникам создавать тикеты поддержки': 'Lets members open support tickets',
    'Текущая конфигурация': 'Current configuration', 'Сводка активных настроек сервера': 'Summary of active server settings',
    '— нет —': '— none —', '— выбери канал —': '— choose a channel —', '— выбери роль —': '— choose a role —',
    '— выключено —': '— off —', '— не выбрано —': '— not selected —', 'Не выбран': 'Not set',
    'Лог-канал': 'Log channel', 'Тикеты': 'Tickets', '✅ вкл': '✅ on', '✅ вкл ·': '✅ on ·', '⬚ выкл': '⬚ off',
    '1-й / 2-й варн': '1st / 2nd warning', '3-й варн': '3rd warning', 'дн.': 'd', 'дн. /': 'd /',
    'дн. мин. возраст': 'd min. age', '🔨 Авто-бан': '🔨 Auto-ban', '🔇 Мут': '🔇 Mute', '⚖️ Заявка на бан': '⚖️ Ban request',

    // Логирование
    'Куда бот пишет события': 'Where the bot posts events', 'Каналы для логов': 'Log channels',
    'Куда бот будет писать события сервера': 'Where the bot will post server events',
    'Главный лог-канал': 'Main log channel',
    'Логи модерации, наказаний, апелляций, заявок на бан': 'Moderation, punishment, appeal and ban request logs',
    'Канал для предложений': 'Suggestions channel', 'Канал starboard': 'Starboard channel',
    'Какие события логировать': 'Which events to log',
    'Бот пишет в главный лог-канал только отмеченные события': 'The bot posts only the selected events to the main log channel',
    'вкл/выкл все': 'toggle all',
    'Участники': 'Members', 'Сообщения': 'Messages',
    'Входы на сервер': 'Joins', 'Выходы с сервера': 'Leaves', 'Баны': 'Bans', 'Кики': 'Kicks',
    'Таймауты': 'Timeouts', 'Смена никнейма': 'Nickname changes', 'Изменение ролей': 'Role changes',
    'Смена аватара': 'Avatar changes', 'Подозрительные аккаунты': 'Suspicious accounts',
    'Удаление сообщений': 'Deleted messages', 'Правка сообщений': 'Edited messages', 'Реакции': 'Reactions',
    'Ветки': 'Threads', 'Голосовые каналы': 'Voice channels', 'Создание/удаление каналов': 'Channels created/deleted',
    'Создание/удаление ролей': 'Roles created/deleted', 'Изменение сервера': 'Server changes',
    'Использование команд': 'Command usage', 'Анти-рейд': 'Anti-raid', 'Анти-спам': 'Anti-spam',

    // Модерация
    'Наказания и карантин': 'Punishments and quarantine', 'Прогрессивные наказания': 'Progressive punishments',
    '3-варн система с настраиваемой длительностью': 'Three-warning system with configurable durations',
    '1-й варн → таймаут (дней)': '1st warning → timeout (days)', '2-й варн → таймаут (дней)': '2nd warning → timeout (days)',
    'Макс. 27 дней (лимит Discord)': 'Max. 27 days (Discord limit)', '3-й варн → действие': '3rd warning → action',
    '🔇 Мут (как 2-й варн)': '🔇 Mute (like the 2nd warning)', '⚖️ Заявка на бан (вручную)': '⚖️ Ban request (manual)',
    'Дней бана (если выбран бан)': 'Ban days (if ban is chosen)', 'Карантин': 'Quarantine',
    'Авто-роль для аккаунтов младше N дней': 'Automatic role for accounts younger than N days',
    'Карантин включён': 'Quarantine on', 'Новые аккаунты автоматически получают роль': 'New accounts get the role automatically',
    'Роль карантина': 'Quarantine role', 'Мин. возраст аккаунта (дней)': 'Min. account age (days)',
    'Длительность карантина (часов)': 'Quarantine duration (hours)',

    // Внешний вид
    'Личная тема — хранится в этом браузере': 'Personal theme — stored in this browser', 'Готовые темы': 'Themes',
    'Клик — применяется мгновенно': 'Click to apply instantly', 'Фоновое изображение': 'Background image',
    'Своя картинка вместо однотонного фона · хранится в браузере': 'Your own picture instead of a plain background · stored in the browser',
    '📷 Загрузить фото': '📷 Upload photo', '✕ Убрать фон': '✕ Remove background', 'Затемнение фона': 'Background dimming',
    'Чем выше — тем читабельнее текст поверх фото': 'Higher means more readable text over the photo',
    'Стеклянные панели': 'Glass panels',
    'Карточки становятся полупрозрачными с blur — фон виден сквозь них': 'Cards become translucent with blur — the background shows through',
    'Анимация': 'Animation', 'Живой фон в стиле выбранной темы': 'Animated background in the theme’s style',
    'Тематическая анимация': 'Theme animation',
    'Emerald — матричный дождь · Frost — снег · Crimson — угли...': 'Emerald — matrix rain · Frost — snow · Crimson — embers...',
    'Интенсивность': 'Intensity', 'Свои цвета': 'Custom colors', 'Тонкая настройка поверх выбранной темы': 'Fine-tune on top of the theme',
    'Акцент': 'Accent', 'Кнопки, ссылки, активные элементы': 'Buttons, links, active elements', 'Фон': 'Background',
    'Основной задний фон': 'Main background', 'Панели': 'Panels', 'Карточки, сайдбар, таблицы': 'Cards, sidebar, tables',
    'Текст': 'Text', 'Основной цвет текста': 'Main text color', 'Текущая тема →': 'Current theme →',
    'Тема сброшена': 'Theme reset', '✓ Фон установлен': '✓ Background set', 'Фон убран': 'Background removed',
    'Это не изображение': 'This is not an image', 'Фото слишком большое даже после сжатия': 'The photo is too large even after compression',
    'Не хватило места в браузере — попробуй фото поменьше': 'Not enough browser storage — try a smaller photo',
    'матричный дождь': 'matrix rain', 'снегопад': 'snowfall', 'угли': 'embers', 'пузыри': 'bubbles',
    'светлячки': 'fireflies', 'частицы': 'particles', 'пауза': 'paused',

    // Автоматизация
    'Фоновые задачи бота': 'Bot background tasks', 'Автоочистка инвайтов': 'Invite cleanup',
    'Раз в 2 дня удаляет приглашения без единого перехода': 'Every 2 days deletes invites that were never used',
    'Включена': 'Enabled', 'Удаляются только инвайты с 0 переходов': 'Only invites with 0 uses are deleted',
    'Удалять старше (дней)': 'Delete older than (days)', 'Канал для отчёта': 'Report channel',
    'Сообщения с реакциями ⭐ попадают в отдельный канал': 'Messages with ⭐ reactions go to a separate channel',
    'Порог звёзд': 'Star threshold', 'Дни рождения': 'Birthdays', 'Бот поздравляет участников в 09:00 UTC': 'The bot greets members at 09:00 UTC',
    'Канал для поздравлений': 'Greetings channel',

    // Безопасность
    'AutoMod, угрозы и анти-нюк': 'AutoMod, threats and anti-nuke', 'Правил AutoMod активно': 'Active AutoMod rules',
    'Нерешённых угроз за 7 дней': 'Unresolved threats, last 7 days', 'Правила защиты': 'Protection rules',
    'Что делать с угрозой: пропустить, прислать алерт модераторам или заблокировать сообщение до отправки':
      'What to do with a threat: ignore it, alert moderators, or block the message before it is sent',
    'Фишинг-ссылки': 'Phishing links', 'Скам': 'Scam', 'Спам': 'Spam', 'Свои слова': 'Own words',
    'Поддельные домены Discord и Steam: discord-nitro.gift, steamcommunlty.com и похожие':
      'Fake Discord and Steam domains: discord-nitro.gift, steamcommunlty.com and the like',
    'Фейковые раздачи Nitro и Steam, крипто-схемы «удвою», ссылки с подменой букв':
      'Fake Nitro and Steam giveaways, “I’ll double it” crypto schemes, links with look-alike letters',
    'Встроенный детектор спама Discord и повтор одного и того же текста': 'Discord’s built-in spam filter and repeated text',
    'Слова и домены, которые запрещены именно на этом сервере — список ниже': 'Words and domains banned on this server — list below',
    'Выкл': 'Off', 'Алерт': 'Alert', 'Блок': 'Block',
    'выключено': 'off', 'проверяет сам бот': 'checked by the bot itself', 'правило не создано': 'rule not created',
    'на сервере уже есть своё правило спама': 'the server already has its own spam rule',
    'добавьте слова в поле ниже': 'add words in the field below',
    'У бота нет права': 'The bot lacks the', '«Управлять сервером»': '“Manage Server”',
    '— правила AutoMod не создаются. Выдайте право, и режимы применятся.': 'permission — AutoMod rules are not created. Grant it and the modes will apply.',
    'лог-канал': 'log channel',
    '(Логирование) — режиму «Алерт» некуда писать, а «Блок» сработает без уведомления модераторам.':
      '(Logging) is set — “Alert” has nowhere to post, and “Block” will work without notifying moderators.',
    'Свои слова и домены — по одному на строку': 'Own words and domains — one per line',
    'слово': 'word', 'слово*': 'word*', '*слово': '*word', '*слово*': '*word*',
    '— целиком,': '— whole word,', '— начало,': '— start,', '— конец,': '— end,',
    '— где угодно. Слова, которые задели бы обычные ссылки (discord.com, youtube.com, github.com…), не сохранятся, а сами эти домены AutoMod всегда пропускает.':
      '— anywhere. Words that would hit ordinary links (discord.com, youtube.com, github.com…) are not saved, and AutoMod always lets those domains through.',
    'Эти слова не сохранены:': 'These words were not saved:', 'Исправьте список слов': 'Fix the word list',
    '✓ Режимы сохранены и применены': '✓ Modes saved and applied',
    'Исключения': 'Exemptions',
    'Роли и каналы, на которые правила защиты не действуют: например, модераторы или канал для ссылок':
      'Roles and channels the protection rules skip: for example, moderators or a links channel',
    'Роли': 'Roles', 'Каналы': 'Channels', '+ добавить роль': '+ add role', '+ добавить канал': '+ add channel',
    'Последние угрозы': 'Recent threats', 'Срабатывания AutoMod и проверок бота, до 100 последних': 'AutoMod and bot check hits, up to the last 100',
    'Любой уровень': 'Any severity', 'Высокий и критический': 'High and critical', 'Средний': 'Medium', 'Низкий': 'Low',
    'Любой источник': 'Any source', 'Бот': 'Bot', 'бот': 'bot', 'Показывать решённые': 'Show resolved',
    'Уровень': 'Severity', 'Событие': 'Event', 'Источник': 'Source', 'Решено': 'Resolve', 'Вернуть': 'Reopen',
    'Угроз не найдено': 'No threats found', 'Анти-нюк': 'Anti-nuke',
    'Лимиты на массовые действия модераторов': 'Limits on mass moderator actions',
    'Лимит': 'Limit', 'За период': 'Per period', 'Муты': 'Mutes', 'Удаление каналов': 'Channel deletions',
    'Удаление ролей': 'Role deletions', '30 сек': '30 s', '20 сек': '20 s',
    'При превышении действие блокируется, а владелец сервера получает уведомление. Владелец сервера из проверки исключён.':
      'Going over the limit blocks the action and notifies the server owner. The owner is not checked.',
    'фишинг-ссылки': 'phishing links', 'подозрительные сообщения': 'suspicious messages', 'спам': 'spam', 'свои слова': 'own words',

    // Анти-нюк, анти-рейд, сгорание варнов
    'Защита от взломанного или недобросовестного модератора: лимиты на массовые действия и что делать при превышении':
      'Protection against a compromised or rogue moderator: limits on mass actions and what to do when exceeded',
    'Анти-нюк включён': 'Anti-nuke on', 'Владелец сервера, Witness и доверенные роли не проверяются': 'The server owner, Witness and trusted roles are not checked',
    'При превышении лимита': 'When a limit is exceeded', 'Только уведомить': 'Only notify',
    'Снять опасные роли': 'Remove dangerous roles', 'Снять роли и таймаут на час': 'Remove roles and time out for an hour',
    'Опасные — роли с правами бана, кика, таймаута, управления сервером, ролями, каналами или вебхуками':
      'Dangerous roles are those with ban, kick, timeout, manage server, roles, channels or webhooks permissions',
    'Доверенные роли': 'Trusted roles',
    'Например, старшие модераторы, которые чистят сервер после рейда массовыми банами':
      'For example, senior moderators who clean up after a raid with mass bans',
    'Не больше': 'At most', 'За (сек)': 'Within (s)', 'Создание вебхуков': 'Webhooks created',
    'Добавили бота': 'A bot was added', 'Ничего не делать': 'Do nothing', 'Уведомить': 'Notify', 'Выгнать бота': 'Kick the bot',
    'Если бота добавил не владелец и не доверенная роль': 'If the bot was added by someone other than the owner or a trusted role',
    'Выдали опасные права': 'Dangerous permissions granted', 'Откатить': 'Revert',
    'Роли дали «Администратор», «Управлять сервером» и т.п., или такую роль выдали участнику':
      'A role got “Administrator”, “Manage Server” and the like, or such a role was given to a member',
    'Волна входов за короткое время: что делать с участниками волны и включать ли локдаун':
      'A wave of joins in a short time: what to do with the wave and whether to turn on lockdown',
    'Анти-рейд включён': 'Anti-raid on', 'Локдаун сейчас включён': 'Lockdown is on now', 'Локдаун сейчас выключен': 'Lockdown is off now',
    'Рейд — это входов': 'A raid is this many joins', 'За (секунд)': 'Within (seconds)',
    'Участников волны': 'Members of the wave', 'Не трогать, только уведомить': 'Leave them, only notify',
    'Кикнуть': 'Kick', 'Отправить в карантин': 'Send to quarantine',
    'Карантин использует роль из «Модерация → Карантин»': 'Quarantine uses the role from “Moderation → Quarantine”',
    'Авто-локдаун (минут)': 'Auto-lockdown (minutes)',
    'На это время новые аккаунты младше минимального возраста не смогут зайти. 0 — не включать':
      'For this long, accounts younger than the minimum age cannot join. 0 — don’t turn on',
    'Анти-рейд работает на тарифе': 'Anti-raid works on the', '. Настройки сохранятся и включатся вместе с тарифом.':
      ' plan. Settings are saved and take effect with the plan.',
    'Варн сгорает через (дней)': 'A warning expires after (days)',
    '0 — варны не сгорают. Сгоревший варн виден в истории, но к наказанию не ведёт':
      '0 — warnings never expire. An expired warning stays in the history but no longer leads to punishment',

    // Конструктор правил
    'Защита от взломанного или недобросовестного модератора. Собери правила: что считать атакой и что делать':
      'Protection against a compromised or rogue moderator. Build the rules: what counts as an attack and what to do',
    'Если модератор сделает больше…': 'If a moderator makes more than…',
    'банов': 'bans', 'киков': 'kicks', 'мутов': 'mutes', 'удалений каналов': 'channel deletions',
    'удалений ролей': 'role deletions', 'созданных вебхуков': 'webhooks created', 'за': 'within', 'сек': 's', 'мин': 'min',
    'Тогда': 'Then', 'Что делать при превышении': 'What to do when exceeded', 'Снять роли + таймаут 1 ч': 'Remove roles + 1 h timeout',
    'Опасные роли — с правами бана, кика, таймаута, управления сервером, ролями, каналами или вебхуками. Роли выше роли Witness бот снять не может и напишет об этом.':
      'Dangerous roles have ban, kick, timeout, manage server, roles, channels or webhooks permissions. The bot cannot remove roles above its own and will say so.',
    'Ещё следить за': 'Also watch for', 'Добавление ботов': 'Bots being added',
    'Если кто-то добавит на сервер бота': 'If someone adds a bot to the server',
    'Выдача опасных прав': 'Dangerous permissions being granted',
    'Если роли или участнику выдадут «Администратор», «Управлять сервером» и т.п.':
      'If a role or a member is given “Administrator”, “Manage Server” and the like',
    'Не проверять': 'Don’t check', 'Владелец': 'Owner', 'Добавить доверенную роль': 'Add a trusted role',
    '+ доверенная роль': '+ trusted role', 'Меньше': 'Less', 'Больше': 'More',
    'Если за': 'If within', 'сек зайдут': 's', 'человек': 'people join',
    'Что делать с участниками волны': 'What to do with the wave', 'В карантин': 'Quarantine',
    'Авто-локдаун': 'Auto-lockdown', 'и включить локдаун на': 'and turn on lockdown for',
    'Локдаун': 'Lockdown', 'не пускает аккаунты моложе': 'keeps out accounts younger than',
    'дн. — и при авто-локдауне, и после': 'days — both during auto-lockdown and after',
    'Карантин использует роль из «Модерация → Карантин». Если она не настроена — участники волны будут кикнуты':
      'Quarantine uses the role from “Moderation → Quarantine”. If it is not set, the wave is kicked instead',
    'Сгорание варнов': 'Warning expiry', 'Варн сгорает через': 'A warning expires after',
    'Сгоревший варн остаётся в истории, но к наказанию больше не ведёт. Выключено — варны не сгорают':
      'An expired warning stays in the history but no longer leads to punishment. Off — warnings never expire',

    // Поиск участника
    'Полное досье: наказания, апелляции, инвайты': 'Full record: punishments, appeals, invites',
    'Найди участника': 'Find a member', 'Ник или ID участника...': 'Member nickname or ID...',
    'Введи ник или ID выше, затем кликни по результату': 'Enter a nickname or ID above, then click a result',
    'Аккаунт создан': 'Account created', 'На сервере с': 'On server since', 'Варнов:': 'Warnings:',
    'История наказаний': 'Punishment history', 'Наказаний не было': 'No punishments', 'Наказание': 'Punishment',
    'Срок': 'Term', 'без нарушений': 'no violations', 'в муте': 'muted', 'не на сервере': 'not on the server',
    'Не удалось загрузить досье': 'Could not load the record', 'Ошибка при загрузке досье': 'Error loading the record',

    // Mod log, апелляции, заявки, инвайты
    'Заявки участников на снятие наказаний': 'Members’ requests to lift punishments',
    '⏳ Ожидают': '⏳ Pending', '⏳ Ожидают:': '⏳ Pending:', '✅ Принятые': '✅ Accepted', '✅ Приняты:': '✅ Accepted:',
    '❌ Отклонённые': '❌ Rejected', '❌ Отклонены:': '❌ Rejected:', '✅ Одобренные': '✅ Approved',
    '✅ Одобрено:': '✅ Approved:', '❌ Отклонено:': '❌ Rejected:', 'Нет апелляций': 'No appeals', 'Нет заявок': 'No requests',
    'с этим статусом': 'with this status', 'Объяснение': 'Explanation', 'Будет показан участнику...': 'Will be shown to the member...',
    '✓ Апелляция принята': '✓ Appeal accepted', '✗ Апелляция отклонена': '✗ Appeal rejected',
    '✗ Заявка отклонена': '✗ Request rejected', '⚠️ Одобрено, но бан не выдан (нет прав у бота)': '⚠️ Approved, but the ban failed (the bot lacks permissions)',
    'Созданы при 3-м варне · решение за администратором': 'Created on the 3rd warning · an administrator decides',
    'Кто кого пригласил на сервер': 'Who invited whom', 'Поиск по коду, имени, заметке...': 'Search by code, name, note...',
    'Код': 'Code', 'Создал': 'Created by', 'Использований': 'Uses', 'Последний раз': 'Last used', 'Заметка': 'Note',
    'Список обновляется автоматически': 'The list updates automatically',
    '«Банить участников»': '“Ban Members”', '«Модерировать участников»': '“Moderate Members”', '«Управлять сообщениями»': '“Manage Messages”',

    // Тексты алертов, которые пишет бот
    'AutoMod: фишинг-ссылка заблокирована': 'AutoMod: phishing link blocked',
    'AutoMod: подозрение на фишинг/скам': 'AutoMod: suspected phishing/scam',
    'AutoMod: подозрение на спам': 'AutoMod: suspected spam',
    'Сообщение в канале-ловушке': 'Message in the trap channel',

    // Дополнительная защита: ответ на рейд, фильтр входа, взломанные аккаунты
    'и удалить инвайт, по которому шла волна': 'and delete the invite the wave came through',
    'и приостановить вход по инвайтам на': 'and pause joining by invites for',
    'а заодно и личные сообщения между участниками сервера': 'and also direct messages between server members',
    'Пауза — штатная функция Discord, как «Приостановить приглашения» в настройках сервера. Не дольше суток, снимается сама':
      'The pause is a built-in Discord feature, like “Pause invites” in server settings. At most a day, lifts on its own',
    'Фильтр входа': 'Join filter',
    'Проверка каждого нового участника — всегда, а не только в локдаун': 'Checks every new member — always, not only during lockdown',
    'Если аккаунту меньше': 'If the account is younger than', 'или у него нет аватарки': 'or it has no avatar',
    'Что делать с новым участником': 'What to do with the new member', 'Без аватарки': 'No avatar',
    '0 дней — возраст не проверяется. Владелец и доверенные роли анти-нюка не проверяются':
      '0 days — age is not checked. The owner and anti-nuke trusted roles are not checked',
    'Взломанные аккаунты и спам': 'Compromised accounts and spam',
    'Ловят типичную рассылку со взломанного аккаунта: скам-ссылки, @everyone, одно и то же во все каналы':
      'Catch the typical spam from a hijacked account: scam links, @everyone, the same message in every channel',
    'Если кто-то напишет в канал-ловушку': 'If someone posts in the trap channel', '— выбери канал —': '— choose a channel —',
    'Канал-ловушка': 'Trap channel', 'Софтбан': 'Softban', 'Таймаут на сутки': 'Time out for a day', 'Бан': 'Ban',
    'Создай отдельный канал с подписью «Не пишите сюда — бан». Люди туда не пишут, а спам-боты и взломанные аккаунты пишут во все каналы подряд. Софтбан — бан и сразу разбан: сообщения за сутки удаляются, а владелец аккаунта может вернуться, когда восстановит его. Бот пришлёт ему в ЛС, как это сделать':
      'Create a separate channel labelled “Don’t post here — ban”. People don’t post there, but spam bots and hijacked accounts post in every channel. Softban is a ban followed by an unban: the last day of messages is deleted, and the owner can come back once they recover the account. The bot DMs them how to do that',
    'Если одно и то же сообщение придёт': 'If the same message arrives', 'раз за': 'times within',
    'сек в разные каналы': 's in different channels', 'удалить копии и таймаут на': 'delete the copies and time out for',
    'Одинаковые сообщения в разных каналах': 'Same message in different channels',
    'Нагрев за частые сообщения, чувствительность': 'Heat for rapid posting, sensitivity',
    'Каждое сообщение даёт очки — ссылки, упоминания, @everyone и вложения больше, — и со временем они остывают. Перегрелся — таймаут, повторно за сутки — вдвое дольше':
      'Every message adds points — links, mentions, @everyone and attachments add more — and they cool down over time. Overheat means a timeout, twice as long for each repeat within a day',
    'Нагрев': 'Heat', 'Чувствительность нагрева': 'Heat sensitivity',
    'Низкая': 'Low', 'Средняя': 'Medium', 'Высокая': 'High', 'таймаут на': 'time out for',
    'Если ссылка есть в базах фишинга': 'If a link is in the phishing lists',
    'Проверка по базам фишинга': 'Phishing list check', 'Что делать со ссылкой из базы': 'What to do with a listed link',
    'Удалить сообщение': 'Delete the message',
    'Открытые базы Sinking Yachts и Discord-AntiScam (≈45 тыс. доменов, обновляются раз в 6 ч) скачиваются целиком, ссылки проверяются у бота — никуда не отправляются. Роли и каналы из карточки «Исключения» выше действуют и здесь (кроме ловушки)':
      'The open Sinking Yachts and Discord-AntiScam lists (≈45k domains, refreshed every 6 h) are downloaded whole and links are checked by the bot — they are not sent anywhere. Roles and channels from the “Exemptions” card above apply here too (except the trap)',
    'Для всех фильтров на этой карточке': 'For all filters on this card',
    'Проверять и модераторов': 'Check moderators too',
    'Их аккаунты тоже взламывают. Модератора бот не банит: только таймаут и сообщение владельцу сервера':
      'Their accounts get hijacked too. The bot never bans a moderator: only a timeout and a message to the server owner',
    'Проверять модераторов': 'Check moderators', 'Режим наблюдения': 'Observe mode',
    'Режим наблюдения — никого не наказывать, только писать в лог': 'Observe mode — punish no one, only log',
    'В логе видно, что было бы сделано, — удобно, чтобы сначала подобрать пороги. Действует и на «Фильтр входа», и на паузу инвайтов и удаление инвайта при рейде':
      'The log shows what would have been done — handy for tuning thresholds first. Also covers the “Join filter” and the invite pause and invite deletion during a raid',
    'Канал-ловушка не выбран — ловушка не сработает': 'No trap channel selected — the trap will not work',

    // Инструменты модераторов, заметки
    'Инструменты модераторов': 'Moderator tools',
    'Кейсы, заметки, отмена авто-наказаний и подсказки ИИ по жалобам': 'Cases, notes, undoing auto-punishments and AI hints on reports',
    'Кейсы и заметки': 'Cases and notes', 'У каждой записи в': 'Every entry in',
    'и в «Поиске участника» — номер кейса.': 'and in “Member lookup” gets a case number.',
    'показывает кейс и меняет причину,': 'shows a case and edits its reason,',
    '— заметки об участнике, которые видят только модераторы': '— notes about a member that only moderators see',
    'Кнопка «Отменить» в логах': '“Undo” button in logs', 'Кнопка Отменить': 'Undo button',
    'Под логом автоматического таймаута, бана или карантина (ловушка, нагрев, фильтр входа и т.п.) — кнопка, которая снимает наказание':
      'Under the log of an automatic timeout, ban or quarantine (trap, heat, join filter, etc.) — a button that lifts the punishment',
    'ИИ-разбор жалоб': 'AI report triage', 'К жалобе из': 'A report from',
    'добавляется оценка ИИ: приоритет, токсичность, угрозы, спам. Текст сообщения, на которое пожаловались, отправляется в Groq или Google Gemini. Наказывает всегда модератор':
      'gets an AI assessment: priority, toxicity, threats, spam. The text of the reported message is sent to Groq or Google Gemini. A moderator always makes the call',
    'Заметки модераторов': 'Moderator notes', 'Участник их не видит. В Discord —': 'The member cannot see them. In Discord —',
    'Заметок пока нет': 'No notes yet', 'Новая заметка': 'New note', 'Добавить': 'Add', 'Удалить': 'Delete',
    'Например: предупреждён устно в войсе': 'For example: warned verbally in voice',
    'Напиши текст заметки': 'Write the note text',
  };

  // Строки с числами и вставками: [шаблон, замена]
  const tr = s => DICT[s] ?? s;
  const PATTERNS = [
    [/^(\d+) участников$/, (m, n) => `${n} members`],
    [/^Апелляция #(\d+)$/, (m, n) => `Appeal #${n}`],
    [/^варнов: (\d+)\/3$/, (m, n) => `warnings: ${n}/3`],
    [/^⚖️ Новых апелляций: (\d+)$/, (m, n) => `⚖️ New appeals: ${n}`],
    [/^🔨 Новых заявок на бан: (\d+)$/, (m, n) => `🔨 New ban requests: ${n}`],
    [/^🔨 Бан на (\d+) дней выдан$/, (m, n) => `🔨 ${n}-day ban issued`],
    [/^Инициатор: (.*?)(?: · Решил: (.*))?$/, (m, a, b) => `Requested by: ${a}` + (b ? ` · Decided by: ${b}` : '')],
    [/^код (\d+)$/, (m, n) => `code ${n}`],
    [/^Лимит: (.+)$/, (m, x) => `Limit: ${tr(x)}`],
    [/^Убрать (.+)$/, (m, x) => `Remove ${x}`],
    [/^(.+): режим$/, (m, x) => `${tr(x)}: mode`],
    [/^AutoMod: (блокирует|алерт)(?: \+ (алерт))?$/, (m, a, b) =>
      'AutoMod: ' + [a, b].filter(Boolean).map(x => ({ 'блокирует': 'blocks', 'алерт': 'alert' })[x]).join(' + ')],
    [/^AutoMod: (.+) — (заблокировано|без блокировки)$/, (m, x, how) =>
      `AutoMod: ${({ 'фишинг-ссылка': 'phishing link', 'подозрение на фишинг/скам': 'suspected phishing/scam',
                    'подозрение на спам': 'suspected spam', 'запрещённое слово сервера': 'server-banned word' })[x] || x}`
      + (how === 'заблокировано' ? ' — blocked' : ' — not blocked')],
    [/^Анти-нюк: (\d+) (банов|киков|мутов|удалений каналов|удалений ролей|созданных вебхуков) за (\d+) сек\.$/, (m, n, what, s) =>
      `Anti-nuke: ${n} ${({ 'банов': 'bans', 'киков': 'kicks', 'мутов': 'mutes', 'удалений каналов': 'channel deletions',
                           'удалений ролей': 'role deletions', 'созданных вебхуков': 'webhooks created' })[what]} in ${s} s`],
    [/^Анти-рейд: (\d+) входов за (\d+) сек\.$/, (m, n, s) => `Anti-raid: ${n} joins in ${s} s`],
    [/^Добавлен бот (\d+)$/, (m, id) => `Bot added: ${id}`],
    [/^Выданы опасные права: (.+)$/, (m, list) => 'Dangerous permissions granted: ' + list.split(', ').map(p => ({
      'Администратор': 'Administrator', 'Управлять сервером': 'Manage Server', 'Управлять ролями': 'Manage Roles',
      'Банить участников': 'Ban Members', 'Управлять вебхуками': 'Manage Webhooks' })[p] || p).join(', ')],
    [/^короче (\d+) символов$/, (m, n) => `shorter than ${n} characters`],
    [/^длиннее (\d+) символов$/, (m, n) => `longer than ${n} characters`],
    [/^звёздочка только в начале или в конце$/, () => 'an asterisk is only allowed at the start or end'],
    [/^заблокирует обычную ссылку или текст: (.+)$/, (m, x) => `would block an ordinary link or text: ${x}`],
  ];

  function translate(text) {
    // Переносы и отступы внутри абзаца из разметки — как один пробел
    const t = text.replace(/\s+/g, ' ').trim();
    if (!t || !/[А-Яа-яЁё]/.test(t)) return null;
    let r = DICT[t];
    if (r === undefined) {
      for (const [re, fn] of PATTERNS) {
        const m = t.match(re);
        if (m) { r = fn(...m); break; }
      }
    }
    if (r === undefined) return null;
    // Пробелы по краям сохраняем: от них зависят отступы между словами и тегами
    return text.match(/^\s*/)[0] + r + text.match(/\s*$/)[0];
  }

  const ATTRS = ['placeholder', 'title', 'aria-label'];
  const SKIP = new Set(['SCRIPT', 'STYLE', 'TEXTAREA']);

  function translateTree(root) {
    if (root.nodeType === Node.TEXT_NODE) {
      if (root.parentNode && !SKIP.has(root.parentNode.nodeName)) {
        const r = translate(root.nodeValue);
        if (r !== null) root.nodeValue = r;
      }
      return;
    }
    if (root.nodeType !== Node.ELEMENT_NODE || SKIP.has(root.nodeName)) return;
    for (const el of [root, ...root.querySelectorAll('*')]) {
      for (const a of ATTRS) {
        const v = el.getAttribute(a);
        if (v) { const r = translate(v); if (r !== null) el.setAttribute(a, r); }
      }
    }
    const walker = document.createTreeWalker(root, NodeFilter.SHOW_TEXT);
    const nodes = [];
    while (walker.nextNode()) nodes.push(walker.currentNode);
    nodes.forEach(translateTree);
  }

  function getLang() {
    let lang;
    try { lang = localStorage.getItem('lang'); } catch (e) {}
    return lang || (/^(ru|uk|be|kk)/i.test(navigator.language || '') ? 'ru' : 'en');
  }

  window.witnessSetLang = function (lang) {
    try { localStorage.setItem('lang', lang); } catch (e) {}
    location.reload();   // русский текст — исходный, проще всего перерисовать страницу
  };

  function start() {
    const lang = getLang();
    document.querySelectorAll('[data-lang-toggle]').forEach(b => {
      b.textContent = lang === 'en' ? 'RU' : 'EN';
      b.setAttribute('aria-label', lang === 'en' ? 'Переключить на русский' : 'Switch to English');
      b.addEventListener('click', () => window.witnessSetLang(lang === 'en' ? 'ru' : 'en'));
    });
    if (lang !== 'en') return;
    document.documentElement.lang = 'en';
    translateTree(document.body);
    new MutationObserver(muts => {
      for (const m of muts) {
        if (m.type === 'characterData') translateTree(m.target);
        else if (m.type === 'attributes') translateTree(m.target);
        else m.addedNodes.forEach(translateTree);
      }
    }).observe(document.body, { subtree: true, childList: true, characterData: true,
                                attributes: true, attributeFilter: ATTRS });
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', start);
  else start();
})();
