# сбор сообщений из telegram каналов и отправка их в kafka в виде JSON

from telethon import TelegramClient, events
from telethon.tl.functions.channels import JoinChannelRequest
from kafka import KafkaProducer
import json
import asyncio

# параметры telegram app
#API_ID = secret
#API_HASH = secret

# имя локального файла сессии telethon
SESSION_NAME = "tg_kafka_session"

# параметры kafka
KAFKA_SERVERS = "localhost:9092"
KAFKA_TOPIC = "telegram_messages"

# ID telegram каналов:
# канал Марка, форсаж, дрифт экспо, МГТУ им Баумана, го хард, мой тг канал, deadp47, ds тесты, анекдоты, москва 24/7, пекарня, москва афиша, вопросы собесов, интересная москва
TARGET_CHANNEL_IDS = [
    -1002172717570, -1001699887755, -1001170073337, -1001144182771, -1001176410367, -1002023697245, -1001408669138,
    -1002075081423, -1001743905774, -1002026660192, -1001418440636, -1001942030635, -1001863771680, -1001125816841
]

# инициализация клиента telegram и продюсера kafka
tg_client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
kafka_producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVERS,
    value_serializer=lambda value: json.dumps(
        value, ensure_ascii=False
    ).encode("utf-8"),
)


async def message_handler(event):
    """
    Обработчик новых сообщений
    """
    sender = await event.get_sender()
    username = sender.username or str(sender.id)

    msg_info = {
        "username": username,
        "timestamp": event.message.date.isoformat(),
    }

    print(f"Новое сообщение от {username} в {msg_info['timestamp']}")

    kafka_producer.send(KAFKA_TOPIC, msg_info)
    kafka_producer.flush()


async def ensure_join_channels():
    """
    Пытается вступить во все указанные каналы
    """
    for channel_id in TARGET_CHANNEL_IDS:
        try:
            entity = await tg_client.get_entity(channel_id)
            await tg_client(JoinChannelRequest(entity))
            print(f"Успешно вступил в канал {channel_id}")
        except Exception as error:
            print(f"Не удалось вступить в канал {channel_id}: {error}")


async def run():
    
    await tg_client.start()

    # регистрируем обработчик для новых сообщений из нужных каналов
    tg_client.add_event_handler(
        message_handler,
        events.NewMessage(chats=TARGET_CHANNEL_IDS),
    )

    await ensure_join_channels()

    print("telegram клиент работает и ждет сообщений...")
    await tg_client.run_until_disconnected()


if __name__ == "__main__":
    asyncio.run(run())
