from faststream.rabbit import Channel, RabbitBroker, RabbitExchange, RabbitQueue

PAYMENTS_EXCHANGE = RabbitExchange("payments", durable=True)
DEAD_EXCHANGE = RabbitExchange("payments.dlx", durable=True)
PAYMENTS_QUEUE = RabbitQueue(
    "payments.new",
    durable=True,
    arguments={
        "x-single-active-consumer": True,
        "x-dead-letter-exchange": "payments.dlx",
        "x-dead-letter-routing-key": "payments.dead",
    },
)
DEAD_QUEUE = RabbitQueue("payments.dead", durable=True)


def make_broker(url: str) -> RabbitBroker:
    return RabbitBroker(
        url,
        default_channel=Channel(prefetch_count=1, publisher_confirms=True, on_return_raises=True),
    )


async def declare_topology(broker: RabbitBroker) -> None:
    exchange = await broker.declare_exchange(PAYMENTS_EXCHANGE)
    dead_exchange = await broker.declare_exchange(DEAD_EXCHANGE)
    queue = await broker.declare_queue(PAYMENTS_QUEUE)
    dead_queue = await broker.declare_queue(DEAD_QUEUE)
    await queue.bind(exchange, routing_key=PAYMENTS_QUEUE.name)
    await dead_queue.bind(dead_exchange, routing_key=DEAD_QUEUE.name)
