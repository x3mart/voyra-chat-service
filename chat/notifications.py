import logging

log = logging.getLogger(__name__)


def enqueue_push(user_id: int, payload: dict) -> None:
    """Вызывается, когда у получателя нет открытого сокета.
    TODO: заменить на Celery-задачу: send_push.delay(user_id, payload)."""
    log.info("offline push for user %s: %s", user_id, payload)
