import pytest
from starlette.websockets import WebSocketDisconnect

from chat.helpers import token


def make_chat(client, a=1, b=2) -> str:
    r = client.post("/chats/direct", json={"user_id": b}, headers=token(a))
    assert r.status_code == 200
    return r.json()["conversation_id"]


def test_rejects_without_token(client):
    with pytest.raises(WebSocketDisconnect):
        with client.websocket_connect("/ws"):
            pass


def test_send_ack_deliver_read(client):
    conv = make_chat(client)
    assert make_chat(client, 2, 1) == conv  # личный диалог не дублируется

    with client.websocket_connect("/ws", headers=token(1)) as ws1, \
         client.websocket_connect("/ws", headers=token(2)) as ws2:
        ws1.send_json({"type": "message.send", "ref": "r1",
                       "data": {"conversation_id": conv, "client_id": "c1", "text": " Hello "}})
        ack = ws1.receive_json()
        assert ack["type"] == "message.ack" and ack["ref"] == "r1"
        assert ack["data"]["seq"] == 1 and ack["data"]["created"] is True

        new = ws2.receive_json()
        assert new["type"] == "message.new"
        assert new["data"]["text"] == "Hello" and new["data"]["sender_id"] == 1

        # повторная отправка с тем же client_id идемпотентна
        ws1.send_json({"type": "message.send",
                       "data": {"conversation_id": conv, "client_id": "c1", "text": "Hello"}})
        dup = ws1.receive_json()
        assert dup["data"]["seq"] == 1 and dup["data"]["created"] is False

        ws2.send_json({"type": "message.read", "data": {"conversation_id": conv, "up_to_seq": 99}})
        read = ws1.receive_json()
        assert read["type"] == "message.read"
        assert read["data"] == {"conversation_id": conv, "user_id": 2, "up_to_seq": 1}


def test_typing_and_errors(client):
    conv = make_chat(client)
    with client.websocket_connect("/ws", headers=token(1)) as ws1, \
         client.websocket_connect("/ws", headers=token(2)) as ws2, \
         client.websocket_connect("/ws", headers=token(3)) as ws3:
        ws1.send_json({"type": "typing", "data": {"conversation_id": conv}})
        assert ws2.receive_json()["type"] == "typing"

        # посторонний не может писать в чужой диалог
        ws3.send_json({"type": "message.send",
                       "data": {"conversation_id": conv, "client_id": "x", "text": "hi"}})
        assert ws3.receive_json()["data"]["code"] == "not_participant"

        ws1.send_json({"type": "message.send", "data": {"conversation_id": "bad"}})
        assert ws1.receive_json()["data"]["code"] == "invalid_payload"

        ws1.send_text("not json")
        assert ws1.receive_json()["data"]["code"] == "invalid_json"


def test_sync_after_offline_and_history(client):
    conv = make_chat(client)
    with client.websocket_connect("/ws", headers=token(1)) as ws1:
        for i in range(3):  # пользователь 2 офлайн
            ws1.send_json({"type": "message.send",
                           "data": {"conversation_id": conv, "client_id": f"c{i}", "text": f"m{i}"}})
            assert ws1.receive_json()["data"]["seq"] == i + 1

    with client.websocket_connect("/ws", headers=token(2)) as ws2:
        ws2.send_json({"type": "sync.request", "ref": "s", "data": {"cursors": {conv: 1}}})
        batch = ws2.receive_json()
        assert batch["type"] == "sync.batch"
        assert [m["seq"] for m in batch["data"]["messages"]] == [2, 3]

    r = client.get(f"/chats/{conv}/messages?limit=2", headers=token(2)).json()
    assert [m["seq"] for m in r["messages"]] == [3, 2] and r["has_more"] is True
    r = client.get(f"/chats/{conv}/messages?before_seq=2", headers=token(2)).json()
    assert [m["seq"] for m in r["messages"]] == [1] and r["has_more"] is False
    assert client.get(f"/chats/{conv}/messages", headers=token(3)).status_code == 403
