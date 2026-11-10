import jwt

SECRET = "test-secret"


def token(uid: int) -> dict:
    return {"Authorization": "Bearer " + jwt.encode({"sub": str(uid)}, SECRET, algorithm="HS256")}