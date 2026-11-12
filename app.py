"""숫자야구 멀티플레이 (Flask + Flask-SocketIO)

- 방장이 방을 만들고 4자리 방 코드를 공유
- 모두가 같은 비밀 숫자를 동시에 맞히는 대결 방식
- 다른 사람의 시도 횟수/결과(S·B)는 실시간 공개, 입력한 숫자는 본인만 봄
- 방 상태는 메모리에만 저장 (서버 재시작 시 초기화)
"""
import os
import random
import string
import threading
import time
import uuid

from flask import Flask, render_template, request
from flask_socketio import SocketIO, emit, join_room

app = Flask(__name__)
app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", uuid.uuid4().hex)
socketio = SocketIO(app, async_mode="threading", cors_allowed_origins="*")

MAX_PLAYERS = 12
ROOM_TTL = 60 * 60 * 6  # 6시간 지난 빈 방 정리

rooms = {}          # code -> room dict
sid_index = {}      # sid -> (code, pid)
lock = threading.RLock()


# ---------- 게임 로직 ----------
def make_secret(digits):
    return "".join(random.sample(string.digits, digits))


def judge(secret, guess):
    s = sum(1 for a, b in zip(secret, guess) if a == b)
    b = sum(1 for ch in guess if ch in secret) - s
    return s, b


def valid_guess(guess, digits):
    return (
        isinstance(guess, str)
        and len(guess) == digits
        and guess.isdigit()
        and len(set(guess)) == digits
    )


def new_code():
    while True:
        code = "".join(random.choices(string.digits, k=4))
        if code not in rooms:
            return code


def cleanup():
    now = time.time()
    for code in list(rooms):
        r = rooms[code]
        online = any(p["online"] for p in r["players"].values())
        if not online and now - r["touched"] > ROOM_TTL:
            del rooms[code]


# ---------- 상태 전송 ----------
def public_players(room):
    out = []
    for pid, p in room["players"].items():
        last = p["history"][-1] if p["history"] else None
        out.append({
            "pid": pid,
            "name": p["name"],
            "attempts": len(p["history"]),
            "last": {"s": last["s"], "b": last["b"]} if last else None,
            "solved": p["solved"],
            "rank": p["rank"],
            "online": p["online"],
            "gaveup": p["gaveup"],
        })
    # 정렬: 순위 있는 사람 먼저, 그다음 시도 횟수 적은 순
    out.sort(key=lambda x: (0, x["rank"]) if x["rank"] else (2 if x["gaveup"] else 1, x["attempts"]))
    return out


def push_state(code):
    room = rooms.get(code)
    if not room:
        return
    base = {
        "code": code,
        "digits": room["digits"],
        "status": room["status"],
        "round": room["round"],
        "host": room["host"],
        "players": public_players(room),
        "feed": room["feed"][-30:],
        "secret": room["secret"] if room["status"] == "ended" else None,
    }
    for pid, p in room["players"].items():
        if p["sid"] and p["online"]:
            payload = dict(base, me=pid, history=p["history"])
            socketio.emit("state", payload, to=p["sid"])


def add_feed(room, text, kind="info"):
    room["feed"].append({"t": time.time(), "text": text, "kind": kind})
    room["feed"] = room["feed"][-60:]


def check_all_done(room):
    active = [p for p in room["players"].values() if p["online"] or p["history"]]
    if active and all(p["solved"] or p["gaveup"] for p in active):
        room["status"] = "ended"
        add_feed(room, f"라운드 종료! 정답은 {room['secret']}", "end")


# ---------- 라우트 ----------
@app.route("/")
def index():
    return render_template("index.html")


@app.route("/healthz")
def healthz():
    return "ok"


# ---------- 소켓 이벤트 ----------
def attach(code, pid, sid):
    prev = sid_index.get(sid)
    if prev and prev != (code, pid):  # 같은 기기가 다른 자리로 재입장 → 이전 자리 정리
        old_room = rooms.get(prev[0])
        old = old_room and old_room["players"].get(prev[1])
        if old:
            if old["history"]:
                old.update(online=False, sid=None)
            else:
                old_room["players"].pop(prev[1])
            reassign_host(old_room)
    room = rooms[code]
    p = room["players"][pid]
    if p["sid"] and p["sid"] != sid:
        sid_index.pop(p["sid"], None)
    p["sid"] = sid
    p["online"] = True
    sid_index[sid] = (code, pid)
    join_room(code)
    room["touched"] = time.time()


@socketio.on("create_room")
def on_create(data):
    name = (data.get("name") or "").strip()[:12]
    digits = int(data.get("digits") or 3)
    if not name:
        return {"ok": False, "error": "닉네임을 입력하세요"}
    if digits not in (3, 4):
        digits = 3
    with lock:
        cleanup()
        code = new_code()
        pid = uuid.uuid4().hex[:10]
        rooms[code] = {
            "digits": digits,
            "status": "lobby",
            "round": 0,
            "secret": None,
            "host": pid,
            "players": {},
            "feed": [],
            "next_rank": 1,
            "touched": time.time(),
        }
        rooms[code]["players"][pid] = new_player(name)
        attach(code, pid, request.sid)
        add_feed(rooms[code], f"{name}님이 방을 만들었어요")
    push_state(code)
    return {"ok": True, "code": code, "pid": pid}


def new_player(name):
    return {"name": name, "sid": None, "online": False, "history": [],
            "solved": False, "rank": None, "gaveup": False}


@socketio.on("join_room")
def on_join(data):
    code = str(data.get("code") or "").strip()
    name = (data.get("name") or "").strip()[:12]
    pid = data.get("pid")
    with lock:
        room = rooms.get(code)
        if not room:
            return {"ok": False, "error": "방을 찾을 수 없어요"}
        if pid and pid in room["players"]:  # 재접속
            attach(code, pid, request.sid)
        else:
            if not name:
                return {"ok": False, "error": "닉네임을 입력하세요"}
            if len(room["players"]) >= MAX_PLAYERS:
                return {"ok": False, "error": "방이 가득 찼어요"}
            names = {p["name"] for p in room["players"].values()}
            base, n = name, 2
            while name in names:
                name = f"{base}{n}"
                n += 1
            pid = uuid.uuid4().hex[:10]
            room["players"][pid] = new_player(name)
            attach(code, pid, request.sid)
            add_feed(room, f"{name}님 입장")
        if room["host"] not in room["players"] or not room["players"][room["host"]]["online"]:
            room["host"] = pid
    push_state(code)
    return {"ok": True, "code": code, "pid": pid}


def ctx():
    return sid_index.get(request.sid, (None, None))


@socketio.on("set_digits")
def on_set_digits(data):
    code, pid = ctx()
    with lock:
        room = rooms.get(code)
        if not room or room["host"] != pid or room["status"] == "playing":
            return
        d = int(data.get("digits") or 3)
        if d in (3, 4):
            room["digits"] = d
    push_state(code)


@socketio.on("start_game")
def on_start(_=None):
    code, pid = ctx()
    with lock:
        room = rooms.get(code)
        if not room or room["host"] != pid:
            return
        room["secret"] = make_secret(room["digits"])
        room["status"] = "playing"
        room["round"] += 1
        room["next_rank"] = 1
        for p in room["players"].values():
            p.update(history=[], solved=False, rank=None, gaveup=False)
        add_feed(room, f"{room['round']}라운드 시작! ({room['digits']}자리)", "start")
    push_state(code)


@socketio.on("guess")
def on_guess(data):
    code, pid = ctx()
    guess = str(data.get("guess") or "")
    with lock:
        room = rooms.get(code)
        if not room or room["status"] != "playing":
            return {"ok": False, "error": "진행 중인 게임이 없어요"}
        p = room["players"].get(pid)
        if not p or p["solved"] or p["gaveup"]:
            return {"ok": False, "error": "이미 끝났어요"}
        if not valid_guess(guess, room["digits"]):
            return {"ok": False, "error": "서로 다른 숫자로 입력하세요"}
        if any(h["guess"] == guess for h in p["history"]):
            return {"ok": False, "error": "이미 입력한 숫자예요"}
        s, b = judge(room["secret"], guess)
        p["history"].append({"guess": guess, "s": s, "b": b})
        n = len(p["history"])
        if s == room["digits"]:
            p["solved"] = True
            p["rank"] = room["next_rank"]
            room["next_rank"] += 1
            add_feed(room, f"🎉 {p['name']}님 {n}번 만에 정답! ({p['rank']}등)", "win")
            check_all_done(room)
        else:
            res = "OUT" if s == 0 and b == 0 else f"{s}S {b}B"
            add_feed(room, f"{p['name']} · {n}번째 · {res}")
        room["touched"] = time.time()
    push_state(code)
    return {"ok": True, "s": s, "b": b}


@socketio.on("give_up")
def on_give_up(_=None):
    code, pid = ctx()
    with lock:
        room = rooms.get(code)
        if not room or room["status"] != "playing":
            return
        p = room["players"].get(pid)
        if not p or p["solved"]:
            return
        p["gaveup"] = True
        add_feed(room, f"{p['name']}님 포기 🏳️")
        check_all_done(room)
    push_state(code)


@socketio.on("end_game")
def on_end(_=None):
    code, pid = ctx()
    with lock:
        room = rooms.get(code)
        if not room or room["host"] != pid or room["status"] != "playing":
            return
        room["status"] = "ended"
        add_feed(room, f"방장이 라운드를 끝냈어요. 정답은 {room['secret']}", "end")
    push_state(code)


@socketio.on("leave_room")
def on_leave(_=None):
    code, pid = ctx()
    with lock:
        room = rooms.get(code)
        sid_index.pop(request.sid, None)
        if not room or pid not in room["players"]:
            return
        p = room["players"].pop(pid)
        add_feed(room, f"{p['name']}님 퇴장")
        reassign_host(room)
        if room["status"] == "playing":
            check_all_done(room)
    push_state(code)


def reassign_host(room):
    host = room["players"].get(room["host"])
    if host and host["online"]:
        return
    for pid, p in room["players"].items():
        if p["online"]:
            room["host"] = pid
            return


@socketio.on("disconnect")
def on_disconnect(*_):
    with lock:
        code, pid = sid_index.pop(request.sid, (None, None))
        room = rooms.get(code)
        if not room or pid not in room["players"]:
            return
        p = room["players"][pid]
        if p["sid"] == request.sid:
            p["online"] = False
            p["sid"] = None
        reassign_host(room)
    push_state(code)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    socketio.run(app, host="0.0.0.0", port=port, allow_unsafe_werkzeug=True)
