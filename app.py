"""숫자야구 멀티플레이 (Flask + Flask-SocketIO)

게임 모드
- race   (동시 맞히기): 서버가 정한 같은 비밀 숫자를 모두가 동시에 맞히는 경쟁
- duel   (1:1 대결)  : 두 사람이 각자 숫자를 정하고 번갈아 상대 숫자를 맞힘
                       선공이 먼저 맞히면 후공에게 마지막 기회 1번 (같이 맞히면 무승부)
- battle (여럿이서)  : 각자 숫자를 정하고 차례대로 상대 한 명을 골라 공격
                       숫자를 들킨 사람은 탈락, 마지막까지 남은 사람이 승리

방 상태는 메모리에만 저장 (서버 재시작 시 초기화)
"""
import os
import random
import string
import threading
import time
import uuid

from flask import Flask, render_template, request
from flask_socketio import SocketIO, join_room

app = Flask(__name__)
app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", uuid.uuid4().hex)
socketio = SocketIO(app, async_mode="threading", cors_allowed_origins="*")

MAX_PLAYERS = 12
ROOM_TTL = 60 * 60 * 6  # 6시간 지난 빈 방 정리
MODES = ("race", "duel", "battle")
MODE_NAMES = {"race": "동시 맞히기", "duel": "1:1 대결", "battle": "여럿이서"}

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


def result_text(s, b):
    return "OUT" if s == 0 and b == 0 else f"{s}S {b}B"


def new_code():
    while True:
        code = "".join(random.choices(string.digits, k=4))
        if code not in rooms:
            return code


def new_player(name):
    return {
        "name": name, "sid": None, "online": False,
        # race
        "history": [], "solved": False, "gaveup": False,
        # duel / battle
        "secret": None, "part": False, "out": False,
        # 공통
        "rank": None,
    }


def cleanup():
    now = time.time()
    for code in list(rooms):
        r = rooms[code]
        online = any(p["online"] for p in r["players"].values())
        if not online and now - r["touched"] > ROOM_TTL:
            del rooms[code]


# ---------- 상태 전송 ----------
def public_players(room):
    is_race = room["mode"] == "race"
    attacks = {}
    for e in room["log"]:
        attacks[e["by"]] = attacks.get(e["by"], 0) + 1
    out = []
    for pid, p in room["players"].items():
        last = p["history"][-1] if p["history"] else None
        out.append({
            "pid": pid,
            "name": p["name"],
            "online": p["online"],
            "rank": p["rank"],
            "attempts": len(p["history"]),
            "last": {"s": last["s"], "b": last["b"]} if last else None,
            "solved": p["solved"],
            "gaveup": p["gaveup"],
            "part": p["part"],
            "out": p["out"],
            "ready": bool(p["secret"]),
            "attacks": attacks.get(pid, 0),
        })
    if is_race:
        # 순위 있는 사람 먼저, 그다음 시도 횟수 적은 순
        out.sort(key=lambda x: (0, x["rank"]) if x["rank"] else (2 if x["gaveup"] else 1, x["attempts"]))
    elif room["status"] == "ended":
        out.sort(key=lambda x: (not x["part"], x["rank"] or 99))
    else:
        order = room["order"]
        out.sort(key=lambda x: (not x["part"], order.index(x["pid"]) if x["pid"] in order else 50))
    return out


def current_turn(room):
    if room["mode"] == "race" or room["status"] != "playing" or not room["order"]:
        return None
    return room["order"][room["turn"]]


def push_state(code):
    room = rooms.get(code)
    if not room:
        return
    ended = room["status"] == "ended"
    base = {
        "code": code,
        "mode": room["mode"],
        "digits": room["digits"],
        "status": room["status"],
        "round": room["round"],
        "host": room["host"],
        "players": public_players(room),
        "feed": room["feed"][-30:],
        "secret": room["secret"] if ended and room["mode"] == "race" else None,
        "turn": current_turn(room),
        "log": room["log"][-300:],
        "last_chance": room["last_chance"],
        "secrets": {pid: p["secret"] for pid, p in room["players"].items() if p["part"]} if ended and room["mode"] != "race" else None,
    }
    for pid, p in room["players"].items():
        if p["sid"] and p["online"]:
            payload = dict(base, me=pid, history=p["history"], my_secret=p["secret"])
            socketio.emit("state", payload, to=p["sid"])


def add_feed(room, text, kind="info"):
    room["feed"].append({"t": time.time(), "text": text, "kind": kind})
    room["feed"] = room["feed"][-60:]


# ---------- race 전용 ----------
def check_all_done(room):
    active = [p for p in room["players"].values() if p["online"] or p["history"]]
    if active and all(p["solved"] or p["gaveup"] for p in active):
        room["status"] = "ended"
        add_feed(room, f"라운드 종료! 정답은 {room['secret']}", "end")


# ---------- duel / battle 전용 ----------
def participants(room):
    return [pid for pid, p in room["players"].items() if p["part"]]


def alive_parts(room):
    return [pid for pid in room["order"] if pid in room["players"] and not room["players"][pid]["out"]]


def finish(room, winner):
    room["status"] = "ended"
    room["last_chance"] = None
    if winner and winner in room["players"]:
        room["players"][winner]["rank"] = 1
        add_feed(room, f"🏆 {room['players'][winner]['name']}님 승리!", "end")
    else:
        add_feed(room, "게임 종료", "end")


def eliminate(room, pid, text):
    """pid 탈락 처리. 한 명만 남으면 게임 종료."""
    p = room["players"][pid]
    p["out"] = True
    alive = alive_parts(room)
    p["rank"] = len(alive) + 1
    add_feed(room, text, "win")
    if len(alive) <= 1:
        finish(room, alive[0] if alive else None)


def advance_turn(room):
    """다음 차례로. 접속 중인 생존자를 우선, 없으면 오프라인 생존자."""
    order, n = room["order"], len(room["order"])
    for need_online in (True, False):
        for i in range(1, n + 1):
            idx = (room["turn"] + i) % n
            p = room["players"].get(order[idx])
            if p and not p["out"] and (p["online"] or not need_online):
                room["turn"] = idx
                return


def maybe_begin(room):
    """setup 단계에서 모두 숫자를 정하면 게임 시작."""
    parts = participants(room)
    if len(parts) < 2:
        room["status"] = "lobby"
        for p in room["players"].values():
            p.update(part=False, secret=None)
        add_feed(room, "인원이 부족해서 대기실로 돌아왔어요", "end")
        return
    if all(room["players"][pid]["secret"] for pid in parts):
        random.shuffle(parts)
        room["order"] = parts
        room["turn"] = 0
        room["status"] = "playing"
        if not room["players"][parts[0]]["online"]:
            room["turn"] = len(parts) - 1
            advance_turn(room)
        names = " → ".join(room["players"][pid]["name"] for pid in parts)
        add_feed(room, f"모두 숫자를 정했어요! 순서: {names}", "start")


def leave_battle(room, pid):
    """duel/battle 진행 중 참가자가 나가거나 포기할 때."""
    p = room["players"][pid]
    if not p["part"] or p["out"]:
        return
    if room["status"] == "setup":
        p.update(part=False, secret=None)
        maybe_begin(room)
    elif room["status"] == "playing":
        was_turn = current_turn(room) == pid
        eliminate(room, pid, f"{p['name']}님 탈락 (포기) 🏳️")
        if room["status"] == "playing" and was_turn:
            advance_turn(room)


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
            if old["history"] or old["part"]:
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
    mode = data.get("mode") if data.get("mode") in MODES else "race"
    if not name:
        return {"ok": False, "error": "닉네임을 입력하세요"}
    if digits not in (3, 4):
        digits = 3
    with lock:
        cleanup()
        code = new_code()
        pid = uuid.uuid4().hex[:10]
        rooms[code] = {
            "mode": mode,
            "digits": digits,
            "status": "lobby",
            "round": 0,
            "secret": None,
            "host": pid,
            "players": {},
            "feed": [],
            "next_rank": 1,
            "order": [],
            "turn": 0,
            "log": [],
            "last_chance": None,
            "touched": time.time(),
        }
        rooms[code]["players"][pid] = new_player(name)
        attach(code, pid, request.sid)
        add_feed(rooms[code], f"{name}님이 방을 만들었어요 ({MODE_NAMES[mode]})")
    push_state(code)
    return {"ok": True, "code": code, "pid": pid}


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
            late = room["mode"] != "race" and room["status"] in ("setup", "playing")
            add_feed(room, f"{name}님 입장" + (" (관전)" if late else ""))
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
        if not room or room["host"] != pid or room["status"] != "lobby":
            return
        d = int(data.get("digits") or 3)
        if d in (3, 4):
            room["digits"] = d
    push_state(code)


@socketio.on("set_mode")
def on_set_mode(data):
    code, pid = ctx()
    with lock:
        room = rooms.get(code)
        if not room or room["host"] != pid or room["status"] != "lobby":
            return
        if data.get("mode") in MODES:
            room["mode"] = data["mode"]
    push_state(code)


@socketio.on("to_lobby")
def on_to_lobby(_=None):
    code, pid = ctx()
    with lock:
        room = rooms.get(code)
        if not room or room["host"] != pid or room["status"] != "ended":
            return
        room["status"] = "lobby"
    push_state(code)


@socketio.on("start_game")
def on_start(_=None):
    code, pid = ctx()
    with lock:
        room = rooms.get(code)
        if not room or room["host"] != pid:
            return {"ok": False, "error": "방장만 시작할 수 있어요"}
        if room["status"] not in ("lobby", "ended"):
            return {"ok": False, "error": "이미 진행 중이에요"}
        online = [x for x, p in room["players"].items() if p["online"]]
        mode = room["mode"]
        if mode == "duel" and len(online) != 2:
            return {"ok": False, "error": f"1:1 대결은 2명이 있어야 해요 (지금 {len(online)}명)"}
        if mode == "battle" and len(online) < 2:
            return {"ok": False, "error": "2명 이상 있어야 시작할 수 있어요"}

        room["round"] += 1
        room["next_rank"] = 1
        room["log"] = []
        room["order"] = []
        room["turn"] = 0
        room["last_chance"] = None
        for x, p in room["players"].items():
            p.update(history=[], solved=False, rank=None, gaveup=False,
                     secret=None, part=(mode != "race" and x in online), out=False)
        if mode == "race":
            room["secret"] = make_secret(room["digits"])
            room["status"] = "playing"
            add_feed(room, f"{room['round']}라운드 시작! ({room['digits']}자리)", "start")
        else:
            room["secret"] = None
            room["status"] = "setup"
            add_feed(room, f"{room['round']}라운드 · {MODE_NAMES[mode]} — 각자 비밀 숫자를 정하세요", "start")
    push_state(code)
    return {"ok": True}


@socketio.on("set_secret")
def on_set_secret(data):
    code, pid = ctx()
    secret = str(data.get("secret") or "")
    with lock:
        room = rooms.get(code)
        if not room or room["status"] != "setup":
            return {"ok": False, "error": "지금은 숫자를 정할 수 없어요"}
        p = room["players"].get(pid)
        if not p or not p["part"]:
            return {"ok": False, "error": "이번 판 참가자가 아니에요"}
        if p["secret"]:
            return {"ok": False, "error": "이미 숫자를 정했어요"}
        if not valid_guess(secret, room["digits"]):
            return {"ok": False, "error": "서로 다른 숫자로 입력하세요"}
        p["secret"] = secret
        add_feed(room, f"{p['name']}님 숫자 정함 ✓")
        maybe_begin(room)
    push_state(code)
    return {"ok": True}


@socketio.on("attack")
def on_attack(data):
    code, pid = ctx()
    guess = str(data.get("guess") or "")
    with lock:
        room = rooms.get(code)
        if not room or room["mode"] == "race" or room["status"] != "playing":
            return {"ok": False, "error": "진행 중인 게임이 없어요"}
        if current_turn(room) != pid:
            return {"ok": False, "error": "내 차례가 아니에요"}
        p = room["players"][pid]
        if room["mode"] == "duel":
            target = next((x for x in room["order"] if x != pid), None)
        else:
            target = data.get("target")
        t = room["players"].get(target)
        if not t or target == pid or not t["part"] or t["out"]:
            return {"ok": False, "error": "공격할 상대를 고르세요"}
        if not valid_guess(guess, room["digits"]):
            return {"ok": False, "error": "서로 다른 숫자로 입력하세요"}
        if any(e["by"] == pid and e["to"] == target and e["guess"] == guess for e in room["log"]):
            return {"ok": False, "error": "이미 던진 숫자예요"}

        s, b = judge(t["secret"], guess)
        hit = s == room["digits"]
        room["log"].append({"by": pid, "to": target, "guess": guess, "s": s, "b": b})
        room["touched"] = time.time()
        arrow = "" if room["mode"] == "duel" else f" → {t['name']}"
        add_feed(room, f"{p['name']}{arrow} · {guess} · {result_text(s, b)}")

        if room["last_chance"]:
            # duel: 후공의 마지막 기회
            winner = room["last_chance"]
            if hit:
                p["rank"] = room["players"][winner]["rank"] = 1
                room["status"] = "ended"
                room["last_chance"] = None
                add_feed(room, f"🎯 {p['name']}님도 정답! 🤝 무승부", "end")
            else:
                eliminate(room, pid, f"{p['name']}님 마지막 기회 실패")
        elif hit:
            if room["mode"] == "duel" and pid == room["order"][0]:
                room["last_chance"] = pid
                room["turn"] = 1
                add_feed(room, f"🎯 {p['name']}님 정답! {t['name']}님에게 마지막 기회 한 번", "win")
            else:
                eliminate(room, target, f"🎯 {p['name']}님이 {t['name']}님 숫자({t['secret']})를 맞혔어요! {t['name']}님 탈락")
                if room["status"] == "playing":
                    advance_turn(room)
        else:
            advance_turn(room)
    push_state(code)
    return {"ok": True, "s": s, "b": b, "hit": hit}


@socketio.on("skip_turn")
def on_skip(_=None):
    code, pid = ctx()
    with lock:
        room = rooms.get(code)
        if not room or room["host"] != pid or room["status"] != "playing" or room["mode"] == "race":
            return
        cur = current_turn(room)
        name = room["players"][cur]["name"] if cur in room["players"] else "?"
        if room["last_chance"] and cur != room["last_chance"]:
            eliminate(room, cur, f"방장이 {name}님 차례를 넘겼어요 (마지막 기회 무효)")
        else:
            add_feed(room, f"방장이 {name}님 차례를 넘겼어요")
            advance_turn(room)
    push_state(code)


@socketio.on("guess")
def on_guess(data):
    code, pid = ctx()
    guess = str(data.get("guess") or "")
    with lock:
        room = rooms.get(code)
        if not room or room["mode"] != "race" or room["status"] != "playing":
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
            add_feed(room, f"{p['name']} · {n}번째 · {result_text(s, b)}")
        room["touched"] = time.time()
    push_state(code)
    return {"ok": True, "s": s, "b": b}


@socketio.on("give_up")
def on_give_up(_=None):
    code, pid = ctx()
    with lock:
        room = rooms.get(code)
        if not room or room["status"] not in ("playing", "setup"):
            return
        p = room["players"].get(pid)
        if not p:
            return
        if room["mode"] == "race":
            if room["status"] != "playing" or p["solved"]:
                return
            p["gaveup"] = True
            add_feed(room, f"{p['name']}님 포기 🏳️")
            check_all_done(room)
        else:
            leave_battle(room, pid)
    push_state(code)


@socketio.on("end_game")
def on_end(_=None):
    code, pid = ctx()
    with lock:
        room = rooms.get(code)
        if not room or room["host"] != pid:
            return
        if room["status"] == "setup":
            room["status"] = "lobby"
            for p in room["players"].values():
                p.update(part=False, secret=None)
            add_feed(room, "방장이 게임을 취소했어요", "end")
        elif room["status"] == "playing":
            room["status"] = "ended"
            room["last_chance"] = None
            if room["mode"] == "race":
                add_feed(room, f"방장이 라운드를 끝냈어요. 정답은 {room['secret']}", "end")
            else:
                add_feed(room, "방장이 게임을 끝냈어요", "end")
    push_state(code)


@socketio.on("leave_room")
def on_leave(_=None):
    code, pid = ctx()
    with lock:
        room = rooms.get(code)
        sid_index.pop(request.sid, None)
        if not room or pid not in room["players"]:
            return
        if room["mode"] != "race":
            leave_battle(room, pid)
        p = room["players"].pop(pid)
        add_feed(room, f"{p['name']}님 퇴장")
        reassign_host(room)
        if room["mode"] == "race" and room["status"] == "playing":
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
