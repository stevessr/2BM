#!/usr/bin/env python3
"""Real Synapse + local fake OneBot integration, no external secrets."""
import json
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import quote
from urllib.request import Request, urlopen
from urllib.error import HTTPError

HS="http://127.0.0.1:8008"
AS="http://127.0.0.1:9001"
ADMIN="http://127.0.0.1:9000"
SENT=[]


class OneBot(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def response(self, obj):
        data=json.dumps(obj).encode()
        self.send_response(200)
        self.send_header("Content-Type","application/json")
        self.send_header("Content-Length",str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        if self.path=="/get_friend_list":
            self.response({"status":"ok","retcode":0,
                "data":[{"user_id":123,"nickname":"Alice"}]})
        elif self.path=="/get_group_list":
            self.response({"status":"ok","retcode":0,"data":[]})
        else:
            self.send_error(404)

    def do_POST(self):
        body=json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        if self.path=="/send_private_msg":
            SENT.append(body)
            self.response({"status":"ok","retcode":0,
                "data":{"message_id":9001}})
        else:
            self.send_error(404)


def api(method,base,path,payload=None,token=None):
    data=json.dumps(payload).encode() if payload is not None else None
    headers={"Content-Type":"application/json"}
    if token:
        headers["Authorization"]="Bearer "+token
    req=Request(base+path,method=method,data=data,headers=headers)
    try:
        with urlopen(req,timeout=12) as response:
            raw=response.read()
            return json.loads(raw) if raw else {}
    except HTTPError as ex:
        raise AssertionError(f"{method} {path}: {ex.code} {ex.read()[:600]!r}") from ex


def until(fn,desc,seconds=40):
    end=time.monotonic()+seconds
    last=None
    while time.monotonic()<end:
        try:
            result=fn()
            if result:
                return result
        except Exception as ex:
            last=ex
        time.sleep(0.3)
    raise AssertionError(f"Timeout awaiting {desc}, last={last}")


def messages(owner,room):
    rid=quote(room,safe="")
    return api("GET",HS,f"/_matrix/client/v3/rooms/{rid}/messages?dir=b&limit=100",
        token=owner)["chunk"]


def main():
    server=ThreadingHTTPServer(("127.0.0.1",3010),OneBot)
    threading.Thread(target=server.serve_forever,daemon=True).start()
    try:
        until(lambda:api("GET",AS,"/readyz").get("status")=="ready","bridge readiness")
        owner=api("POST",HS,"/_matrix/client/v3/login",{
            "type":"m.login.password",
            "identifier":{"type":"m.id.user","user":"owner"},
            "password":"ci-owner-password"})["access_token"]
        admin=os.environ["BRIDGE_ADMIN_TOKEN"]
        account=api("POST",ADMIN,"/api/admin/accounts",{
            "platform":"qq","label":"Synapse QA",
            "owner_mxid":"@owner:smoke.test",
            "credentials":{"api_base":"http://127.0.0.1:3010",
                "access_token":"local-onebot","hook_secret":"local-hook"}} ,admin)["id"]
        hook=f"/hooks/qq/{account}"
        msg={"post_type":"message","message_type":"private","self_id":999,
             "user_id":123,"message_id":456,"raw_message":"Hello from QQ",
             "sender":{"nickname":"Alice"}}
        api("POST",AS,hook,msg,"local-hook")
        room=until(lambda:next(
            (m["matrix_room"] for m in api("GET",ADMIN,"/api/admin/mappings",
                token=admin) if m["account_id"]==account and
                m["external_chat"]=="private:123"),None),"Matrix room creation")
        rid=quote(room,safe="")
        api("POST",HS,f"/_matrix/client/v3/join/{rid}",{},owner)
        inbound=until(lambda:next((e for e in messages(owner,room)
            if e["type"]=="m.room.message" and
            e["content"].get("body")=="Hello from QQ"),None),"ghost message")
        assert inbound["sender"].startswith("@_2bm_p_")

        api("POST",AS,hook,msg,"local-hook")
        assert sum(1 for e in messages(owner,room) if e["type"]=="m.room.message"
            and e["content"].get("body")=="Hello from QQ")==1

        source_reply={"post_type":"message","message_type":"private",
            "self_id":999,"user_id":123,"message_id":457,
            "message":[{"type":"reply","data":{"id":"456"}},
                       {"type":"text","data":{"text":"QQ native reply"}}],
            "sender":{"nickname":"Alice"}}
        api("POST",AS,hook,source_reply,"local-hook")
        quoted=until(lambda:next((e for e in messages(owner,room)
            if e["type"]=="m.room.message" and
            e["content"].get("body")=="QQ native reply"),None),
            "OneBot native reply => Matrix relation")
        assert quoted["content"]["m.relates_to"]["m.in_reply_to"]["event_id"]==inbound["event_id"]

        event={"msgtype":"m.text",
            "body":"> <Alice> Hello from QQ\n\nMatrix response",
            "m.relates_to":{"m.in_reply_to":{"event_id":inbound["event_id"]}}}
        api("PUT",HS,f"/_matrix/client/v3/rooms/{rid}/send/m.room.message/ci-txn",event,owner)
        sent=until(lambda:SENT[0] if SENT else None,"Matrix -> OneBot")
        assert sent["message"][0]=={"type":"reply","data":{"id":"456"}},sent
        assert sent["message"][1]["data"]["text"]=="Matrix response",sent
        echo={"post_type":"message_sent","message_type":"private",
              "self_id":999,"user_id":999,"target_id":123,
              "message_id":9001,"raw_message":"Matrix response",
              "sender":{"nickname":"Me"}}
        api("POST",AS,hook,echo,"local-hook")
        time.sleep(1)
        # The owner's Matrix reply includes a quoted fallback in its body.
        # Only the original owner event may end in the reply text; there
        # must not be an additional ghost echo with the same content.
        matching=[e for e in messages(owner,room) if e["type"]=="m.room.message"
            and e["content"].get("body","").endswith("Matrix response")]
        assert len(matching)==1,matching
        assert matching[0]["sender"]=="@owner:smoke.test",matching

        recall={"post_type":"notice","notice_type":"friend_recall",
                "self_id":999,"user_id":123,"message_id":456}
        api("POST",AS,hook,recall,"local-hook")
        origin=quote(inbound["event_id"],safe="")
        until(lambda:api("GET",HS,
            f"/_matrix/client/v3/rooms/{rid}/event/{origin}",token=owner)
            .get("content")=={},"ghost redaction")
        # A self-authored message arriving before any contact message must
        # create a DM owned by the *contact* ghost, not the self ghost.
        second=api("POST",ADMIN,"/api/admin/accounts",{
            "platform":"qq","label":"Self-first DM test",
            "owner_mxid":"@owner:smoke.test",
            "credentials":{"api_base":"http://127.0.0.1:3010",
                "access_token":"local-onebot","hook_secret":"local-hook"}},admin)["id"]
        second_hook=f"/hooks/qq/{second}"
        api("POST",AS,second_hook,{
            "post_type":"message_sent","message_type":"private",
            "self_id":999,"user_id":999,"target_id":123,
            "message_id":9010,"raw_message":"Sent from another QQ device"
        },"local-hook")
        second_room=until(lambda:next(
            (m["matrix_room"] for m in api("GET",ADMIN,"/api/admin/mappings",
                token=admin) if m["account_id"]==second and
                m["external_chat"]=="private:123"),None),"self-first Matrix room")
        from uuid import uuid5,NAMESPACE_URL
        expected="@_2bm_p_"+uuid5(NAMESPACE_URL,f"{second}:123").hex+":smoke.test"
        created=api("GET",HS,f"/_matrix/client/v3/rooms/{quote(second_room,safe='')}/state/m.room.create",
            token=owner)
        # /state returns content, not the event envelope; query /messages for
        # the m.room.create event after joining the invited owner.
        api("POST",HS,f"/_matrix/client/v3/join/{quote(second_room,safe='')}",{},owner)
        creation=next(e for e in messages(owner,second_room) if e["type"]=="m.room.create")
        assert creation["sender"]==expected,creation
        names=[e for e in messages(owner,second_room) if e["type"]=="m.room.name"]
        assert names and not names[-1]["content"]["name"].startswith("Me ("),names
        print("PASS Synapse/2BM/OneBot: virtual room, replies, dedup, recalls, self-first DM")
    finally:
        server.shutdown()
        server.server_close()


if __name__=="__main__":
    main()
