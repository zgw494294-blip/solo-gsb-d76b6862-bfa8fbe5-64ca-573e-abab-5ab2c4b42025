"""公告迁移：HTTP 导出（格式版本 1 归档）与 HTTP 导入（只追加缺失后缀）的测试。"""

import json
import os
import sys
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor

# 公告库文件指向临时目录：测试不污染项目 data/，且每个进程独立。
_TMPDIR = tempfile.mkdtemp(prefix="bulletin-transfer-test-")
os.environ["BULLETIN_DB_PATH"] = os.path.join(_TMPDIR, "bulletins.db")

from fastapi.testclient import TestClient

sys.path.insert(0, ".")
import app.main as main_module
from app.bulletin import (
    BulletinConflict,
    BulletinDivergence,
    BulletinNotFound,
    BulletinStore,
    import_bulletins,
)
from app.main import app

client = TestClient(app)

failures = 0


def check(cond, msg):
    global failures
    if cond:
        print("  ✓", msg)
    else:
        failures += 1
        print("  ✗", msg)


RANGES = {"smMin": 10, "smMax": 60, "mfMin": 10, "mfMax": 60}

# 选手 101：两趟 a1-b1-c1（25 秒）/ a2-b2-c2（28 秒），总用时 53；
# 选手 102：一趟 d1-e1-f1（20 秒）。
RECORDS = [
    {"id": "a1", "runner": 101, "gate": "S", "time": 0},
    {"id": "b1", "runner": 101, "gate": "M", "time": 12},
    {"id": "c1", "runner": 101, "gate": "F", "time": 25},
    {"id": "a2", "runner": 101, "gate": "S", "time": 30},
    {"id": "b2", "runner": 101, "gate": "M", "time": 45},
    {"id": "c2", "runner": 101, "gate": "F", "time": 58},
    {"id": "d1", "runner": 102, "gate": "S", "time": 5},
    {"id": "e1", "runner": 102, "gate": "M", "time": 15},
    {"id": "f1", "runner": 102, "gate": "F", "time": 25},
]

INPUT = {
    "standards": RANGES,
    "runners": [{"number": 101}, {"number": 102}],
    "records": RECORDS,
}


def review(runner, triple, status="pass", note=""):
    (sid, st), (mid, mt), (fid, ft) = triple
    return {
        "runner": runner,
        "records": [
            {"id": sid, "gate": "S", "time": st},
            {"id": mid, "gate": "M", "time": mt},
            {"id": fid, "gate": "F", "time": ft},
        ],
        "status": status,
        "note": note,
    }


ALL_REVIEWS = [
    review(101, (("a1", 0), ("b1", 12), ("c1", 25)), note="第一趟无误"),
    review(101, (("a2", 30), ("b2", 45), ("c2", 58))),
    review(102, (("d1", 5), ("e1", 15), ("f1", 25))),
]

SNAPSHOT_KEYS = ("input", "adjudication", "reviews", "penalties", "ranking")


def pub_body(number, base=0, input_=None, reviews=None, penalties=None):
    return {
        "number": number,
        "baseVersion": base,
        "input": input_ if input_ is not None else INPUT,
        "reviews": ALL_REVIEWS if reviews is None else reviews,
        "penalties": [] if penalties is None else penalties,
    }


def publish(number, base=0, **kw):
    return client.post("/api/bulletins/publish", json=pub_body(number, base, **kw))


def read(number, version=None):
    q = "" if version is None else f"?version={version}"
    return client.get(f"/api/bulletins/{number}{q}")


def export(numbers):
    return client.post("/api/bulletins/export", json={"numbers": numbers})


def import_http(archive):
    return client.post("/api/bulletins/import", json=archive)


def reorder_json(value):
    """递归重排所有对象的键序（数组顺序不动）：用于验证键序忽略。"""
    if isinstance(value, dict):
        return {k: reorder_json(value[k]) for k in sorted(value, reverse=True)}
    if isinstance(value, list):
        return [reorder_json(v) for v in value]
    return value


def test_export_basic():
    print("场景1：导出格式版本 1，含每版编号/版本/发布时间/完整快照，与读取结果一致")
    publish("x-exp", base=0)
    publish("x-exp", base=1, penalties=[{"runner": 101, "seconds": 5, "reason": "迟到"}])
    publish("x-exp2", base=0)
    r = export(["x-exp", "x-exp2"])
    check(r.status_code == 200 and r.json()["ok"], "导出成功 200")
    archive = r.json()["result"]
    check(archive["format"] == 1, "归档格式版本为 1")
    items = archive["bulletins"]
    check(
        [(i["number"], i["version"]) for i in items]
        == [("x-exp", 1), ("x-exp", 2), ("x-exp2", 1)],
        "条目按请求编号顺序、版本升序排列",
    )
    for item in items:
        check(
            set(item) == {"number", "version", "publishedAt", "snapshot"},
            f"条目恰好四个字段：{item['number']} v{item['version']}",
        )
        check(
            set(item["snapshot"]) == set(SNAPSHOT_KEYS),
            f"快照为读取结果正文五部分：{item['number']} v{item['version']}",
        )
    b = read("x-exp", 2).json()["result"]
    item = items[1]
    check(item["publishedAt"] == b["publishedAt"], "发布时间与读取接口一致")
    rest = {k: b[k] for k in SNAPSHOT_KEYS}
    check(item["snapshot"] == rest, "快照与读取结果正文一致")
    check(item["snapshot"]["adjudication"]["runners"][0]["lapCount"] == 2, "快照含完整裁决方案")
    print("test_export_basic OK")


def test_export_errors():
    print("场景2：未知编号整次 404；非法选择 422；坏 JSON 400")
    publish("x-err", base=0)
    r = export(["x-err", "no-such-number"])
    check(r.status_code == 404, "任一编号不存在整次 404")
    check("no-such-number" in r.json()["error"], "404 指出缺失编号")
    check(export([]).status_code == 422, "空编号列表 422")
    r = export(["x-err", "x-err"])
    check(r.status_code == 422 and "重复" in r.json()["error"], "编号重复 422")
    check(export(["  "]).status_code == 422, "空白编号 422")
    check(export([True]).status_code == 422, "布尔编号 422")
    r = client.post("/api/bulletins/export", json={"numbers": "x-err"})
    check(r.status_code == 422, "numbers 非数组 422")
    r = client.post("/api/bulletins/export", json={})
    check(r.status_code == 422, "缺少 numbers 422")
    r = client.post("/api/bulletins/export", json="x-err")
    check(r.status_code == 422, "请求体非对象 422")
    r = client.post(
        "/api/bulletins/export",
        content=b"{bad",
        headers={"Content-Type": "application/json"},
    )
    check(r.status_code == 400, "坏 JSON 400")
    publish("123", base=0)
    r = export([123])
    check(
        r.status_code == 200
        and r.json()["result"]["bulletins"][0]["number"] == "123",
        "JSON 整数编号按十进制文本接受",
    )
    print("test_export_errors OK")


def test_dual_instance_migration():
    print("场景3：双实例迁移——A 导出 → B 导入 → B 从最新版继续发布 → 回迁 A")
    publish("mig", base=0)
    publish("mig", base=1, penalties=[{"runner": 102, "seconds": 40, "reason": "抢跑"}])
    archive = export(["mig"]).json()["result"]

    # 切到“另一实例”：换一个库文件并重置全局存储（模拟第二台部署）。
    first_path = os.environ["BULLETIN_DB_PATH"]
    os.environ["BULLETIN_DB_PATH"] = os.path.join(_TMPDIR, "instance-b.db")
    main_module._bulletin_store = None
    try:
        r = import_http(archive)
        check(r.status_code == 200, "B 实例导入成功")
        check(r.json()["result"] == {"added": 2, "skipped": 0}, "B 新增 2 版、跳过 0")
        for v in (1, 2):
            b = read("mig", v).json()["result"]
            item = archive["bulletins"][v - 1]
            check(
                b["publishedAt"] == item["publishedAt"]
                and {k: b[k] for k in SNAPSHOT_KEYS} == item["snapshot"],
                f"B 读取 v{v}：发布时间与快照与 A 完全一致",
            )
        check(read("mig").json()["result"]["version"] == 2, "B 缺省读最新版 v2")
        # B 上后续发布从导入的最新版本继续。
        r = publish("mig", base=1)
        check(
            r.status_code == 409 and r.json()["currentVersion"] == 2,
            "B 上过期基线 409 且报告最新版本 2",
        )
        r = publish("mig", base=2, penalties=[{"runner": 101, "seconds": 7, "reason": "设备误触"}])
        check(
            r.status_code == 200 and r.json()["result"]["version"] == 3,
            "B 后续发布从最新版本继续为 v3",
        )
        archive_b = export(["mig"]).json()["result"]
        check(len(archive_b["bulletins"]) == 3, "B 导出含 3 版")
    finally:
        os.environ["BULLETIN_DB_PATH"] = first_path
        main_module._bulletin_store = None

    # 回迁 A：v1/v2 相同跳过，只追加缺失后缀 v3。
    r = import_http(archive_b)
    check(
        r.status_code == 200 and r.json()["result"] == {"added": 1, "skipped": 2},
        "回迁 A：相同版跳过、只追加缺失后缀",
    )
    check(read("mig").json()["result"]["version"] == 3, "A 读取到回迁后的最新版")
    check(
        read("mig", 3).json()["result"]["ranking"][0]["penalty"] == 7,
        "回迁 v3 快照内容完整",
    )
    r = import_http(archive_b)
    check(r.json()["result"] == {"added": 0, "skipped": 3}, "重复导入同一归档幂等（全部跳过）")
    print("test_dual_instance_migration OK")


def test_import_overlap_and_divergence():
    print("场景4：本地较长保留；键序忽略；快照/发布时间/数组顺序/JSON 类型分叉均 409")
    publish("ovr", base=0)
    publish("ovr", base=1)
    publish("ovr", base=2)
    archive3 = export(["ovr"]).json()["result"]  # v1..v3
    archive2 = {"format": 1, "bulletins": archive3["bulletins"][:2]}  # v1..v2

    r = import_http(archive2)
    check(
        r.status_code == 200 and r.json()["result"] == {"added": 0, "skipped": 2},
        "本地链更长：全部跳过",
    )
    check(read("ovr").json()["result"]["version"] == 3, "本地 v3 保留不动")

    reordered = {
        "format": 1,
        "bulletins": [
            {**it, "snapshot": reorder_json(it["snapshot"])} for it in archive3["bulletins"]
        ],
    }
    r = import_http(reordered)
    check(
        r.status_code == 200 and r.json()["result"] == {"added": 0, "skipped": 3},
        "对象键序忽略：重排所有键仍视为相同",
    )

    # 分叉：v2 快照不同 → 409；即使归档带着可追加的 v4 也什么都不写。
    diverged = json.loads(json.dumps(archive3))
    diverged["bulletins"][1]["snapshot"]["penalties"] = [
        {"runner": 101, "seconds": 999, "reason": "篡改"}
    ]
    v4 = dict(diverged["bulletins"][2])
    v4["version"] = 4
    v4["publishedAt"] = "2026-10-07T00:00:00+00:00"
    diverged["bulletins"].append(v4)
    r = import_http(diverged)
    check(r.status_code == 409, "重叠版本快照不同 → 409")
    check("分叉" in r.json()["error"] and "ovr" in r.json()["error"], "409 提示分叉编号")
    check(read("ovr").json()["result"]["version"] == 3, "分叉时不改动现有公告")
    check(read("ovr", 4).status_code == 404, "分叉时连缺失后缀也不写入")

    tampered = json.loads(json.dumps(archive3))
    tampered["bulletins"][0]["publishedAt"] = "2026-01-01T00:00:00+00:00"
    check(import_http(tampered).status_code == 409, "快照相同但发布时间不同 → 409")

    shuffled = json.loads(json.dumps(archive3))
    shuffled["bulletins"][0]["snapshot"]["reviews"] = list(
        reversed(shuffled["bulletins"][0]["snapshot"]["reviews"])
    )
    check(import_http(shuffled).status_code == 409, "数组顺序不同 → 409")

    typed = json.loads(json.dumps(archive3))
    typed["bulletins"][0]["snapshot"]["ranking"][0]["penalty"] = "0"
    check(import_http(typed).status_code == 409, "数字与字符串类型不同 → 409")
    typed2 = json.loads(json.dumps(archive3))
    typed2["bulletins"][0]["snapshot"]["ranking"][0]["penalty"] = False
    check(import_http(typed2).status_code == 409, "数字与布尔类型不同 → 409")
    check(read("ovr").json()["result"]["version"] == 3, "全部 409 后原公告不变")
    print("test_import_overlap_and_divergence OK")


def test_import_atomic_across_numbers():
    print("场景5：任一编号分叉或格式非法，整批不写入任何编号")
    publish("atom-a", base=0)
    entry = export(["atom-a"]).json()["result"]["bulletins"][0]

    # 可追加的新编号 + 分叉的旧编号：409 时新编号也不写入。
    new_entry = dict(entry)
    new_entry["number"] = "atom-b"
    bad = json.loads(json.dumps(entry))
    bad["snapshot"]["penalties"] = [{"runner": 101, "seconds": 1, "reason": "x"}]
    r = import_http({"format": 1, "bulletins": [new_entry, bad]})
    check(r.status_code == 409, "含分叉编号整批 409")
    check(read("atom-b").status_code == 404, "可追加的编号同样不写入")

    # 可追加的新编号 + 版本链非法的编号：422 时同样整批不写。
    good = dict(entry)
    good["number"] = "atom-c"
    orphan = dict(entry)
    orphan["number"] = "atom-d"
    orphan["version"] = 2
    r = import_http({"format": 1, "bulletins": [good, orphan]})
    check(r.status_code == 422, "版本不从 1 起整批 422")
    check(
        read("atom-c").status_code == 404 and read("atom-d").status_code == 404,
        "422 同样整批不写",
    )
    print("test_import_atomic_across_numbers OK")


def test_import_validation():
    print("场景6：归档格式非法 422（格式版本/条目字段/版本链/快照结构），均不写入")
    publish("val", base=0)
    entry = export(["val"]).json()["result"]["bulletins"][0]

    def imp(bulletins=None, fmt=1, raw=None):
        if raw is not None:
            return client.post("/api/bulletins/import", json=raw)
        return import_http({"format": fmt, "bulletins": bulletins})

    check(imp(fmt=2, bulletins=[]).status_code == 422, "格式版本不是 1 → 422")
    check(imp(fmt="1", bulletins=[]).status_code == 422, "格式版本为字符串 → 422")
    check(imp(fmt=True, bulletins=[]).status_code == 422, "格式版本为布尔 → 422")
    check(imp(raw={"bulletins": []}).status_code == 422, "缺 format → 422")
    check(imp(raw={"format": 1}).status_code == 422, "缺 bulletins → 422")
    check(imp(raw={"format": 1, "bulletins": {}}).status_code == 422, "bulletins 非数组 → 422")
    check(imp(raw="not-a-dict").status_code == 422, "请求体非对象 → 422")
    r = imp(bulletins=[])
    check(
        r.status_code == 200 and r.json()["result"] == {"added": 0, "skipped": 0},
        "空归档合法：新增 0 跳过 0",
    )

    def e(**kw):
        return {**entry, **kw}

    check(imp(bulletins=[["not-object"]]).status_code == 422, "条目非对象 → 422")
    check(
        imp(bulletins=[{k: v for k, v in entry.items() if k != "version"}]).status_code == 422,
        "条目缺字段 → 422",
    )
    check(imp(bulletins=[{**entry, "extra": 1}]).status_code == 422, "条目多字段 → 422")
    check(imp(bulletins=[e(version=0)]).status_code == 422, "版本 0 → 422")
    check(imp(bulletins=[e(version=-1)]).status_code == 422, "负版本 → 422")
    check(imp(bulletins=[e(version=1.5)]).status_code == 422, "非整数版本 → 422")
    check(imp(bulletins=[e(version=True)]).status_code == 422, "布尔版本 → 422")
    check(imp(bulletins=[e(publishedAt="")]).status_code == 422, "空发布时间 → 422")
    check(imp(bulletins=[e(publishedAt=123)]).status_code == 422, "非字符串发布时间 → 422")
    check(imp(bulletins=[e(number="  ")]).status_code == 422, "空白编号 → 422")
    check(imp(bulletins=[e(version=2)]).status_code == 422, "版本不从 1 起 → 422")
    check(imp(bulletins=[entry, e(version=3)]).status_code == 422, "版本断链（1,3）→ 422")
    check(imp(bulletins=[entry, dict(entry)]).status_code == 422, "重复版本 → 422")
    check(imp(bulletins=[e(snapshot={})]).status_code == 422, "快照缺部分 → 422")
    check(
        imp(bulletins=[e(snapshot={**entry["snapshot"], "extra": {}})]).status_code == 422,
        "快照多字段 → 422",
    )
    check(
        imp(bulletins=[e(snapshot={**entry["snapshot"], "reviews": {}})]).status_code == 422,
        "reviews 非数组 → 422",
    )
    check(
        imp(bulletins=[e(snapshot={**entry["snapshot"], "adjudication": {"x": 1}})]).status_code
        == 422,
        "adjudication 结构不符 → 422",
    )
    check(
        imp(bulletins=[e(snapshot={**entry["snapshot"], "input": []})]).status_code == 422,
        "input 非对象 → 422",
    )
    check(imp(bulletins=[e(snapshot="x")]).status_code == 422, "快照非对象 → 422")
    r = client.post(
        "/api/bulletins/import",
        content=b"{bad",
        headers={"Content-Type": "application/json"},
    )
    check(r.status_code == 400, "坏 JSON → 400")
    check(read("val").json()["result"]["version"] == 1, "全部 422 后原公告不变")
    print("test_import_validation OK")


def test_import_idempotent_and_suffix():
    print("场景7：重复导入幂等；重叠跳过、只追加缺失后缀；迁入版本保留归档发布时间")
    publish("suf", base=0)
    publish("suf", base=1)
    archive2 = export(["suf"]).json()["result"]
    publish("suf", base=2)
    publish("suf", base=3)
    r = import_http(archive2)
    check(r.json()["result"] == {"added": 0, "skipped": 2}, "本地更长：全部跳过")

    archive4 = export(["suf"]).json()["result"]
    v5 = dict(archive4["bulletins"][3])
    v5["version"] = 5
    v5["publishedAt"] = "2026-10-07T12:00:00+00:00"
    v5["snapshot"] = {
        **v5["snapshot"],
        "penalties": [{"runner": 101, "seconds": 5, "reason": "补迁"}],
    }
    archive5 = {"format": 1, "bulletins": archive4["bulletins"] + [v5]}
    r = import_http(archive5)
    check(
        r.status_code == 200 and r.json()["result"] == {"added": 1, "skipped": 4},
        "只追加缺失后缀 v5",
    )
    b5 = read("suf", 5).json()["result"]
    check(b5["publishedAt"] == "2026-10-07T12:00:00+00:00", "迁入版本保留归档发布时间")
    check(
        b5["penalties"] == [{"runner": 101, "seconds": 5, "reason": "补迁"}],
        "迁入版本快照完整",
    )
    check(read("suf").json()["result"]["version"] == 5, "缺省读最新版 v5")
    r = import_http(archive5)
    check(r.json()["result"] == {"added": 0, "skipped": 5}, "重复导入全部跳过")
    # 后续发布从迁入后的最新版本继续。
    r = publish("suf", base=5)
    check(r.status_code == 200 and r.json()["result"]["version"] == 6, "后续发布从最新版继续")
    print("test_import_idempotent_and_suffix OK")


def test_import_entry_order_irrelevant():
    print("场景8：归档条目顺序无关（按编号聚合、版本排序后比对）")
    publish("ord", base=0)
    publish("ord", base=1)
    publish("ord2", base=0)
    items = export(["ord", "ord2"]).json()["result"]["bulletins"]
    shuffled = {"format": 1, "bulletins": [items[2], items[1], items[0]]}
    store = BulletinStore(os.path.join(_TMPDIR, "order.db"))
    result = import_bulletins(shuffled, store)
    check(result == {"added": 3, "skipped": 0}, "乱序归档照常导入")
    check(
        store.read("ord", 2) is not None and store.read("ord2", 1) is not None,
        "两个编号的版本链都完整",
    )
    check(
        store.read("ord", 1)["publishedAt"] == items[0]["publishedAt"],
        "乱序导入仍保留归档发布时间",
    )
    store.close()
    print("test_import_entry_order_irrelevant OK")


def test_transfer_precision():
    print("场景9：大编号/大秒数迁移全程精确（超过 2^53 不截断）")
    big = "9007199254740993"
    ts, tm, tf = "9007199254740992", "9007199254741003", "9007199254741014"
    big_input = {
        "standards": RANGES,
        "runners": [{"number": big}],
        "records": [
            {"id": "h1", "runner": big, "gate": "S", "time": ts},
            {"id": "h2", "runner": big, "gate": "M", "time": tm},
            {"id": "h3", "runner": big, "gate": "F", "time": tf},
        ],
    }
    big_review = review(big, (("h1", ts), ("h2", tm), ("h3", tf)))
    r = publish(
        big,
        base=0,
        input_=big_input,
        reviews=[big_review],
        penalties=[{"runner": big, "seconds": "9007199254740996", "reason": "大罚秒"}],
    )
    check(r.status_code == 200, "大编号/大秒数发布成功")
    archive = export([big]).json()["result"]
    store = BulletinStore(os.path.join(_TMPDIR, "precision.db"))
    result = import_bulletins(archive, store)
    check(result == {"added": 1, "skipped": 0}, "大编号归档导入成功")
    text = json.dumps(store.read(big), ensure_ascii=False)
    check(ts in text and tm in text and tf in text, "超过 2^53 的记录秒数精确保留")
    check("9007199254741018" in text, "调整后用时按任意精度保留")
    b = store.read(big)
    check(b["number"] == big, "大公告编号精确保留")
    check(
        b["adjudication"]["runners"][0]["laps"][0]["smTime"] == 11,
        "两段耗时精确为 11 秒",
    )
    store.close()
    print("test_transfer_precision OK")


def test_import_publish_coordination():
    print("场景10：导入与发布按提交时库内状态协调——不覆盖旧版、不留断链、不留半批")
    store = BulletinStore(os.path.join(_TMPDIR, "coord.db"))
    snap = {k: ([] if k in ("reviews", "penalties", "ranking") else {}) for k in SNAPSHOT_KEYS}
    snap["adjudication"] = {"runners": []}
    archive = {
        "format": 1,
        "bulletins": [
            {
                "number": "race",
                "version": v,
                "publishedAt": f"2026-10-0{v}T00:00:00+00:00",
                "snapshot": snap,
            }
            for v in (1, 2, 3)
        ],
    }

    def do_import():
        try:
            import_bulletins(archive, store)
            return "import-ok"
        except BulletinDivergence:
            return "import-409"

    def do_publish():
        try:
            store.publish("race", 0, {**snap, "mark": "local"})
            return "publish-ok"
        except BulletinConflict:
            return "publish-409"

    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = list(pool.map(lambda f: f(), [do_import, do_publish]))
    check(
        outcomes.count("import-ok") + outcomes.count("publish-ok") == 1,
        "并发导入/发布恰好一项成功",
    )
    current = store.current_version("race")
    check(current in (1, 3), "最终版本数来自胜出的整批（1 或 3）")
    chain = [store.read("race", v) for v in range(1, current + 1)]
    check(all(b is not None for b in chain), "版本链连续无断链")
    check(store.read("race", current + 1) is None, "无半批多出的版本")
    # 重试结果与已提交状态一致：导入赢则重试全部跳过，发布赢则重试仍分叉。
    again = do_import()
    expect = "import-ok" if "import-ok" in outcomes else "import-409"
    check(again == expect, "重试导入与已提交状态一致（不覆盖旧版）")
    if again == "import-ok":
        check(store.read("race", 1)["publishedAt"] == "2026-10-01T00:00:00+00:00",
              "导入赢：归档发布时间原样保留")
    else:
        check(store.read("race", 1).get("mark") == "local", "发布赢：本地内容不被覆盖")
    store.close()
    print("test_import_publish_coordination OK")


def test_export_consistent_snapshot():
    print("场景11：整包取自同一数据库时点（并发发布时导出的链始终连续完整）")
    store = BulletinStore(os.path.join(_TMPDIR, "consist.db"))
    stop = threading.Event()

    def writer():
        while not stop.is_set():
            current = store.current_version("c")
            try:
                store.publish("c", current, {"input": {}, "v": current})
            except BulletinConflict:
                pass

    thread = threading.Thread(target=writer)
    thread.start()
    ok = True
    try:
        for _ in range(60):
            try:
                items = store.export(["c"])
            except BulletinNotFound:
                continue  # 首次发布尚未提交：整次 404 是预期行为
            versions = [i["version"] for i in items]
            if versions != list(range(1, len(versions) + 1)):
                ok = False
    finally:
        stop.set()
        thread.join()
    check(ok, "并发发布期间每次导出都是连续完整链")
    check(store.current_version("c") >= 1, "并发发布持续推进")
    store.close()
    print("test_export_consistent_snapshot OK")


def test_original_contract_untouched():
    print("场景12：原裁决/发布/读取接口行为不变")
    r = client.post("/api/adjudicate", json=INPUT)
    check(
        r.status_code == 200 and r.json()["result"]["runners"][0]["lapCount"] == 2,
        "裁决端点不变",
    )
    publish("keep", base=0)
    check(read("keep").status_code == 200, "缺省读最新不变")
    check(read("keep", 1).status_code == 200, "按版本读旧版不变")
    check(read("keep", 2).status_code == 404, "未知版本仍 404")
    check(read("keep", "abc").status_code == 400, "非法版本参数仍 400")
    r = client.get("/api/bulletins/export")
    check(r.status_code == 404, "GET /api/bulletins/export 仍按编号读取处理")
    print("test_original_contract_untouched OK")


def main():
    test_export_basic()
    test_export_errors()
    test_dual_instance_migration()
    test_import_overlap_and_divergence()
    test_import_atomic_across_numbers()
    test_import_validation()
    test_import_idempotent_and_suffix()
    test_import_entry_order_irrelevant()
    test_transfer_precision()
    test_import_publish_coordination()
    test_export_consistent_snapshot()
    test_original_contract_untouched()
    print("\nALL BULLETIN TRANSFER TESTS PASSED" if failures == 0 else f"\n{failures} ASSERTION(S) FAILED")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
