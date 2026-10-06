"""成绩公告迁移：完整版本链的 HTTP 导出 / 导入（格式版本 1）测试。

覆盖：导出归档结构与同一数据库时点、未知编号整次 404；导入的格式校验（422）、
分叉检测（409）、相同跳过/追加后缀/本地较长保留、键序忽略与数组顺序/JSON 类型
保留、整次原子性、并发协调、大整数精确性，以及导入后读取与继续发布不变。
"""

import copy
import json
import os
import sys
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor

# 公告库文件指向临时目录：测试不污染项目 data/，且每个进程独立。
_TMPDIR = tempfile.mkdtemp(prefix="bulletin-transfer-test-")
os.environ["BULLETIN_DB_PATH"] = os.path.join(_TMPDIR, "instance-a.db")

from fastapi.testclient import TestClient

sys.path.insert(0, ".")
import app.main as main_module
from app.bulletin import BulletinConflict, BulletinFork, BulletinStore
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


# 模拟多个实例：每个实例一个独立的 SQLite 库文件，通过切换全局存储实现。
DB_A = os.path.join(_TMPDIR, "instance-a.db")
DB_B = os.path.join(_TMPDIR, "instance-b.db")
DB_C = os.path.join(_TMPDIR, "instance-c.db")
DB_D = os.path.join(_TMPDIR, "instance-d.db")
DB_E = os.path.join(_TMPDIR, "instance-e.db")


def use_instance(path):
    old = main_module._bulletin_store
    if old is not None:
        old.close()
    main_module._bulletin_store = BulletinStore(path)


RANGES = {"smMin": 10, "smMax": 60, "mfMin": 10, "mfMax": 60}

# 选手 101：两趟 a1-b1-c1（25 秒）/ a2-b2-c2（28 秒）；选手 102：一趟 d1-e1-f1（20 秒）。
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

# 结构合法的最小快照：导入不重新裁决，内容不做语义校验。
STUB_SNAPSHOT = {
    "input": {"standards": {}, "runners": [], "records": []},
    "adjudication": {"runners": []},
    "reviews": [],
    "penalties": [],
    "ranking": [],
}


def publish(number, base=0, **kw):
    body = {
        "number": number,
        "baseVersion": base,
        "input": kw.get("input_", INPUT),
        "reviews": ALL_REVIEWS if "reviews" not in kw else kw["reviews"],
        "penalties": [] if "penalties" not in kw else kw["penalties"],
    }
    return client.post("/api/bulletins/publish", json=body)


def read(number, version=None):
    q = "" if version is None else f"?version={version}"
    return client.get(f"/api/bulletins/{number}{q}")


def export(numbers):
    return client.post("/api/bulletins/export", json={"numbers": numbers})


def import_archive(archive):
    return client.post("/api/bulletins/import", json=archive)


def test_export_basic():
    print("场景1：导出所选编号的完整版本链（格式 1，含每版编号/版本/发布时间/完整快照）")
    use_instance(DB_A)
    r = publish("t-basic", penalties=[{"runner": 102, "seconds": 40, "reason": "抢跑"}])
    check(r.status_code == 200, "A 实例发布 v1")
    r = publish("t-basic", base=1)
    check(r.status_code == 200 and r.json()["result"]["version"] == 2, "A 实例发布 v2")
    check(publish("t-other").status_code == 200, "A 实例发布另一编号")

    r = export(["t-basic"])
    check(r.status_code == 200 and r.json()["ok"], "导出 200")
    archive = r.json()["result"]
    check(archive["format"] == 1, "归档格式版本为 1")
    check(len(archive["bulletins"]) == 1, "只含所选编号")
    entry = archive["bulletins"][0]
    check(entry["number"] == "t-basic", "含公告编号")
    check([v["version"] for v in entry["versions"]] == [1, 2], "含完整版本链 1..2")
    for v in entry["versions"]:
        check(
            set(v["snapshot"].keys())
            == {"input", "adjudication", "reviews", "penalties", "ranking"},
            f"版本 {v['version']} 快照结构与原读取结果一致",
        )
        check(isinstance(v["publishedAt"], str) and v["publishedAt"], "含发布时间")
        got = read("t-basic", v["version"]).json()["result"]
        check(
            v["publishedAt"] == got["publishedAt"]
            and v["snapshot"] == {k: got[k] for k in v["snapshot"]},
            f"版本 {v['version']} 归档内容与按版本读取一致",
        )
    check(
        archive["bulletins"][0]["versions"][0]["snapshot"]["penalties"][0]["seconds"] == 40,
        "v1 的罚秒保留在归档中",
    )
    r = export(["t-basic", "t-other"])
    check([b["number"] for b in r.json()["result"]["bulletins"]] == ["t-basic", "t-other"],
          "多编号按选择顺序导出")
    print("test_export_basic OK")


def test_export_errors():
    print("场景2：未知编号整次 404；空/非法选择 422；坏 JSON 400；重复选择去重")
    use_instance(DB_A)
    r = export(["t-basic", "ghost-1"])
    check(r.status_code == 404 and r.json()["ok"] is False, "含未知编号整次 404")
    check("bulletins" not in r.text, "404 不出部分归档")
    check(export(["ghost-only"]).status_code == 404, "单个未知编号同样 404")
    check(export([]).status_code == 422, "空选择 422")
    check(export(["  "]).status_code == 422, "空白编号 422")
    check(export([{}]).status_code == 422, "编号元素非法 422")
    r = client.post("/api/bulletins/export", json={"numbers": "t-basic"})
    check(r.status_code == 422, "numbers 非数组 422")
    r = client.post("/api/bulletins/export", json={})
    check(r.status_code == 422, "缺 numbers 422")
    r = client.post(
        "/api/bulletins/export",
        content=b"{not json",
        headers={"Content-Type": "application/json"},
    )
    check(r.status_code == 400, "请求体不是 JSON 返回 400")
    r = export(["t-basic", " t-basic ", "t-basic"])
    check(
        r.status_code == 200 and len(r.json()["result"]["bulletins"]) == 1,
        "选择中重复编号（含去空白后相同）去重",
    )
    print("test_export_errors OK")


def test_roundtrip_and_continue_publish():
    print("场景3：导出→导入另一实例；读取接口不变；后续发布从最新版本继续")
    use_instance(DB_A)
    archive = export(["t-basic", "t-other"]).json()["result"]
    a_v1 = read("t-basic", 1).json()["result"]
    a_v2 = read("t-basic", 2).json()["result"]

    use_instance(DB_B)
    check(read("t-basic").status_code == 404, "B 实例初始无公告")
    r = import_archive(archive)
    check(r.status_code == 200, "导入 200")
    check(r.json()["result"] == {"added": 3, "skipped": 0}, "新增 3 版、跳过 0")
    check(read("t-basic").json()["result"] == a_v2, "缺省读最新版本且内容与 A 一致")
    check(read("t-basic", 1).json()["result"] == a_v1, "旧版本可读且与 A 一致")
    check(read("t-basic", 3).status_code == 404, "未导入的版本 404")
    check(read("t-other").json()["result"]["version"] == 1, "另一编号一并迁入")

    r = publish("t-basic", base=1)
    check(r.status_code == 409 and r.json()["currentVersion"] == 2,
          "过期基线 409 且当前最新版本为导入的 2")
    r = publish("t-basic", base=2)
    check(r.status_code == 200 and r.json()["result"]["version"] == 3,
          "后续发布从导入的最新版本继续（v3）")
    check(read("t-basic").json()["result"]["version"] == 3, "最新版本为本地新发布的 v3")
    check(read("t-basic", 2).json()["result"] == a_v2, "导入的旧版本不受新发布影响")
    print("test_roundtrip_and_continue_publish OK")


def test_reimport_skip_and_local_longer():
    print("场景4：相同项跳过；本地链更长则保留本地")
    use_instance(DB_A)
    archive = export(["t-basic", "t-other"]).json()["result"]  # t-basic 1..2, t-other 1
    use_instance(DB_B)  # B 现有 t-basic 1..3（v3 为本地发布）、t-other 1
    r = import_archive(archive)
    check(r.status_code == 200, "重复导入 200")
    check(r.json()["result"] == {"added": 0, "skipped": 3}, "完全相同的部分全部跳过")
    check(read("t-basic").json()["result"]["version"] == 3, "本地较长链保留：最新仍为 v3")
    check(read("t-basic", 3).status_code == 200, "本地独有的 v3 不被归档影响")
    print("test_reimport_skip_and_local_longer OK")


def test_import_appends_missing_suffix():
    print("场景5：只追加本地缺失的后缀版本；新增/跳过按版本累计")
    use_instance(DB_A)
    publish("t-basic", base=2)  # v3
    publish("t-basic", base=3)  # v4
    publish("t-other", base=1)  # t-other v2
    archive = export(["t-basic", "t-other"]).json()["result"]  # t-basic 1..4, t-other 1..2

    use_instance(DB_C)
    r = import_archive(archive)
    check(r.json()["result"] == {"added": 6, "skipped": 0}, "全新实例整链导入（4+2）")
    check(read("t-basic").json()["result"]["version"] == 4, "C 上 t-basic 最新 v4")

    use_instance(DB_A)
    publish("t-basic", base=4)  # v5
    archive5 = export(["t-basic", "t-other"]).json()["result"]  # t-basic 1..5, t-other 1..2

    use_instance(DB_C)
    r = import_archive(archive5)
    check(r.json()["result"] == {"added": 1, "skipped": 6},
          "只追加缺失的 t-basic v5，其余（4+2）全部跳过")
    check(read("t-basic").json()["result"]["version"] == 5, "C 上 t-basic 最新 v5")
    chain = [read("t-basic", i).status_code for i in range(1, 6)]
    check(chain == [200] * 5, "导入后 1..5 全链可读、无断链")
    print("test_import_appends_missing_suffix OK")


def test_import_ignores_key_order():
    print("场景6：对象键序忽略——键序不同的相同快照跳过而非分叉")
    use_instance(DB_C)
    archive = export(["t-basic"]).json()["result"]
    shuffled = json.loads(json.dumps(archive, sort_keys=True))
    r = import_archive(shuffled)
    check(r.status_code == 200, "键序重排后导入 200")
    check(r.json()["result"] == {"added": 0, "skipped": 5}, "内容相同全部跳过")
    print("test_import_ignores_key_order OK")


def test_import_fork_409_and_atomicity():
    print("场景7：重叠版本分叉整次 409（快照/发布时间/数组顺序/JSON 类型），不写入任何编号")
    use_instance(DB_C)  # C 现有 t-basic 1..5、t-other 1..2
    base_archive = export(["t-basic", "t-other"]).json()["result"]

    def mutated(fn):
        arch = copy.deepcopy(base_archive)
        fn(arch)
        return arch

    def snap(arch, idx=0):
        return arch["bulletins"][0]["versions"][idx]["snapshot"]

    r = import_archive(mutated(
        lambda a: snap(a, 1).update(
            {"penalties": [{"runner": 102, "seconds": 1, "reason": "改过的快照"}]}
        )
    ))
    check(r.status_code == 409, "重叠版本快照内容不同 → 409")
    check(r.json()["number"] == "t-basic" and r.json()["version"] == 2,
          "409 响应指出分叉的编号与版本")

    def change_ts(a):
        a["bulletins"][0]["versions"][1]["publishedAt"] = "2000-01-01T00:00:00+00:00"

    check(import_archive(mutated(change_ts)).status_code == 409,
          "重叠版本发布时间不同 → 409")

    def change_type(a):
        snap(a)["input"]["records"][0]["time"] = "0"  # 数字 0 → 字符串 "0"

    check(import_archive(mutated(change_type)).status_code == 409,
          "JSON 类型不同（数字 vs 字符串）→ 409")

    def change_bool(a):
        snap(a)["input"]["records"][0]["time"] = False  # 数字 0 → 布尔 false

    check(import_archive(mutated(change_bool)).status_code == 409,
          "JSON 类型不同（数字 vs 布尔）→ 409")

    def swap_order(a):
        snap(a)["reviews"].reverse()

    check(import_archive(mutated(swap_order)).status_code == 409,
          "数组顺序不同 → 409")

    def fork_with_newcomer(a):
        snap(a)["ranking"] = []  # t-basic v1 分叉
        a["bulletins"].append({
            "number": "t-atomic",
            "versions": [{
                "version": 1,
                "publishedAt": "2026-01-01T00:00:00+00:00",
                "snapshot": STUB_SNAPSHOT,
            }],
        })

    r = import_archive(mutated(fork_with_newcomer))
    check(r.status_code == 409, "同一归档中任一编号分叉 → 整次 409")
    check(read("t-atomic").status_code == 404, "分叉时同包的全新编号也不写入")
    check(read("t-basic").json()["result"]["version"] == 5, "分叉不改本地链")
    check(read("t-other").json()["result"]["version"] == 2, "分叉不改其他编号")
    print("test_import_fork_409_and_atomicity OK")


def test_import_format_422():
    print("场景8：格式非法整次 422（版本链/快照结构/格式版本），不写入任何编号")
    use_instance(DB_C)
    valid = export(["t-other"]).json()["result"]  # t-other 1..2 的合法归档

    check(import_archive({}).status_code == 422, "缺 format 422")
    check(import_archive({"format": 2, "bulletins": []}).status_code == 422,
          "不支持的格式版本 422")
    check(import_archive({"format": "1", "bulletins": []}).status_code == 422,
          "格式版本为字符串 422")
    check(import_archive({"format": True, "bulletins": []}).status_code == 422,
          "格式版本为布尔 422")
    check(import_archive({"format": 1}).status_code == 422, "缺 bulletins 422")
    check(import_archive({"format": 1, "bulletins": []}).status_code == 422,
          "空公告列表 422")
    check(import_archive({"format": 1, "bulletins": {}}).status_code == 422,
          "公告列表非数组 422")
    check(import_archive([1, 2]).status_code == 422, "归档非对象 422")

    def bad(mutate, msg):
        arch = copy.deepcopy(valid)
        mutate(arch)
        r = import_archive(arch)
        check(r.status_code == 422, msg)

    bad(lambda a: a["bulletins"].append(a["bulletins"][0]), "归档内编号重复 422")
    bad(lambda a: a["bulletins"][0].update({"number": ""}), "公告编号为空 422")
    bad(lambda a: a["bulletins"][0].update({"versions": []}), "空版本列表 422")
    bad(lambda a: a["bulletins"][0]["versions"].pop(0), "版本不从 1 起 422")

    def gap(a):
        a["bulletins"][0]["versions"][1]["version"] = 3

    bad(gap, "版本跳号不连续 422")

    def dup(a):
        a["bulletins"][0]["versions"].append(dict(a["bulletins"][0]["versions"][0]))

    bad(dup, "版本号重复 422")

    def float_ver(a):
        a["bulletins"][0]["versions"][0]["version"] = 1.5

    bad(float_ver, "版本号非整数 422")

    def zero_ver(a):
        a["bulletins"][0]["versions"][0]["version"] = 0

    bad(zero_ver, "版本号为 0 422")

    def no_ts(a):
        del a["bulletins"][0]["versions"][0]["publishedAt"]

    bad(no_ts, "缺发布时间 422")

    def empty_ts(a):
        a["bulletins"][0]["versions"][0]["publishedAt"] = ""

    bad(empty_ts, "空发布时间 422")

    def no_snap(a):
        del a["bulletins"][0]["versions"][0]["snapshot"]

    bad(no_snap, "缺快照 422")

    def snap_extra(a):
        a["bulletins"][0]["versions"][0]["snapshot"]["extra"] = 1

    bad(snap_extra, "快照多出字段 422")

    def snap_missing(a):
        del a["bulletins"][0]["versions"][0]["snapshot"]["ranking"]

    bad(snap_missing, "快照缺字段 422")

    def snap_type(a):
        a["bulletins"][0]["versions"][0]["snapshot"]["reviews"] = {}

    bad(snap_type, "快照字段容器类型不符 422")

    # 422 同样整次不写入：合法新编号与非法编号同包。
    arch = {
        "format": 1,
        "bulletins": [
            {"number": "t-fmt-new",
             "versions": [{"version": 1, "publishedAt": "2026-01-01T00:00:00+00:00",
                           "snapshot": STUB_SNAPSHOT}]},
            {"number": "t-fmt-bad",
             "versions": [{"version": 2, "publishedAt": "2026-01-01T00:00:00+00:00",
                           "snapshot": STUB_SNAPSHOT}]},
        ],
    }
    r = import_archive(arch)
    check(r.status_code == 422, "部分编号非法 → 整次 422")
    check(read("t-fmt-new").status_code == 404, "422 时合法编号也不写入")

    r = client.post(
        "/api/bulletins/import",
        content=b"{not json",
        headers={"Content-Type": "application/json"},
    )
    check(r.status_code == 400, "请求体不是 JSON 返回 400")
    print("test_import_format_422 OK")


def test_import_does_not_readjudicate():
    print("场景9：导入不重新裁决——结构合法的快照按原样保存与读取")
    use_instance(DB_C)
    arch = {
        "format": 1,
        "bulletins": [{
            "number": "t-stub",
            "versions": [{
                "version": 1,
                "publishedAt": "2026-02-03T04:05:06+00:00",
                "snapshot": {
                    "input": {"standards": {"smMin": 1}, "runners": [], "records": "raw"},
                    "adjudication": {"runners": [{"runner": 7, "lapCount": 99}]},
                    "reviews": [{"status": "pass"}],
                    "penalties": [],
                    "ranking": [{"rank": None}],
                },
            }],
        }],
    }
    r = import_archive(arch)
    check(r.status_code == 200 and r.json()["result"] == {"added": 1, "skipped": 0},
          "内容不经重新裁决也按原样导入")
    got = read("t-stub").json()["result"]
    check(got["publishedAt"] == "2026-02-03T04:05:06+00:00", "发布时间按归档原样保留")
    check(got["adjudication"] == {"runners": [{"runner": 7, "lapCount": 99}]},
          "快照内容逐字保留（不重新裁决、不改校验）")
    check(got["input"]["records"] == "raw", "输入快照原样保留")
    print("test_import_does_not_readjudicate OK")


def test_precision_roundtrip():
    print("场景10：大编号/大秒数/大用时经导出→导入全程精确")
    use_instance(DB_A)
    big_runner = "9007199254740993"
    ts, tm, tf = "9007199254740992", "9007199254741003", "9007199254741014"
    big_input = {
        "standards": RANGES,
        "runners": [{"number": big_runner}],
        "records": [
            {"id": "h1", "runner": big_runner, "gate": "S", "time": ts},
            {"id": "h2", "runner": big_runner, "gate": "M", "time": tm},
            {"id": "h3", "runner": big_runner, "gate": "F", "time": tf},
        ],
    }
    big_number = "9007199254740993"
    r = client.post("/api/bulletins/publish", json={
        "number": big_number,
        "baseVersion": 0,
        "input": big_input,
        "reviews": [review(big_runner, (("h1", ts), ("h2", tm), ("h3", tf)))],
        "penalties": [{"runner": big_runner, "seconds": "9007199254740996", "reason": "大罚秒"}],
    })
    check(r.status_code == 200, "大编号/大秒数发布成功")
    a_latest = read(big_number).json()["result"]

    text = client.post("/api/bulletins/export", json={"numbers": [big_number]}).text
    check(ts in text and tm in text and tf in text, "导出 JSON 文本保留超过 2^53 的秒数")
    check(f'"number":"{big_number}"' in text.replace(" ", ""), "导出保留大公告编号原文")
    archive = json.loads(text)["result"]

    use_instance(DB_D)
    r = import_archive(archive)
    check(r.status_code == 200 and r.json()["result"] == {"added": 1, "skipped": 0},
          "大整数归档导入成功")
    got = read(big_number).json()["result"]
    check(got == a_latest, "迁入实例读取结果与源实例完全一致")
    check(got["number"] == big_number, "大公告编号精确保留")
    check(got["ranking"][0]["adjusted"] == 9007199254741018, "大整数用时精确相加")
    check(read("9007199254740992").status_code == 404, "相差 1 的大编号互不相同")
    print("test_precision_roundtrip OK")


def test_concurrent_import_and_publish_coordination():
    print("场景11：并发导入/导入与发布按提交时库内状态协调，不覆盖旧版、不留断链或半批")
    path = os.path.join(_TMPDIR, "concurrent-import.db")
    store = BulletinStore(path)

    def snap(tag):
        return {"input": {"tag": tag}, "adjudication": {}, "reviews": [],
                "penalties": [], "ranking": []}

    versions = [
        {"version": i, "publishedAt": f"2026-01-0{i}T00:00:00+00:00", "snapshot": snap(f"v{i}")}
        for i in range(1, 4)
    ]
    entries = [("race", versions)]

    def do_import(_):
        try:
            return ("ok", store.import_chains(entries))
        except BulletinFork:
            return ("fork", None)

    with ThreadPoolExecutor(max_workers=6) as pool:
        outcomes = list(pool.map(do_import, range(6)))
    check(all(o[0] == "ok" for o in outcomes), "相同归档并发导入全部成功（后到者跳过）")
    check(sum(o[1][0] for o in outcomes) == 3, "并发导入新增总数恰为 3")
    check(sum(o[1][1] for o in outcomes) == 15, "其余版本全部跳过")
    check(store.current_version("race") == 3, "链完整为 1..3")

    # 导入（追加 v4）与发布（baseVersion=3 写 v4，内容不同）并发：
    # 无论谁先提交，都必须恰有一项成功，且旧版不被覆盖、链无断裂。
    versions4 = versions + [{
        "version": 4, "publishedAt": "2026-01-04T00:00:00+00:00", "snapshot": snap("v4"),
    }]
    results = {}
    barrier = threading.Barrier(2)

    def run_import():
        barrier.wait()
        try:
            results["import"] = ("ok", store.import_chains([("race", versions4)]))
        except BulletinFork:
            results["import"] = ("fork", None)

    def run_publish():
        barrier.wait()
        try:
            results["publish"] = ("ok", store.publish("race", 3, snap("local-v4")))
        except BulletinConflict as exc:
            results["publish"] = ("conflict", exc.current_version)

    t1 = threading.Thread(target=run_import)
    t2 = threading.Thread(target=run_publish)
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    pair = sorted([results["import"][0], results["publish"][0]])
    check(pair == ["conflict", "ok"] or pair == ["fork", "ok"],
          f"导入与发布并发恰有一项成功（实际 {pair}）")
    check(store.current_version("race") == 4, "链推进到 v4 且无断链")
    chain = [store.read("race", i) for i in range(1, 5)]
    check(all(c is not None for c in chain), "1..4 全部可读")
    check(chain[0]["input"] == {"tag": "v1"} and chain[2]["input"] == {"tag": "v3"},
          "旧版本不被覆盖")
    check(chain[0]["publishedAt"] == "2026-01-01T00:00:00+00:00", "旧版发布时间不被改写")
    store.close()
    print("test_concurrent_import_and_publish_coordination OK")


def test_export_consistent_snapshot():
    print("场景12：整包取自同一数据库时点（并发发布期间导出的归档链始终连续）")
    use_instance(DB_E)
    stop = threading.Event()
    errors = []

    def publisher():
        for i in range(16):
            if stop.is_set():
                return
            number = "t-cons-a" if i % 2 == 0 else "t-cons-b"
            r = publish(number, base=i // 2)
            if r.status_code != 200:
                errors.append(f"发布 {number} 失败：{r.status_code}")
        stop.set()

    def exporter():
        while not stop.is_set():
            r = export(["t-cons-a", "t-cons-b"])
            if r.status_code == 404:
                continue  # 另一编号尚未发布：整次 404 是合同行为
            if r.status_code != 200:
                errors.append(f"导出状态异常：{r.status_code}")
                continue
            chains = {
                b["number"]: [v["version"] for v in b["versions"]]
                for b in r.json()["result"]["bulletins"]
            }
            a, b = chains["t-cons-a"], chains["t-cons-b"]
            if a != list(range(1, len(a) + 1)) or b != list(range(1, len(b) + 1)):
                errors.append(f"归档断链：{chains}")
            # 发布顺序为 a1,b1,a2,b2,...：任一真实时点都有 len(a) ∈ {len(b), len(b)+1}
            if len(a) - len(b) not in (0, 1):
                errors.append(f"归档混合了不同时点：{chains}")

    threads = [threading.Thread(target=publisher)] + [
        threading.Thread(target=exporter) for _ in range(3)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    check(not errors, f"并发导出归档始终取自同一时点（{errors[:3]}）")
    check(read("t-cons-a").json()["result"]["version"] == 8, "发布后两编号各 8 版")
    print("test_export_consistent_snapshot OK")


def test_original_contract_untouched():
    print("场景13：原发布/读取/裁决合同不变（冒烟）")
    use_instance(DB_A)
    r = client.post("/api/adjudicate", json=INPUT)
    check(r.status_code == 200 and r.json()["result"]["runners"][0]["lapCount"] == 2,
          "裁决端点不变")
    r = publish("t-contract")
    check(r.status_code == 200 and r.json()["result"]["version"] == 1, "发布端点不变")
    check(read("t-contract").json()["result"]["version"] == 1, "读取端点不变")
    r = client.post("/api/bulletins/publish", json={"number": "t-contract", "baseVersion": 0,
                                                    "input": INPUT, "reviews": ALL_REVIEWS,
                                                    "penalties": []})
    check(r.status_code == 409, "版本冲突合同不变")
    print("test_original_contract_untouched OK")


def main():
    test_export_basic()
    test_export_errors()
    test_roundtrip_and_continue_publish()
    test_reimport_skip_and_local_longer()
    test_import_appends_missing_suffix()
    test_import_ignores_key_order()
    test_import_fork_409_and_atomicity()
    test_import_format_422()
    test_import_does_not_readjudicate()
    test_precision_roundtrip()
    test_concurrent_import_and_publish_coordination()
    test_export_consistent_snapshot()
    test_original_contract_untouched()
    print("\nALL BULLETIN TRANSFER TESTS PASSED" if failures == 0 else f"\n{failures} ASSERTION(S) FAILED")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
