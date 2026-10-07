"""FastAPI 入口：提供裁决 API、设备日志合并预览 API、成绩公告 API 与静态页面。"""

from __future__ import annotations

import json
import os
import threading
from pathlib import Path

from fastapi import FastAPI, File, Form, Request, UploadFile
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from .adjudication import AdjudicationError, adjudicate, diagnose, find_missing
from .bulletin import (
    BulletinConflict,
    BulletinDivergence,
    BulletinError,
    BulletinNotFound,
    BulletinStore,
    export_bulletins,
    import_bulletins,
    publish_bulletin,
)
from .merge import MergeError, preview_merge

app = FastAPI(title="本地赛道计次裁决台", version="1.5.0")

STATIC_DIR = Path(__file__).resolve().parent.parent / "static"

# 成绩公告的 SQLite 存储：默认放在项目 data/ 目录（Docker 中由 Compose
# 通过 BULLETIN_DB_PATH 指到数据卷 /data/bulletins.db），跨重启保留。
# 懒加载：只有真正访问公告端点时才创建库文件，不影响既有端点与测试。
_bulletin_store: BulletinStore | None = None
_bulletin_store_lock = threading.Lock()


def get_bulletin_store() -> BulletinStore:
    global _bulletin_store
    with _bulletin_store_lock:
        if _bulletin_store is None:
            path = os.environ.get("BULLETIN_DB_PATH") or str(
                Path(__file__).resolve().parent.parent / "data" / "bulletins.db"
            )
            _bulletin_store = BulletinStore(path)
    return _bulletin_store


@app.post("/api/adjudicate")
async def api_adjudicate(request: Request) -> JSONResponse:
    try:
        payload = await request.json()
    except (json.JSONDecodeError, UnicodeDecodeError):
        return JSONResponse(
            status_code=400, content={"ok": False, "error": "请求体不是合法的 JSON"}
        )
    try:
        result = adjudicate(payload)
    except AdjudicationError as exc:
        # 非法输入：返回错误，不展示任何部分结果。
        return JSONResponse(status_code=422, content={"ok": False, "error": str(exc)})
    return JSONResponse(content={"ok": True, "result": result})


@app.post("/api/diagnose")
async def api_diagnose(request: Request) -> JSONResponse:
    """替代性诊断：请求体与 /api/adjudicate 完全相同（同一套校验与状态码）。

    纯只读分析：不改变任何裁决行为，只针对各选手的未采纳记录给出
    “同优可替代 / 仅次优可用 / 无法成趟”分类及同优替代方案。
    """
    try:
        payload = await request.json()
    except (json.JSONDecodeError, UnicodeDecodeError):
        return JSONResponse(
            status_code=400, content={"ok": False, "error": "请求体不是合法的 JSON"}
        )
    try:
        result = diagnose(payload)
    except AdjudicationError as exc:
        return JSONResponse(status_code=422, content={"ok": False, "error": str(exc)})
    return JSONResponse(content={"ok": True, "result": result})


@app.post("/api/missing")
async def api_missing(request: Request) -> JSONResponse:
    """查找漏读：请求体与 /api/adjudicate 完全相同（同一套校验与状态码）。

    纯只读提示：不追加记录、不改变任何裁决行为。对每名选手枚举两条不同门
    记录与缺失门 S/M/F，返回“只补一条该门虚拟读数即可使最大计次数较原输入
    增加 1”的缺失读数秒数闭区间（覆盖全部可行秒数；不能增次的记录对不返回）。
    """
    try:
        payload = await request.json()
    except (json.JSONDecodeError, UnicodeDecodeError):
        return JSONResponse(
            status_code=400, content={"ok": False, "error": "请求体不是合法的 JSON"}
        )
    try:
        result = find_missing(payload)
    except AdjudicationError as exc:
        return JSONResponse(status_code=422, content={"ok": False, "error": str(exc)})
    return JSONResponse(content={"ok": True, "result": result})


async def _read_json_context(context: str | None) -> object:
    """读取 multipart 表单中的 context 字段（当前选手与记录表的 JSON）。"""
    if context is None:
        raise MergeError("缺少当前记录 context 字段")
    try:
        return json.loads(context)
    except (json.JSONDecodeError, UnicodeDecodeError):
        raise MergeError("当前记录 context 不是合法的 JSON")


async def _handle_merge_request(context: str | None, file: UploadFile | None) -> JSONResponse:
    if file is None:
        return JSONResponse(
            status_code=400, content={"ok": False, "error": "缺少上传的 CSV 文件（file）"}
        )
    try:
        context_obj = await _read_json_context(context)
        data = await file.read()
        result = preview_merge(context_obj, data)
    except MergeError as exc:
        # 整批拒绝：只返回中文原因，不产生任何合并结果。
        return JSONResponse(status_code=422, content={"ok": False, "error": str(exc)})
    except AdjudicationError as exc:
        # 当前记录本身非法（如选手编号重复、ID 重复）时同样拒绝预览。
        return JSONResponse(status_code=422, content={"ok": False, "error": str(exc)})
    return JSONResponse(content={"ok": True, "result": result})


@app.post("/api/import/preview")
async def api_import_preview(
    context: str | None = Form(default=None), file: UploadFile | None = File(default=None)
) -> JSONResponse:
    """设备日志合并预览（只读）：multipart 表单，context 为当前记录 JSON，
    file 为 UTF-8 CSV。返回新增与重复条目，不修改任何数据。
    """
    return await _handle_merge_request(context, file)


@app.post("/api/import/confirm")
async def api_import_confirm(
    context: str | None = Form(default=None), file: UploadFile | None = File(default=None)
) -> JSONResponse:
    """设备日志合并确认：与预览同一套解析与整批校验，返回同样的合并清单。

    服务端不保存状态：真正追加发生在页面端；确认时服务端重新独立校验，
    只有输入与预览所依据的当前记录、文件内容一致时结果才会与预览相同。
    """
    return await _handle_merge_request(context, file)


@app.post("/api/bulletins/publish")
async def api_bulletin_publish(request: Request) -> JSONResponse:
    """发布成绩公告：以点击时快照提交完整裁决输入、隔离、已保存复核、罚秒
    与所见公告版本（首次为 0）。服务端重新裁决并逐趟核对复核（每趟均须
    “通过”，零次无需复核），罚秒/原因/备注沿用原校验，名次按原规则在服务
    端生成；只有所见版本与当前最新版本一致时才保存为下一版本并返回公告
    链接。过期返回 409；任何失败都不留版本、也不改动现有公告。
    """
    try:
        payload = await request.json()
    except (json.JSONDecodeError, UnicodeDecodeError):
        return JSONResponse(
            status_code=400, content={"ok": False, "error": "请求体不是合法的 JSON"}
        )
    try:
        result = publish_bulletin(payload, get_bulletin_store())
    except BulletinConflict as exc:
        # 同版本并发发布只有一项成功；过期发布不留版本、不改现公告。
        return JSONResponse(
            status_code=409,
            content={
                "ok": False,
                "error": f"{exc}，本次未发布；请基于最新版本重新发布",
                "currentVersion": exc.current_version,
            },
        )
    except (BulletinError, AdjudicationError) as exc:
        # 非法输入/未知选手处罚/不匹配复核：整次拒绝，不留版本。
        return JSONResponse(status_code=422, content={"ok": False, "error": str(exc)})
    return JSONResponse(content={"ok": True, "result": result})


@app.post("/api/bulletins/export")
async def api_bulletin_export(request: Request) -> JSONResponse:
    """导出所选编号完整版本链的 JSON 归档（格式版本 1，只读）。

    归档含每版的编号、版本、发布时间与完整快照，整包取自同一数据库时点；
    任一编号不存在整次返回 404，不生成部分归档。归档可直接提交给另一实例的
    /api/bulletins/import 完成迁移。
    """
    try:
        payload = await request.json()
    except (json.JSONDecodeError, UnicodeDecodeError):
        return JSONResponse(
            status_code=400, content={"ok": False, "error": "请求体不是合法的 JSON"}
        )
    try:
        archive = export_bulletins(payload, get_bulletin_store())
    except BulletinNotFound as exc:
        # 未知编号：整次 404，不生成部分归档。
        return JSONResponse(status_code=404, content={"ok": False, "error": str(exc)})
    except BulletinError as exc:
        return JSONResponse(status_code=422, content={"ok": False, "error": str(exc)})
    return JSONResponse(content={"ok": True, "result": archive})


@app.post("/api/bulletins/import")
async def api_bulletin_import(request: Request) -> JSONResponse:
    """把另一实例导出的公告归档并入本库：不重新裁决、不覆盖本地历史。

    每个编号须从 1 起连续且无重复版本；重叠版本的快照与发布时间须相同
    （对象键序忽略，数组顺序与 JSON 类型保留），相同项跳过、只追加缺失
    后缀，本地链更长则保留。任一编号分叉返回 409、格式非法返回 422，
    均不写入任何编号；成功返回新增与跳过数。导入与既有发布按提交时库内
    状态在同一写事务中协调，不留断链或半批。
    """
    try:
        payload = await request.json()
    except (json.JSONDecodeError, UnicodeDecodeError):
        return JSONResponse(
            status_code=400, content={"ok": False, "error": "请求体不是合法的 JSON"}
        )
    try:
        result = import_bulletins(payload, get_bulletin_store())
    except BulletinDivergence as exc:
        # 任一编号分叉：整批回滚，不写入任何编号。
        return JSONResponse(status_code=409, content={"ok": False, "error": str(exc)})
    except BulletinError as exc:
        # 格式非法：同样不写入任何编号。
        return JSONResponse(status_code=422, content={"ok": False, "error": str(exc)})
    return JSONResponse(content={"ok": True, "result": result})


@app.get("/api/bulletins/{number}")
async def api_bulletin_read(number: str, version: str | None = None) -> JSONResponse:
    """按编号与可选版本读取成绩公告（只读）；缺省读最新版本。

    旧版本永久只读：后续编辑、清空与刷新都不影响已发布的公告。
    未知编号或版本返回 404。
    """
    parsed_version: int | None = None
    if version is not None:
        if not version.isascii() or not version.isdigit() or int(version) <= 0:
            return JSONResponse(
                status_code=400,
                content={"ok": False, "error": "版本号必须是正整数"},
            )
        parsed_version = int(version)
    bulletin = get_bulletin_store().read(number, parsed_version)
    if bulletin is None:
        return JSONResponse(
            status_code=404, content={"ok": False, "error": "公告不存在"}
        )
    return JSONResponse(content={"ok": True, "result": bulletin})


app.mount("/", StaticFiles(directory=STATIC_DIR, html=True), name="static")
