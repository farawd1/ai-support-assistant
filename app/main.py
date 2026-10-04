import json
import logging
import os
import re
import sqlite3
import time
from contextlib import asynccontextmanager, contextmanager
from pathlib import Path
from uuid import uuid4

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from typing import Literal

from .providers import TOPICS, classify, generate, priority

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / '.env')
DB = os.getenv('DATABASE_PATH', str(ROOT / 'data/support.db'))
MODE = os.getenv('AI_MODE', 'demo')


@contextmanager
def db():
    conn = sqlite3.connect(DB, timeout=10)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    Path(DB).parent.mkdir(parents=True, exist_ok=True)
    with db() as c:
        c.executescript('''
        PRAGMA journal_mode=WAL;
        CREATE TABLE IF NOT EXISTS tickets(id TEXT PRIMARY KEY, text TEXT NOT NULL, created REAL NOT NULL, state TEXT NOT NULL, result TEXT, error TEXT, action TEXT, final_reply TEXT, latency REAL);
        CREATE TABLE IF NOT EXISTS documents(id INTEGER PRIMARY KEY, title TEXT NOT NULL, body TEXT NOT NULL, topic TEXT NOT NULL, version INTEGER NOT NULL DEFAULT 1);
        CREATE VIRTUAL TABLE IF NOT EXISTS search USING fts5(title,body,content='documents',content_rowid='id',tokenize='unicode61');
        CREATE TABLE IF NOT EXISTS actions(id INTEGER PRIMARY KEY, ticket_id TEXT NOT NULL, action TEXT NOT NULL, reply TEXT NOT NULL, created REAL NOT NULL);
        ''')
        if not c.execute('SELECT COUNT(*) FROM documents').fetchone()[0]:
            samples = [
                ('Возврат товара', 'В демонстрационной компании возврат товара возможен в течение 14 дней после получения при сохранении товарного вида. Для оформления оператору нужен номер заказа. Возврат денег выполняется после проверки товара; конкретный срок уточняется специалистом.', 'refund'),
                ('Проблемы с оплатой', 'Если клиент сообщает о двойном списании, оператор запрашивает номер заказа и дату платежа и передаёт обращение платёжному специалисту. Нельзя подтверждать возврат или повторное списание без проверки платёжной системы. Данные карты и CVV запрашивать запрещено.', 'payment'),
                ('Статус доставки', 'Статус доставки проверяется по номеру заказа в системе заказов. Если срок доставки прошёл, обращение передаётся специалисту по доставке. Нельзя обещать дату получения без подтверждения перевозчика.', 'delivery'),
                ('Восстановление доступа', 'Для восстановления доступа используйте функцию «Забыли пароль» на странице входа. Не передавайте пароль и одноразовые коды оператору. При подозрении на взлом обращение передаётся специалисту по безопасности.', 'account'),
                ('Техническая ошибка', 'Для диагностики технической ошибки запросите описание действия, код ошибки и время возникновения. Скриншот должен быть без паролей и платёжных данных. При полной недоступности сервиса обращение передаётся техническому специалисту.', 'technical'),
            ]
            for title, body, topic in samples:
                cur = c.execute('INSERT INTO documents(title,body,topic) VALUES(?,?,?)', (title, body, topic))
                c.execute('INSERT INTO search(rowid,title,body) VALUES(?,?,?)', (cur.lastrowid, title, body))


@asynccontextmanager
async def lifespan(app):
    if MODE not in ('demo', 'live'):
        raise RuntimeError('AI_MODE должен быть demo или live')
    if MODE == 'live' and not all(os.getenv(k) for k in ['TYPESAFE_API_KEY', 'DEEPSEEK_API_KEY']):
        raise RuntimeError('Для live нужны TYPESAFE_API_KEY и DEEPSEEK_API_KEY в .env')
    init_db()
    with db() as c:
        c.execute("UPDATE tickets SET state='failed',error='Обработка прервана перезапуском. Повторите анализ.' WHERE state='processing'")
    yield


app = FastAPI(title='Support Studio • Jev + DeepSeek', lifespan=lifespan)
app.mount('/static', StaticFiles(directory=ROOT / 'static'), name='static')


@app.get('/')
def home():
    return FileResponse(ROOT / 'static/index.html')


@app.get('/api/config')
def config():
    return {'mode': MODE, 'jev': os.getenv('JEV_MODEL', 'jev-1.13.0'), 'deepseek': os.getenv('DEEPSEEK_MODEL', 'deepseek-flash'), 'topics': TOPICS}


def get_ticket(ticket_id):
    with db() as c:
        row = c.execute('SELECT * FROM tickets WHERE id=?', (ticket_id,)).fetchone()
    if not row:
        raise HTTPException(404, 'Обращение не найдено')
    item = dict(row)
    item['result'] = json.loads(item['result']) if item['result'] else None
    return item


def retrieve(text, topic):
    tokens = re.findall(r'[\w]{3,}', text.lower())[:40]
    stop = {'мне', 'что', 'как', 'это', 'мой', 'меня', 'для', 'пожалуйста', 'здравствуйте'}
    tokens = list(dict.fromkeys(t for t in tokens if t not in stop))
    if not tokens:
        return []
    query = ' OR '.join('"' + t + '"*' for t in tokens)
    with db() as c:
        rows = c.execute('SELECT d.* FROM search JOIN documents d ON d.id=search.rowid WHERE search MATCH ? ORDER BY bm25(search) LIMIT 12', (query,)).fetchall()
    docs = [dict(r) for r in rows]
    # Topic is a ranking preference, never a substitute for a text match.
    docs.sort(key=lambda d: d['topic'] != topic)
    return docs[:3]


async def analyze(ticket_id):
    ticket = get_ticket(ticket_id)
    start = time.perf_counter()
    try:
        classification, jev = await classify(ticket['text'], MODE)
        p, reasons = priority(classification)
        documents = retrieve(ticket['text'], classification.topic)
        draft, deepseek = await generate(ticket['text'], documents, MODE, p)
        result = {'classification': classification.model_dump(), 'priority': p, 'reasons': reasons, 'needs_review': classification.confidence < .7 or p == 'P0', 'documents': documents, 'draft': draft.model_dump(), 'providers': {'jev': jev, 'deepseek': deepseek}, 'prompt_version': 'support-v1', 'policy_version': 'demo-v1'}
        with db() as c:
            c.execute("UPDATE tickets SET state='ready',result=?,latency=?,error=NULL WHERE id=?", (json.dumps(result, ensure_ascii=False), time.perf_counter() - start, ticket_id))
    except Exception:
        # Do not expose provider bodies or credentials in logs or UI.
        logging.warning('Analysis failed for ticket %s', ticket_id)
        with db() as c:
            c.execute("UPDATE tickets SET state='failed',error=? WHERE id=?", ('Не удалось завершить анализ. Проверьте настройки провайдеров и повторите попытку.', ticket_id))
    return get_ticket(ticket_id)


class TicketInput(BaseModel):
    text: str = Field(min_length=3, max_length=12000)


@app.post('/api/tickets')
async def create_ticket(payload: TicketInput):
    if len(payload.text.strip()) < 3:
        raise HTTPException(422, 'Введите содержательное обращение')
    ticket_id = uuid4().hex[:12]
    with db() as c:
        c.execute('INSERT INTO tickets(id,text,created,state) VALUES(?,?,?,?)', (ticket_id, payload.text.strip(), time.time(), 'processing'))
    return await analyze(ticket_id)


@app.get('/api/tickets')
def list_tickets():
    with db() as c:
        rows = c.execute('SELECT id FROM tickets ORDER BY created DESC LIMIT 200').fetchall()
    return [get_ticket(r['id']) for r in rows]


@app.get('/api/tickets/{ticket_id}')
def detail(ticket_id: str):
    return get_ticket(ticket_id)


@app.post('/api/tickets/{ticket_id}/analyze')
async def retry(ticket_id: str):
    get_ticket(ticket_id)
    with db() as c:
        changed = c.execute("UPDATE tickets SET state='processing',result=NULL,error=NULL,action=NULL,final_reply=NULL WHERE id=? AND state!='processing'", (ticket_id,)).rowcount
    if not changed:
        raise HTTPException(409, 'Анализ уже выполняется')
    return await analyze(ticket_id)


class ActionInput(BaseModel):
    action: Literal['accepted', 'edited', 'rejected', 'escalated']
    reply: str = Field(default='', max_length=10000)


@app.post('/api/tickets/{ticket_id}/action')
def action(ticket_id: str, payload: ActionInput):
    with db() as c:
        row = c.execute('SELECT * FROM tickets WHERE id=?', (ticket_id,)).fetchone()
        if not row:
            raise HTTPException(404, 'Обращение не найдено')
        if row['state'] != 'ready':
            raise HTTPException(409, 'Дождитесь готового анализа')
        draft = json.loads(row['result'])['draft']['reply']
        reply = payload.reply.strip()
        if payload.action in ('accepted', 'edited') and not reply:
            raise HTTPException(422, 'Ответ не может быть пустым')
        actual = 'edited' if payload.action == 'accepted' and reply != draft else payload.action
        c.execute('UPDATE tickets SET action=?,final_reply=? WHERE id=?', (actual, reply, ticket_id))
        c.execute('INSERT INTO actions(ticket_id,action,reply,created) VALUES(?,?,?,?)', (ticket_id, actual, reply, time.time()))
    return get_ticket(ticket_id)


class DocumentInput(BaseModel):
    title: str = Field(min_length=3, max_length=200)
    body: str = Field(min_length=20, max_length=18000)
    topic: Literal['payment', 'delivery', 'refund', 'account', 'technical', 'information', 'other']


@app.get('/api/documents')
def documents():
    with db() as c:
        return [dict(r) for r in c.execute('SELECT * FROM documents ORDER BY id DESC').fetchall()]


@app.post('/api/documents')
def add_document(payload: DocumentInput):
    with db() as c:
        cur = c.execute('INSERT INTO documents(title,body,topic) VALUES(?,?,?)', (payload.title, payload.body, payload.topic))
        c.execute('INSERT INTO search(rowid,title,body) VALUES(?,?,?)', (cur.lastrowid, payload.title, payload.body))
        return {'id': cur.lastrowid}


@app.get('/api/analytics')
def analytics():
    with db() as c:
        rows = c.execute('SELECT result,action,state,latency FROM tickets').fetchall()
    topics, priorities = {}, {}
    missing = 0
    for row in rows:
        if row['result']:
            r = json.loads(row['result'])
            t, p = r['classification']['topic'], r['priority']
            topics[t] = topics.get(t, 0) + 1
            priorities[p] = priorities.get(p, 0) + 1
            missing += r['draft']['status'] == 'clarify'
    reviewed = sum(bool(r['action']) for r in rows)
    accepted = sum(r['action'] == 'accepted' for r in rows)
    return {'total': len(rows), 'open': sum(not r['action'] for r in rows), 'reviewed': reviewed, 'accepted': accepted, 'acceptance_rate': accepted / reviewed if reviewed else None, 'missing': missing, 'failed': sum(r['state'] == 'failed' for r in rows), 'topics': topics, 'priorities': priorities, 'average_latency': round(sum(r['latency'] or 0 for r in rows) / max(1, sum(r['latency'] is not None for r in rows)), 2)}
