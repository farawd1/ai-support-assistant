import asyncio
import json
import os
import sqlite3
import time
import unicodedata
from collections import Counter
from contextlib import asynccontextmanager, contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from uuid import uuid4
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from typing import Literal
from . import providers

ROOT=Path(__file__).resolve().parent.parent
load_dotenv(ROOT/'.env')
DB=os.getenv('DATABASE_PATH',str(ROOT/'data/support.db'))
if not Path(DB).is_absolute(): DB=str(ROOT/DB)
TOPIC_LOCK=asyncio.Lock()

@contextmanager
def db():
    conn=sqlite3.connect(DB,timeout=20)
    conn.row_factory=sqlite3.Row
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def init_db():
    Path(DB).parent.mkdir(parents=True,exist_ok=True)
    with db() as c:
        c.executescript('''
        PRAGMA journal_mode=WAL;
        CREATE TABLE IF NOT EXISTS tickets(id TEXT PRIMARY KEY,text TEXT NOT NULL,created REAL NOT NULL,state TEXT NOT NULL,result TEXT,error TEXT,action TEXT,final_reply TEXT,latency REAL);
        CREATE TABLE IF NOT EXISTS actions(id INTEGER PRIMARY KEY,ticket_id TEXT NOT NULL,action TEXT NOT NULL,reply TEXT NOT NULL,created REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS topics(id INTEGER PRIMARY KEY,title TEXT NOT NULL,normalized TEXT UNIQUE NOT NULL,description TEXT NOT NULL,created REAL NOT NULL,model TEXT NOT NULL,evidence_ids TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY,value TEXT NOT NULL);
        ''')
        columns={r['name'] for r in c.execute('PRAGMA table_info(tickets)')}
        for name,kind in [('source',"TEXT NOT NULL DEFAULT 'manual'"),('received_at','REAL'),('analyzed_at','REAL'),('topic_id','INTEGER'),('reviewed_at','REAL'),('decision_actor','TEXT'),('auto_state',"TEXT NOT NULL DEFAULT 'pending'"),('auto_decision','TEXT')]:
            if name not in columns: c.execute(f'ALTER TABLE tickets ADD COLUMN {name} {kind}')
        action_columns={r['name'] for r in c.execute('PRAGMA table_info(actions)')}
        if 'actor' not in action_columns: c.execute("ALTER TABLE actions ADD COLUMN actor TEXT NOT NULL DEFAULT 'human'")
        if 'reason' not in action_columns: c.execute("ALTER TABLE actions ADD COLUMN reason TEXT NOT NULL DEFAULT ''")
        c.execute("INSERT OR IGNORE INTO settings VALUES('autopilot','off')")
        c.execute("INSERT OR IGNORE INTO settings VALUES('autopilot_revision','0')")
        if not c.execute("SELECT 1 FROM settings WHERE key='live-v2'").fetchone():
            # Keep all input text; retire simulated times, decisions and template replies.
            stamp=time.time()
            c.execute("DELETE FROM actions WHERE ticket_id IN (SELECT id FROM tickets WHERE id LIKE 'sim-%')")
            c.execute("UPDATE tickets SET source='synthetic',received_at=NULL,created=?,state='queued',result=NULL,error=NULL,action=NULL,final_reply=NULL,latency=NULL,analyzed_at=NULL,topic_id=NULL,reviewed_at=NULL WHERE id LIKE 'sim-%'",(stamp,))
            c.execute("UPDATE tickets SET received_at=created WHERE source='manual' AND received_at IS NULL")
            c.execute("INSERT INTO settings VALUES('live-v2',?)",(str(stamp),))
        # Legacy demo policies are archived in the pre-migration backup only.
        c.execute('DROP TABLE IF EXISTS search')
        c.execute('DROP TABLE IF EXISTS documents')
        for row in c.execute('SELECT id,result FROM tickets WHERE result IS NOT NULL').fetchall():
            if 'title' not in json.loads(row['result']).get('classification',{}):
                c.execute("UPDATE tickets SET state='queued',result=NULL,topic_id=NULL,analyzed_at=NULL,latency=NULL WHERE id=?",(row['id'],))

def normalize(title):
    return ' '.join(unicodedata.normalize('NFKC',title).casefold().split())

def catalog():
    with db() as c:
        return [dict(r) for r in c.execute('SELECT id,title,description FROM topics ORDER BY id')]

def get_ticket(ticket_id):
    with db() as c:
        row=c.execute('SELECT * FROM tickets WHERE id=?',(ticket_id,)).fetchone()
    if row is None: raise HTTPException(404,'Обращение не найдено')
    t=dict(row)
    t['result']=json.loads(t['result']) if t['result'] else None
    t['auto_decision']=json.loads(t['auto_decision']) if t['auto_decision'] else None
    with db() as c:
        t['history']=[dict(r) for r in c.execute('SELECT action,actor,reason,reply,created FROM actions WHERE ticket_id=? ORDER BY id DESC LIMIT 20',(ticket_id,))]
    return t

def autopilot_settings(c):
    return {'enabled':c.execute("SELECT value FROM settings WHERE key='autopilot'").fetchone()[0]=='on',
            'revision':int(c.execute("SELECT value FROM settings WHERE key='autopilot_revision'").fetchone()[0])}

async def auto_step():
    with db() as c:
        c.execute('BEGIN IMMEDIATE')
        settings=autopilot_settings(c)
        if not settings['enabled']: return False
        row=c.execute("SELECT * FROM tickets WHERE state='ready' AND action IS NULL AND auto_state='pending' ORDER BY created LIMIT 1").fetchone()
        if row is None: return False
        c.execute("UPDATE tickets SET auto_state='processing' WHERE id=?",(row['id'],))
    result=json.loads(row['result'])
    try:
        decision,meta=await providers.evaluate_decision(row['text'],result)
        action=decision.action
        reason=decision.reason
        if result['priority'] in ('P0','P1') or result['draft']['status']=='escalate':
            action='escalate'
            reason='Требуется специалист. '+reason
        elif decision.requires_external_action or decision.confidence < .9:
            action='manual'
        elif action=='accept' and result['draft']['status']!='draft':
            action='manual'
        elif action=='clarify' and result['draft']['status']!='clarify':
            action='manual'
        saved={**decision.model_dump(),'effective_action':action,'reason':reason,'provider':meta,'at':time.time()}
        with db() as c:
            c.execute('BEGIN IMMEDIATE')
            current=autopilot_settings(c)
            latest=c.execute('SELECT state,action,analyzed_at,auto_state FROM tickets WHERE id=?',(row['id'],)).fetchone()
            if latest['action'] or latest['state']!='ready' or latest['analyzed_at']!=row['analyzed_at']:
                return True
            if not current['enabled'] or current['revision']!=settings['revision']:
                c.execute("UPDATE tickets SET auto_state='pending' WHERE id=?",(row['id'],))
                return True
            c.execute("UPDATE tickets SET auto_state='done',auto_decision=? WHERE id=?",(json.dumps(saved,ensure_ascii=False),row['id']))
            if action!='manual':
                actual={'accept':'accepted','clarify':'clarified','escalate':'escalated'}[action]
                # Escalation routes to the local human queue, never sends the draft.
                reply=result['draft']['reply'] if action!='escalate' else ''
                now=time.time()
                c.execute("UPDATE tickets SET action=?,final_reply=?,decision_actor='agent',reviewed_at=? WHERE id=?",(actual,reply,now,row['id']))
                c.execute("INSERT INTO actions(ticket_id,action,reply,created,actor,reason) VALUES(?,?,?,?,'agent',?)",(row['id'],actual,reply,now,reason))
        return True
    except Exception as error:
        reason=f'{error.provider}: {error.code}' if isinstance(error,providers.ProviderError) else 'Не удалось проверить решение'
        with db() as c:
            c.execute("UPDATE tickets SET auto_state='failed',auto_decision=? WHERE id=? AND action IS NULL AND auto_state='processing' AND analyzed_at=?",(json.dumps({'effective_action':'manual','reason':reason},ensure_ascii=False),row['id'],row['analyzed_at']))
        return True

def save_topic(c,title,description,model,evidence_ids):
    normalized=normalize(title)
    c.execute('INSERT OR IGNORE INTO topics(title,normalized,description,created,model,evidence_ids) VALUES(?,?,?,?,?,?)',(title.strip(),normalized,description,time.time(),model,json.dumps(evidence_ids)))
    return c.execute('SELECT id FROM topics WHERE normalized=?',(normalized,)).fetchone()['id']

async def analyze(ticket_id):
    t=get_ticket(ticket_id)
    start=time.perf_counter()
    try:
        risks,jev=await providers.assess_risks(t['text'])
        known=catalog()
        result,deepseek=await providers.analyze_case(t['text'],known,risks)
        async with TOPIC_LOCK:
            # Re-evaluate against topics created while this request was in flight.
            if result.topic_id is None and catalog()!=known:
                result,deepseek=await providers.analyze_case(t['text'],catalog(),risks)
            p,reasons=providers.priority(risks)
            with db() as c:
                topic_id=result.topic_id
                if topic_id is None:
                    topic_id=save_topic(c,result.new_topic.title,result.new_topic.description,deepseek['model'],[ticket_id])
                topic=dict(c.execute('SELECT id,title,description FROM topics WHERE id=?',(topic_id,)).fetchone())
                final={'classification':{'topic':str(topic_id),'title':topic['title'],**risks.model_dump()},'priority':p,'reasons':reasons,'reasoning':result.reasoning,'evidence_quote':result.evidence_quote,'draft':{'status':result.status,'reply':result.reply,'operator_note':result.operator_note},'providers':{'jev':jev,'deepseek':deepseek},'analysis_mode':'live','synthetic_input':t['source']=='synthetic'}
                c.execute("UPDATE tickets SET state='ready',result=?,error=NULL,latency=?,analyzed_at=?,topic_id=? WHERE id=?",(json.dumps(final,ensure_ascii=False),time.perf_counter()-start,time.time(),topic_id,ticket_id))
    except Exception as error:
        message='Некорректный ответ AI. Повторите анализ.'
        if isinstance(error,providers.ProviderError):
            labels={'401':'Ключ не принят','402':'Недостаточно средств','403':'Доступ запрещён','429':'Лимит запросов','network':'Сеть или таймаут'}
            message=f'{error.provider}: {labels.get(error.code,"ошибка ответа")} ({error.code})'
        with db() as c:
            c.execute("UPDATE tickets SET state='failed',error=?,latency=?,analyzed_at=? WHERE id=?",(message,time.perf_counter()-start,time.time(),ticket_id))
    return get_ticket(ticket_id)

async def bootstrap_topics():
    if catalog(): return
    with db() as c:
        tickets=[dict(r) for r in c.execute("SELECT id,text FROM tickets WHERE state='queued' LIMIT 200")]
    if not tickets: return
    try:
        discovered,meta=await providers.discover_topics(tickets)
        with db() as c:
            for topic in discovered.topics:
                save_topic(c,topic.title,topic.description,meta['model'],topic.evidence_ids)
            c.execute("INSERT OR REPLACE INTO settings VALUES('discovery',?)",(json.dumps({'at':time.time(),**meta}),))
    except Exception as error:
        message=f'{error.provider}: {error.code}' if isinstance(error,providers.ProviderError) else 'Некорректный ответ при выделении тем'
        with db() as c:
            c.execute("INSERT OR REPLACE INTO settings VALUES('discovery_error',?)",(message,))
        # Individual analysis can still propose topics. No synthetic fallback.

async def worker():
    while True:
        with db() as c:
            c.execute('BEGIN IMMEDIATE')
            row=c.execute("SELECT id FROM tickets WHERE state='queued' ORDER BY created,id LIMIT 1").fetchone()
            if row: c.execute("UPDATE tickets SET state='processing' WHERE id=?",(row['id'],))
        if row:
            await analyze(row['id'])
        else:
            if not await auto_step(): await asyncio.sleep(.75)

async def start_workers():
    await bootstrap_topics()
    async with asyncio.TaskGroup() as tg:
        for _ in range(4): tg.create_task(worker())

@asynccontextmanager
async def lifespan(app):
    init_db()
    with db() as c:
        c.execute("UPDATE tickets SET state='queued' WHERE state='processing'")
        c.execute("UPDATE tickets SET auto_state='pending' WHERE auto_state='processing'")
    task=asyncio.create_task(start_workers())
    yield
    task.cancel()
    try: await task
    except asyncio.CancelledError: pass

app=FastAPI(title='Support',lifespan=lifespan)
app.mount('/static',StaticFiles(directory=ROOT/'static'),name='static')
@app.get('/')
def home(): return FileResponse(ROOT/'static/index.html')
@app.get('/api/config')
def config(): return {'mode':'live','topics':{str(t['id']):t['title'] for t in catalog()}}

@app.get('/api/autopilot')
def autopilot():
    with db() as c:
        return {**autopilot_settings(c),'processing':c.execute("SELECT COUNT(*) FROM tickets WHERE auto_state='processing'").fetchone()[0], 'pending':c.execute("SELECT COUNT(*) FROM tickets WHERE state='ready' AND action IS NULL AND auto_state='pending'").fetchone()[0]}

class AutopilotInput(BaseModel):
    enabled: bool

@app.post('/api/autopilot')
def set_autopilot(payload:AutopilotInput):
    with db() as c:
        c.execute('BEGIN IMMEDIATE')
        c.execute("UPDATE settings SET value=? WHERE key='autopilot'",('on' if payload.enabled else 'off',))
        c.execute("UPDATE settings SET value=CAST(value AS INTEGER)+1 WHERE key='autopilot_revision'")
    return autopilot()

@app.post('/api/autopilot/retry')
def retry_autopilot():
    with db() as c:
        count=c.execute("UPDATE tickets SET auto_state='pending',auto_decision=NULL WHERE auto_state='failed' AND action IS NULL").rowcount
    return {'queued':count}

class TicketInput(BaseModel):
    text: str=Field(min_length=3,max_length=12000)
@app.post('/api/tickets',status_code=202)
def create_ticket(payload:TicketInput):
    if len(payload.text.strip())<3: raise HTTPException(422,'Введите сообщение')
    ticket_id=uuid4().hex[:12]
    now=time.time()
    with db() as c: c.execute("INSERT INTO tickets(id,text,created,received_at,state,source) VALUES(?,?,?,?,'queued','manual')",(ticket_id,payload.text.strip(),now,now))
    return get_ticket(ticket_id)
@app.get('/api/tickets')
def list_tickets():
    with db() as c: ids=[r['id'] for r in c.execute('SELECT id FROM tickets ORDER BY created DESC,id LIMIT 1000')]
    return [get_ticket(id) for id in ids]
@app.get('/api/tickets/{ticket_id}')
def detail(ticket_id:str): return get_ticket(ticket_id)
@app.post('/api/tickets/{ticket_id}/analyze',status_code=202)
def retry(ticket_id:str):
    get_ticket(ticket_id)
    with db() as c:
        changed=c.execute("UPDATE tickets SET state='queued',result=NULL,error=NULL,action=NULL,final_reply=NULL,analyzed_at=NULL,reviewed_at=NULL,topic_id=NULL,latency=NULL,decision_actor=NULL,auto_state='pending',auto_decision=NULL WHERE id=? AND state NOT IN ('processing','queued')",(ticket_id,)).rowcount
    if not changed: raise HTTPException(409,'Анализ уже в очереди')
    return get_ticket(ticket_id)
@app.post('/api/batch/retry',status_code=202)
def retry_failed():
    with db() as c:
        changed=c.execute("UPDATE tickets SET state='queued',error=NULL,analyzed_at=NULL,latency=NULL WHERE state='failed'").rowcount
    return {'queued':changed}

class ActionInput(BaseModel):
    action: Literal['accepted','edited','rejected','escalated']
    reply: str=Field(default='',max_length=10000)
@app.post('/api/tickets/{ticket_id}/action')
def action(ticket_id:str,payload:ActionInput):
    with db() as c:
        row=c.execute('SELECT * FROM tickets WHERE id=?',(ticket_id,)).fetchone()
        if row is None: raise HTTPException(404,'Обращение не найдено')
        if row['state']!='ready': raise HTTPException(409,'Дождитесь анализа')
        reply=payload.reply.strip()
        if payload.action in ('accepted','edited') and not reply: raise HTTPException(422,'Ответ пуст')
        actual='edited' if payload.action=='accepted' and reply!=json.loads(row['result'])['draft']['reply'] else payload.action
        now=time.time()
        c.execute("UPDATE tickets SET action=?,final_reply=?,reviewed_at=?,decision_actor='human',auto_state='done' WHERE id=?",(actual,reply,now,ticket_id))
        c.execute('INSERT INTO actions(ticket_id,action,reply,created) VALUES(?,?,?,?)',(ticket_id,actual,reply,now))
    return get_ticket(ticket_id)

@app.get('/api/topics')
def topics():
    with db() as c:
        result=[]
        for row in c.execute('SELECT * FROM topics ORDER BY id'):
            t=dict(row)
            t['evidence_ids']=json.loads(t['evidence_ids'])
            examples=[dict(x) for x in c.execute('SELECT id,text,source FROM tickets WHERE topic_id=? ORDER BY created DESC',(t['id'],))]
            t['count']=len(examples)
            t['examples']=examples[:4]
            result.append(t)
    return sorted(result,key=lambda t:-t['count'])

@app.get('/api/analytics')
def analytics():
    with db() as c: rows=[dict(r) for r in c.execute('SELECT * FROM tickets')]
    counts=Counter(r['state'] for r in rows)
    topic_counts,priorities,outcomes=Counter(),Counter(),Counter()
    critical=clarify=0
    for r in rows:
        result=json.loads(r['result']) if r['result'] else None
        if result:
            topic_counts[result['classification']['title']]+=1
            priorities[result['priority']]+=1
            critical+=r['action'] not in ('accepted','edited','rejected') and result['priority'] in ('P0','P1')
            clarify+=result['draft']['status']=='clarify'
        outcomes[('auto_'+r['action']) if r['decision_actor']=='agent' and r['action'] else (r['action'] or 'pending')]+=1
    now=datetime.now().replace(minute=0,second=0,microsecond=0)
    timeline={(now-timedelta(hours=i)).isoformat(timespec='minutes'):{'total':0,'reviewed':0} for i in range(11,-1,-1)}
    for r in rows:
        if r['analyzed_at'] and r['state']=='ready':
            key=datetime.fromtimestamp(r['analyzed_at']).replace(minute=0,second=0,microsecond=0).isoformat(timespec='minutes')
            if key in timeline: timeline[key]['total']+=1
        if r['reviewed_at'] and r['decision_actor']!='agent':
            key=datetime.fromtimestamp(r['reviewed_at']).replace(minute=0,second=0,microsecond=0).isoformat(timespec='minutes')
            if key in timeline: timeline[key]['reviewed']+=1
    latency=sorted(r['latency'] for r in rows if r['state']=='ready' and r['latency'] is not None)
    reviewed=sum(bool(r['action']) and r['decision_actor']!='agent' for r in rows)
    auto_counts=Counter(r['action'] for r in rows if r['decision_actor']=='agent')
    return {'total':len(rows),'open':sum(r['action'] not in ('accepted','edited','rejected') for r in rows),'ready':counts['ready'],'queued':counts['queued'],'processing':counts['processing'],'reviewed':reviewed,'accepted':outcomes['accepted'],'acceptance_rate':outcomes['accepted']/reviewed if reviewed else None,'missing':clarify,'failed':counts['failed'],'topics':dict(topic_counts),'priorities':dict(priorities),'outcomes':{k:outcomes[k] for k in ['accepted','edited','escalated','rejected','auto_accepted','auto_clarified','auto_escalated','pending']},'average_latency':round(sum(latency)/len(latency),2) if latency else None,'p95_latency':round(latency[max(0,int(len(latency)*.95)-1)],2) if latency else None,'timeline':timeline,'synthetic':sum(r['source']=='synthetic' for r in rows),'critical_open':critical,'topic_count':len(catalog()),'auto_accepted':auto_counts['accepted'],'auto_clarified':auto_counts['clarified'],'auto_escalated':auto_counts['escalated'],'auto_manual':sum(r['auto_state']=='done' and not r['action'] for r in rows),'auto_failed':sum(r['auto_state']=='failed' for r in rows)}
