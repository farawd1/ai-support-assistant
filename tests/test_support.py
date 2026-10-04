import asyncio
import json
import sqlite3
import pytest
from fastapi.testclient import TestClient
from app import main, providers

@pytest.fixture
def client(tmp_path,monkeypatch):
    monkeypatch.setattr(main,'DB',str(tmp_path/'test.db'))
    monkeypatch.setattr(main,'TOPIC_LOCK',asyncio.Lock())
    async def idle(): await asyncio.Event().wait()
    monkeypatch.setattr(main,'start_workers',idle)
    with TestClient(main.app) as c:
        main.create_account('operator','operator-test-password','operator')
        main.create_account('customer','customer-test-password','user')
        assert c.post('/api/auth/login',json={'username':'operator','password':'operator-test-password'}).status_code==200
        yield c

@pytest.fixture
def live_mock(monkeypatch):
    async def risks(text):
        return providers.Risks(security=.1,money=.9,blocked=.1),{'model':'test-risk','usage':{}}
    async def case(text,topics,risks):
        return providers.CaseAnalysis(topic_id=topics[0]['id'] if topics else None,new_topic=None if topics else providers.TopicProposal(title='Двойное списание',description='Повторное списание за одну покупку'),evidence_quote=text,reasoning='Клиент сообщает о повторном списании',status='clarify',reply='Уточните дату и сумму платежа.',operator_note='Проверить платёж.'),{'model':'test-draft','usage':{}}
    monkeypatch.setattr(providers,'assess_risks',risks)
    monkeypatch.setattr(providers,'analyze_case',case)

def ready(client):
    client.post('/api/auth/login',json={'username':'customer','password':'customer-test-password'})
    r=client.post('/api/tickets',json={'text':'Списали дважды'})
    client.post('/api/auth/login',json={'username':'operator','password':'operator-test-password'})
    assert r.status_code==202 and r.json()['state']=='queued'
    return asyncio.run(main.analyze(r.json()['id']))

def test_real_queue_topic_and_decision(client,live_mock):
    t=ready(client)
    assert t['state']=='ready' and t['result']['analysis_mode']=='live'
    assert t['result']['priority']=='P1'
    assert len(client.get('/api/topics').json())==1
    a=client.get('/api/analytics').json()
    assert a['reviewed']==0 and a['ready']==1
    client.post(f"/api/tickets/{t['id']}/action",json={'action':'accepted','reply':'Изменённый ответ'})
    a=client.get('/api/analytics').json()
    assert a['outcomes']['edited']==1 and a['reviewed']==1
    assert sum(v['total'] for v in a['timeline'].values())==1
    assert sum(v['reviewed'] for v in a['timeline'].values())==1

def test_reuses_discovered_topic(client,live_mock):
    ready(client); ready(client)
    topics=client.get('/api/topics').json()
    assert len(topics)==1 and topics[0]['count']==2

def test_no_fake_fallback(client,monkeypatch):
    async def fail(text): raise providers.ProviderError('Jev',402)
    monkeypatch.setattr(providers,'assess_risks',fail)
    t=ready(client)
    assert t['state']=='failed' and t['result'] is None
    assert '402' in t['error']
    assert client.get('/api/topics').json()==[]
    assert client.get('/api/analytics').json()['failed']==1

def test_retry_is_queued_and_duplicate_rejected(client,live_mock):
    t=ready(client)
    assert client.post(f"/api/tickets/{t['id']}/analyze").status_code==202
    assert client.post(f"/api/tickets/{t['id']}/analyze").status_code==409
    assert client.post(f"/api/tickets/{t['id']}/action",json={'action':'escalated'}).status_code==409

def test_synthetic_input_real_analysis_latency(client,live_mock):
    t=ready(client)
    with main.db() as c:
        c.execute("UPDATE tickets SET source='synthetic',received_at=NULL,latency=2.5 WHERE id=?",(t['id'],))
    a=client.get('/api/analytics').json()
    assert a['synthetic']==1 and a['average_latency']==2.5
    assert a['reviewed']==0

def test_migration_removes_simulated_history(client):
    with main.db() as c:
        c.execute("DELETE FROM settings WHERE key='live-v2'")
        c.execute("INSERT INTO tickets(id,text,created,state,result,action,final_reply) VALUES('sim-123','Пример',100,'ready','{}','accepted','Муляж')")
        c.execute("INSERT INTO actions(ticket_id,action,reply,created) VALUES('sim-123','accepted','Муляж',100)")
    main.init_db()
    t=main.get_ticket('sim-123')
    assert t['state']=='queued' and t['received_at'] is None and t['action'] is None and t['result'] is None
    with main.db() as c: assert c.execute('SELECT count(*) FROM actions').fetchone()[0]==0

def test_ai_evidence_validation(monkeypatch):
    async def fake(*args,**kwargs):
        return dict(topic_id=None,new_topic={'title':'Новая тема','description':'Новое обращение о платеже'},evidence_quote='Выдуманная цитата',reasoning='Тема платежа',status='clarify',reply='Уточните детали',operator_note=''),{}
    monkeypatch.setattr(providers,'deepseek',fake)
    with pytest.raises(providers.ProviderError):
        asyncio.run(providers.analyze_case('Оплата',[],providers.Risks(security=0,money=0,blocked=0)))

def test_discovery_rejects_unknown_evidence(monkeypatch):
    async def fake(*args,**kwargs):
        return {'topics':[{'title':'Ошибка оплаты','description':'Невозможность оплатить','evidence_ids':['invented']}]},{}
    monkeypatch.setattr(providers,'deepseek',fake)
    with pytest.raises(providers.ProviderError): asyncio.run(providers.discover_topics([{'id':'real','text':'Не могу оплатить'}]))

@pytest.fixture
def agent_mock(monkeypatch):
    async def evaluate(text,result):
        return providers.AgentDecision(action='clarify',confidence=.97,reason='Нужны дата и сумма для проверки',requires_external_action=False),{'model':'test-review'}
    monkeypatch.setattr(providers,'evaluate_decision',evaluate)

def low_risk_ticket(client,live_mock):
    t=ready(client)
    with main.db() as c:
        r=t['result'];r['priority']='P2'
        c.execute('UPDATE tickets SET result=? WHERE id=?',(json.dumps(r),t['id']))
    return t

def test_auto_off_has_no_effect(client,live_mock,agent_mock):
    t=ready(client)
    assert not asyncio.run(main.auto_step())
    assert main.get_ticket(t['id'])['action'] is None

def test_auto_clarification_not_closed_or_human_review(client,live_mock,agent_mock):
    t=low_risk_ticket(client,live_mock)
    client.post('/api/autopilot',json={'enabled':True})
    assert asyncio.run(main.auto_step())
    r=main.get_ticket(t['id'])
    assert r['action']=='clarified' and r['decision_actor']=='agent'
    a=main.analytics()
    assert a['open']==1 and a['reviewed']==0 and a['auto_clarified']==1
    assert not asyncio.run(main.auto_step())
    assert len(r['history'])==1

def test_auto_risk_forces_human(client,live_mock,agent_mock):
    t=ready(client)
    client.post('/api/autopilot',json={'enabled':True})
    asyncio.run(main.auto_step())
    r=main.get_ticket(t['id'])
    assert r['action']=='escalated' and r['final_reply']==''
    assert main.analytics()['open']==1

def test_auto_disabled_during_request(client,live_mock,monkeypatch):
    t=low_risk_ticket(client,live_mock)
    async def evaluate(text,result):
        main.set_autopilot(main.AutopilotInput(enabled=False))
        return providers.AgentDecision(action='clarify',confidence=.99,reason='Можно уточнить сообщение',requires_external_action=False),{}
    monkeypatch.setattr(providers,'evaluate_decision',evaluate)
    client.post('/api/autopilot',json={'enabled':True})
    asyncio.run(main.auto_step())
    assert main.get_ticket(t['id'])['action'] is None
    assert main.get_ticket(t['id'])['auto_state']=='pending'

def test_human_wins_race(client,live_mock,monkeypatch):
    t=low_risk_ticket(client,live_mock)
    async def evaluate(text,result):
        main.action(t['id'],main.ActionInput(action='accepted',reply='Ответ оператора'))
        return providers.AgentDecision(action='clarify',confidence=.99,reason='Можно уточнить сообщение',requires_external_action=False),{}
    monkeypatch.setattr(providers,'evaluate_decision',evaluate)
    client.post('/api/autopilot',json={'enabled':True})
    asyncio.run(main.auto_step())
    r=main.get_ticket(t['id'])
    assert r['decision_actor']=='human' and r['final_reply']=='Ответ оператора'
    assert len(r['history'])==1

def test_auto_uncertain_abstains(client,live_mock,monkeypatch):
    t=low_risk_ticket(client,live_mock)
    async def evaluate(text,result):
        return providers.AgentDecision(action='clarify',confidence=.5,reason='Недостаточно уверенности для ответа',requires_external_action=False),{}
    monkeypatch.setattr(providers,'evaluate_decision',evaluate)
    client.post('/api/autopilot',json={'enabled':True})
    asyncio.run(main.auto_step())
    r=main.get_ticket(t['id'])
    assert r['action'] is None and r['auto_decision']['effective_action']=='manual'
    assert not asyncio.run(main.auto_step())


def test_roles_and_sessions(client):
    assert client.post('/api/tickets',json={'text':'Запрос оператора'}).status_code==403
    client.post('/api/auth/logout')
    assert client.get('/api/tickets').status_code==401
    assert client.post('/api/tickets',json={'text':'Запрос гостя'}).status_code==401
    assert client.post('/api/auth/login',json={'username':'customer','password':'wrong'}).status_code==401
    login=client.post('/api/auth/login',json={'username':'customer','password':'customer-test-password'})
    assert login.json()['role']=='user'
    assert 'httponly' in login.headers['set-cookie'].lower()
    for path in ['/api/tickets','/api/analytics','/api/topics','/api/config','/api/autopilot','/api/audit']:
        assert client.get(path).status_code==403
    assert client.post('/api/autopilot',json={'enabled':True}).status_code==403
    assert client.post('/api/batch/retry').status_code==403
    r=client.post('/api/tickets',json={'text':'Мой вопрос'})
    assert r.status_code==202 and set(r.json())=={'id','state'}
    id=r.json()['id']
    assert client.get('/api/tickets/'+id).status_code==403
    assert client.post('/api/tickets/'+id+'/analyze').status_code==403
    assert client.post('/api/tickets/'+id+'/action',json={'action':'rejected'}).status_code==403
    with main.db() as c:
        row=c.execute('SELECT owner_id FROM tickets WHERE id=?',(id,)).fetchone()
        assert row['owner_id'] is not None
    client.post('/api/auth/logout')
    assert client.get('/api/auth/me').status_code==401


def test_audit_interval_and_pagination(client):
    with main.db() as c:
        for stamp in [100,200,300]:
            c.execute("INSERT INTO audit(created,actor,event,target) VALUES(?,'operator','test','ticket')",(stamp,))
    params={'start':'1970-01-01T00:01:40Z','end':'1970-01-01T00:05:00Z','limit':1}
    r=client.get('/api/audit',params=params)
    assert r.status_code==200 and r.json()['total']==2
    assert r.json()['events'][0]['created']==200
    assert client.get('/api/audit',params={**params,'offset':1}).json()['events'][0]['created']==100
    assert client.get('/api/audit',params={**params,'end':params['start']}).status_code==422
    assert client.get('/api/audit',params={**params,'start':'1970-01-01T00:01:40'}).status_code==422
    assert client.get('/api/audit',params={**params,'limit':501}).status_code==422


def test_cross_origin_and_password_storage(client):
    assert client.post('/api/auth/logout',headers={'Origin':'https://attacker.example'}).status_code==403
    assert client.get('/api/auth/me').status_code==200
    with main.db() as c:
        hashed=c.execute("SELECT password_hash FROM accounts WHERE username='operator'").fetchone()[0]
        assert 'operator-test-password' not in hashed
        assert main.verify_password('operator-test-password',hashed)
        assert not main.verify_password('wrong',hashed)
